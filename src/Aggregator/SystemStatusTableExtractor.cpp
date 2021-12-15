/************************************************************************
Copyright 2021, eBay, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "common/settings_factory.hpp"
#include "common/logging.hpp"
#include "monitor/metrics_collector.hpp"

#include <Aggregator/SystemStatusTableExtractor.h>
#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/AggregatorLoader.h>

#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNumberBase.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>

#include <stdexcept>
#include <chrono>

namespace nuclm {

namespace ErrorCodes {
extern const int CANNOT_RETRIEVE_DEFINED_TABLES;
extern const int TABLE_DEFINITION_NOT_FOUND;
} // namespace ErrorCodes

template <typename T> decltype(auto) extractor_retry(T&& func, size_t max_retries = 10, size_t sleep_time_ms = 100) {
    std::exception_ptr exception;
    for (size_t try_number = 0; try_number < max_retries; try_number++) {
        try {
            return func();
        } catch (...) {
            exception = std::current_exception();
            if (try_number < max_retries) {
                std::string start_message = "Current retry: " + std::to_string(try_number) + " with exception: ";
                LOG(ERROR) << start_message + DB::getCurrentExceptionMessage(true);

                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time_ms));
            }
        }
    }

    std::rethrow_exception(exception);
}

SystemStatusTableExtractor::SystemStatusTableExtractor(boost::asio::io_context& ioc,
                                                       const AggregatorLoaderManager& loader_manager_,
                                                       DB::ContextMutablePtr context_) :
        loader_manager(loader_manager_), context(context_), timer(ioc), running(true) {}

void SystemStatusTableExtractor::setupAsyncWaitTimer() {
    timer.async_wait(std::bind(&SystemStatusTableExtractor::extractTables, this));
}

void SystemStatusTableExtractor::start() {
    if (!running.load()) {
        LOG(INFO) << "system status table extractor is stopped";
    }

    auto extract_interval =
        with_settings([this](SETTINGS s) { return s.config.systemTableExtractor.extractIntervalSecs; });
    LOG_ADMIN(4) << "Scheduling system table extractor timer in " << extract_interval << " secs";
    timer.expires_after(boost::asio::chrono::seconds(extract_interval));
    setupAsyncWaitTimer();
}

void SystemStatusTableExtractor::stop() { running = false; }

void SystemStatusTableExtractor::extractTables() {
    if (!running.load()) {
        LOG(INFO) << "system status table extractor is stopped";
        return;
    }

    doExtractTablesWork();
    start();
}

void SystemStatusTableExtractor::doExtractTablesWork() {
    std::vector<SystemReplicasExtractedResult> row_results_from_replicas_table;
    bool extr_replicas_status = doExtractSystemReplicasWork(row_results_from_replicas_table);
    reportSystemReplicasMetrics(row_results_from_replicas_table);
    LOG_ADMIN(4) << "Finish system.replicas extraction work with " << (extr_replicas_status ? "success" : "failure");

    // NOTE: to turn on system.tables related metrics extraction as exception related to parts resolved.
    std::vector<SystemTablesExtractedResult> row_results_from_system_table;
    bool extr_tables_status = doExtractSystemTablesWork(row_results_from_system_table);
    reportSystemTablesMetrics(row_results_from_system_table);
    LOG_ADMIN(4) << "Finish system.tables extraction work with " << (extr_tables_status ? "success" : "failure");
}

/**
 * To connect to the backend database and extract system.replicas' rows selectively to be become metrics. The tables
 * being retrieved only from the user-defined tables.
 *
 */
bool SystemStatusTableExtractor::doExtractSystemReplicasWork(std::vector<SystemReplicasExtractedResult>& row_results) {
    std::vector<std::string> tables_defined;

    auto load_table_definitions = [&]() {
        bool result = false;
        try {
            AggregatorLoader loader(context, loader_manager.getConnectionPool(),
                                    loader_manager.getConnectionParameters());
            loader.init();
            DB::Block query_result;

            std::string table_name = "system.replicas";
            std::string query_on_replicas_status =
                "select table, \n"           /* 0. type: String */
                "     is_leader,\n"          /* 1. type: UInt8 */
                "     is_readonly,\n"        /* 2. type: UInt8 */
                "     is_session_expired,\n" /* 3. type: UInt8 */
                "     future_parts,\n"       /* 4. type: UInt32 */
                "     parts_to_check,\n"     /* 5. type: UInt32 */
                "     queue_size,\n"         /* 6. type: UInt32 */
                "     inserts_in_queue,\n"   /* 7. type: UInt32 */
                "     merges_in_queue,\n"    /* 8. type: UInt32 */
                "     log_max_index,\n"      /* 9. type: UInt64 */
                "     log_pointer,\n"        /* 10. type: UInt64 */
                "     last_queue_update,\n"  /* 11. type: DateTime */
                "     absolute_delay,\n"     /* 12. type: UInt64, How big lag in seconds the current replica has. */
                "     total_replicas,\n"     /* 13. type: UInt8 */
                "     active_replicas\n"     /* 14. type: UInt8 */
                "from system.replicas\n";

            bool status = loader.executeTableSelectQuery(table_name, query_on_replicas_status, query_result);
            LOG_ADMIN(4) << "at system replicas status extractor, after executeTableSelectQuery with status: "
                         << status;
            if (status) {
                std::shared_ptr<AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

                std::shared_ptr<SelectQueryStateMachine> sm =
                    std::static_pointer_cast<SelectQueryStateMachine>(state_machine);
                // header definition for the table definition block:
                DB::Block sample_block;
                sm->loadSampleHeader(sample_block);
                const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
                int column_index = 0;

                for (auto& p : columns_with_type_and_name) {
                    LOG_ADMIN(4) << "system.replicas column index: " << column_index++
                                 << " column type: " << p.type->getName() << " column name: " << p.name
                                 << " number of rows: " << p.column->size();
                }

                DB::MutableColumns columns = query_result.mutateColumns();

                size_t number_of_columns = columns.size();
                CHECK_EQ(number_of_columns, 15U);

                auto& column_string_0 = assert_cast<DB::ColumnString&>(*columns[0]);
                auto& column_uint8_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);
                auto& column_uint8_2 = assert_cast<DB::ColumnUInt8&>(*columns[2]);
                auto& column_uint8_3 = assert_cast<DB::ColumnUInt8&>(*columns[3]);
                auto& column_uint32_4 = assert_cast<DB::ColumnUInt32&>(*columns[4]);
                auto& column_uint32_5 = assert_cast<DB::ColumnUInt32&>(*columns[5]);
                auto& column_uint32_6 = assert_cast<DB::ColumnUInt32&>(*columns[6]);
                auto& column_uint32_7 = assert_cast<DB::ColumnUInt32&>(*columns[7]);
                auto& column_uint32_8 = assert_cast<DB::ColumnUInt32&>(*columns[8]);
                auto& column_uint64_9 = assert_cast<DB::ColumnUInt64&>(*columns[9]);
                auto& column_uint64_10 = assert_cast<DB::ColumnUInt64&>(*columns[10]);
                // Follow DataTypeDateTime.cpp, it uses time_t for internal representation. More specifically, it is
                // mapped to uint32_t, by following the C++ ClickHouse client library's ColumnDateTime implementation.
                auto& column_datetime_container_11 = assert_cast<DB::ColumnUInt32&>(*columns[11]);
                auto& column_uint64_12 = assert_cast<DB::ColumnUInt64&>(*columns[12]);
                auto& column_uint8_13 = assert_cast<DB::ColumnUInt8&>(*columns[13]);
                auto& column_uint8_14 = assert_cast<DB::ColumnUInt8&>(*columns[14]);

                size_t total_row_count = column_string_0.size(); // pick the first column to check the total row size
                for (size_t i = 0; i < total_row_count; i++) {
                    SystemReplicasExtractedResult row_result;
                    row_result.table_name = column_string_0.getDataAt(i).toString();
                    row_result.is_leader = static_cast<bool>(column_uint8_1.getData()[i]);
                    row_result.is_readonly = static_cast<bool>(column_uint8_2.getData()[i]);
                    row_result.is_session_expired = static_cast<bool>(column_uint8_3.getData()[i]);
                    row_result.future_parts = column_uint32_4.getData()[i];
                    row_result.parts_to_check = column_uint32_5.getData()[i];
                    row_result.queue_size = column_uint32_6.getData()[i];
                    row_result.inserts_in_queue = column_uint32_7.getData()[i];
                    row_result.merges_in_queue = column_uint32_8.getData()[i];
                    row_result.log_max_index = column_uint64_9.getData()[i];
                    row_result.log_pointer = column_uint64_10.getData()[i];
                    row_result.last_queue_update = column_datetime_container_11.getData()[i];

                    std::chrono::time_point<std::chrono::high_resolution_clock> current_time =
                        std::chrono::high_resolution_clock::now();
                    row_result.last_queue_update_time_diff_to_now =
                        std::chrono::duration_cast<std::chrono::seconds>(current_time.time_since_epoch()).count() -
                        row_result.last_queue_update;

                    row_result.absolute_delay = column_uint64_12.getData()[i];

                    // LOG_ADMIN(4) << " total replicas retrieved from DB (char8_t) is: " <<
                    // column_uint8_13.getData()[i]; LOG_ADMIN(4) << " active_replicas retrieved from DB (char8_t) is: "
                    // << column_uint8_14.getData()[i];
                    row_result.total_replicas = static_cast<int32_t>(column_uint8_13.getData()[i]);
                    row_result.active_replicas = static_cast<int32_t>(column_uint8_14.getData()[i]);

                    row_results.push_back(row_result);
                }

                LOG_ADMIN(4) << " total number of rows retrieved from system.replicas:  " << total_row_count;
                result = true;
            } else {
                LOG(ERROR) << "can not retrieve system.replicas from system database.";
            }

            return result;
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            auto code = DB::getCurrentExceptionCode();

            LOG(ERROR) << "with exception return code: " << code << " in thread: " << std::this_thread::get_id();
            std::string err_msg =
                "system status table extractor cannot retrieve system.replicas from the backend server";
            throw DB::Exception(err_msg, ErrorCodes::CANNOT_RETRIEVE_DEFINED_TABLES);
        }
    };

    bool result = false;
    try {
        // total retry latency time is: a * sleep_time = 100 ms;
        extractor_retry(load_table_definitions, 10, 100);
        result = true;
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "system status table extractor finally failed on retrieving system.replicas with with exception "
                      "return code: "
                   << code;
        result = false;
    }

    return result;
}

/**
 * To connect to the backend database and retrieve rows selectively from system.tables.  The tables
 * being retrieved contain both the user-defined tables and the system tables.
 */
bool SystemStatusTableExtractor::doExtractSystemTablesWork(std::vector<SystemTablesExtractedResult>& row_results) {
    std::vector<std::string> tables_defined;

    auto load_table_definitions = [&]() {
        bool result = false;
        try {
            AggregatorLoader loader(context, loader_manager.getConnectionPool(),
                                    loader_manager.getConnectionParameters());
            loader.init();
            DB::Block query_result;

            std::string table_name = "system.tables";
            std::string query_on_tables_status = "select name, \n"    /* 0. type: String */
                                                 "     total_rows,\n" /* 1. type: UInt8 */
                                                 "     total_bytes\n" /* 2. type: UInt8 */
                                                 "from system.tables\n";

            bool status = loader.executeTableSelectQuery(table_name, query_on_tables_status, query_result);
            LOG_ADMIN(4) << "at system tables extractor, after executeTableSelectQuery with status: " << status;
            if (status) {
                std::shared_ptr<AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

                std::shared_ptr<SelectQueryStateMachine> sm =
                    std::static_pointer_cast<SelectQueryStateMachine>(state_machine);
                // header definition for the table definition block:
                DB::Block sample_block;
                sm->loadSampleHeader(sample_block);
                const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
                int column_index = 0;

                for (auto& p : columns_with_type_and_name) {
                    LOG_ADMIN(4) << "system.tables column index: " << column_index++
                                 << " column type: " << p.type->getName() << " column name: " << p.name
                                 << " number of rows: " << p.column->size();
                }

                DB::MutableColumns columns = query_result.mutateColumns();

                size_t number_of_columns = columns.size();
                CHECK_EQ(number_of_columns, 3U);

                auto& column_string_0 = assert_cast<DB::ColumnString&>(*columns[0]);
                auto& column_nullable_uint64_1 = assert_cast<DB::ColumnNullable&>(*columns[1]);
                auto& column_nullable_uint64_2 = assert_cast<DB::ColumnNullable&>(*columns[2]);

                size_t total_row_count = column_string_0.size(); // pick the first column to check the total row size
                for (size_t i = 0; i < total_row_count; i++) {
                    SystemTablesExtractedResult row_result;
                    row_result.table_name = column_string_0.getDataAt(i).toString();

                    if (column_nullable_uint64_1.isNullAt(i)) {
                        row_result.total_rows = 0;
                    } else {
                        auto& nested_val_1 = assert_cast<DB::ColumnUInt64&>(column_nullable_uint64_1.getNestedColumn());
                        row_result.total_rows = nested_val_1.getData()[i];
                    }

                    if (column_nullable_uint64_2.isNullAt(i)) {
                        row_result.total_bytes = 0;
                    } else {
                        auto& nested_val_2 = assert_cast<DB::ColumnUInt64&>(column_nullable_uint64_2.getNestedColumn());
                        row_result.total_bytes = nested_val_2.getData()[i];
                    }

                    row_results.push_back(row_result);
                }

                LOG_ADMIN(4) << " total number of rows retrieved from system.tables:  " << total_row_count;
                result = true;
            } else {
                LOG(ERROR) << "can not retrieve system.tables from system database.";
            }

            return result;
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            auto code = DB::getCurrentExceptionCode();

            LOG(ERROR) << "with exception return code: " << code << " in thread: " << std::this_thread::get_id();
            std::string err_msg = "system status table extractor cannot retrieve system.tables from the backend server";
            throw DB::Exception(err_msg, ErrorCodes::CANNOT_RETRIEVE_DEFINED_TABLES);
        }
    };

    bool result = false;
    try {
        // total retry latency time is: a * sleep_time = 100 ms;
        extractor_retry(load_table_definitions, 10, 100);
        result = true;
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "system status extractor finally failed on retrieving system.tables with exception code: "
                   << code;
        result = false;
    }

    return result;
}

void SystemStatusTableExtractor::reportSystemReplicasMetrics(const std::vector<SystemReplicasExtractedResult>& rows) {
    std::shared_ptr<SystemReplicasMetrics> system_replicas_metrics =
        MetricsCollector::instance().getSystemReplicasMetrics();
    for (auto& row : rows) {
        system_replicas_metrics->replica_is_leader->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.is_leader));
        system_replicas_metrics->replica_is_readonly->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.is_readonly));
        system_replicas_metrics->replica_is_session_expired->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.is_session_expired));
        system_replicas_metrics->replica_having_future_parts->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.future_parts));
        system_replicas_metrics->replica_having_parts_to_check->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.parts_to_check));
        system_replicas_metrics->replica_queue_size->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.queue_size));
        system_replicas_metrics->replica_having_inserts_in_queue->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.inserts_in_queue));
        system_replicas_metrics->replica_having_merges_in_queue->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.merges_in_queue));
        system_replicas_metrics->replica_log_max_index->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.log_max_index));
        system_replicas_metrics->replica_log_pointer->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.log_pointer));

        int64_t index_to_pointer_diff = row.log_max_index - row.log_pointer;
        system_replicas_metrics->replica_index_to_pointer_diff->labels({{"table", row.table_name}})
            .update(index_to_pointer_diff);

        system_replicas_metrics->replica_queue_update_time_diff_from_now_in_sec->labels({{"table", row.table_name}})
            .update(row.last_queue_update_time_diff_to_now);

        system_replicas_metrics->replica_absolute_delay_in_sec->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.absolute_delay));
        system_replicas_metrics->replica_reported_total_replicas_in_shard->labels({{"table", row.table_name}})
            .update(row.total_replicas);
        system_replicas_metrics->replica_reported_active_replicas_in_shard->labels({{"table", row.table_name}})
            .update(row.active_replicas);
    }
}

void SystemStatusTableExtractor::reportSystemTablesMetrics(const std::vector<SystemTablesExtractedResult>& rows) {
    std::shared_ptr<SystemTablesMetrics> system_tables_metrics = MetricsCollector::instance().getSystemTablesMetrics();
    for (auto& row : rows) {
        system_tables_metrics->replica_total_bytes->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.total_bytes));
        system_tables_metrics->replica_total_rows->labels({{"table", row.table_name}})
            .update(static_cast<int64_t>(row.total_rows));
    }
}

} // namespace nuclm
