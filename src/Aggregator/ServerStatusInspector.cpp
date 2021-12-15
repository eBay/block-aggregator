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

#include <Aggregator/ServerStatusInspector.h>
#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/AggregatorLoader.h>

#include "common/logging.hpp"
#include "common/settings_factory.hpp"
#include "monitor/metrics_collector.hpp"

#include <Common/Exception.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <stdexcept>

namespace nuclm {

namespace ErrorCodes {
extern const int CANNOT_RETRIEVE_DEFINED_TABLES;
extern const int TABLE_DEFINITION_NOT_FOUND;
} // namespace ErrorCodes

template <typename T> decltype(auto) inspector_retry(T&& func, size_t max_retries = 100, size_t sleep_time_ms = 1000) {
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

ServerStatusInspector::ServerStatusInspector(boost::asio::io_context& ioc,
                                             const AggregatorLoaderManager& loader_manager_,
                                             DB::ContextMutablePtr context_) :
        loader_manager(loader_manager_),
        coord_client(ioc),
        context(context_),
        server_status{ServerStatus::UNKNOWN},
        timer(ioc),
        running(true) {}

void ServerStatusInspector::setupAsyncWaitTimer() {
    timer.async_wait(std::bind(&ServerStatusInspector::inspectServer, this));
}

void ServerStatusInspector::start() {
    if (!running.load()) {
        LOG(INFO) << "database server status inspector is stopped";
    }

    auto scan_interval = with_settings([this](SETTINGS s) { return s.config.databaseInspector.scanIntervalSecs; });
    LOG_ADMIN(4) << "Scheduling database inspector timer in " << scan_interval << " secs";
    timer.expires_after(boost::asio::chrono::seconds(scan_interval));
    setupAsyncWaitTimer();
}

void ServerStatusInspector::stop() { running = false; }

void ServerStatusInspector::inspectServer() {
    if (!running.load()) {
        LOG(INFO) << "database server inspector is stopped";
        return;
    }
    std::string err_msg;
    syncStatus(err_msg);
    LOG_ADMIN(4) << "Finish one round of inspector work: " << err_msg;
    start();
}

/**
 * to connect to the backend database and then check whether show tables called is working, and make it in a retry loop.
 *
 */
bool ServerStatusInspector::doInspectionWork() {
    std::vector<std::string> tables_defined;

    auto load_table_definitions = [&]() {
        bool result = false;
        try {
            AggregatorLoader loader(context, loader_manager.getConnectionPool(),
                                    loader_manager.getConnectionParameters());
            loader.init();
            DB::Block query_result;

            bool status = loader.getDefinedTables(query_result);
            // the block header has the definition of:
            // name String String(size = 3), type String String(size = 3), default_type String String(size = 3),
            //       default_expression String String(size = 3), comment String String(size = 3),
            //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
            LOG_ADMIN(4) << "at inspector, after getDefinedTables() with status: " << status;
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
                    LOG_ADMIN(4) << "column index: " << column_index++ << " column type: " << p.type->getName()
                                 << " column name: " << p.name << " number of rows: " << p.column->size();
                }

                DB::MutableColumns columns = query_result.mutateColumns();
                // only one column, with type of string;
                auto& column_string_0 = assert_cast<DB::ColumnString&>(*columns[0]);

                size_t total_row_count = column_string_0.size();
                for (size_t i = 0; i < total_row_count; i++) {
                    std::string column_0 = column_string_0.getDataAt(i).toString();
                    // the table name;
                    tables_defined.push_back(column_0);

                    LOG_ADMIN(4) << " row: " << i << ": " << column_0;
                }
                LOG_ADMIN(4) << " total number of rows retrieved:  " << total_row_count;
                result = true;
            } else {
                LOG(ERROR) << "can not retrieve defined tables from the default database.";
            }

            return result;
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            auto code = DB::getCurrentExceptionCode();

            LOG(ERROR) << "with exception return code: " << code << " in thread: " << std::this_thread::get_id();
            std::string err_msg = "database server inspector cannot retrieve defined tables from the backend server";
            throw DB::Exception(err_msg, ErrorCodes::CANNOT_RETRIEVE_DEFINED_TABLES);
        }
    };

    bool result = false;

    size_t sleep_time_for_retry_table_definition_retrieval_ms = 10;         // sleep_time in ms.
    size_t table_definition_retrieval_retries = 5;                          // a
    size_t table_definition_retrieval_retries_due_to_empty_definitions = 5; // b

    auto number_of_tables_defined_in_configuration = with_settings([this](SETTINGS s) {
        auto& databases = s.config.schema.databases;
        for (const auto& database : databases) {
            if (database.databaseName.compare("default") == 0) {
                return database.tables.size();
            }
        }
        return (size_t)0; // if no database name matches default.
    });

    LOG_ADMIN(4) << "retrieved number of tables defined in configuration: "
                 << number_of_tables_defined_in_configuration;

    try {
        if (number_of_tables_defined_in_configuration == 0) {
            // total retry latency time is: a * sleep_time = 100 ms;
            inspector_retry(load_table_definitions, table_definition_retrieval_retries,
                            sleep_time_for_retry_table_definition_retrieval_ms);
            result = true;
        } else {
            // to retry until the table definitions are ready at the server side. This is because database connection
            // is ready, does not mean the table definition is ready. There is still some delay.
            int counter = table_definition_retrieval_retries_due_to_empty_definitions;
            while ((counter > 0) && running.load()) {
                if (tables_defined.empty()) {
                    LOG_ADMIN(4) << "table definition retrieved size is empty. to retrieve table definitions"
                                 << " in thread: " << std::this_thread::get_id();
                    inspector_retry(load_table_definitions, table_definition_retrieval_retries,
                                    sleep_time_for_retry_table_definition_retrieval_ms);

                    LOG_ADMIN(4) << "to retrieve the table definition with retrieved table size:  "
                                 << tables_defined.size()
                                 << " compared to the expected size for tables defined in configuration: "
                                 << number_of_tables_defined_in_configuration
                                 << " in thread: " << std::this_thread::get_id();
                } else {
                    result = true;
                    break; // finally the table definitions at the server side is ready.
                }
                counter--;
            }
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "database server inspector finally failed with with exception return code: " << code;
    }

    return result;
}

bool ServerStatusInspector::syncStatus(std::string& err_msg, bool force) {
    auto old_status = server_status.load();
    if (running.load()) {
        try {
            bool result = doInspectionWork();
            server_status = result ? ServerStatus::UP : ServerStatus::DOWN;
        } catch (...) {
            LOG(ERROR) << "database server inspector fails to do work due to exception: "
                       << " in thread: " << std::this_thread::get_id();
            server_status = ServerStatus::DOWN;
        }
    }

    reportMetrics(server_status.load());

    if (running.load()) {
        if (old_status != server_status.load()) {
            LOG_ADMIN(1) << "Database status changed from " << toServerStatusName(old_status) << " to "
                         << toServerStatusName(server_status);
            return coord_client.report_db_status(server_status.load(), err_msg);
        } else if (force) {
            LOG_ADMIN(1) << "Database status force sync as " << toServerStatusName(server_status);
            return coord_client.report_db_status(server_status.load(), err_msg);
        } else {
            LOG_ADMIN(3) << "Database status updated as " << toServerStatusName(server_status);
        }
    }

    return true;
}

void ServerStatusInspector::reportMetrics(ServerStatus status) {
    std::shared_ptr<ServerStatusMetrics> server_status_metrics = MetricsCollector::instance().getServerStatusMetrics();
    std::string version_number = SETTINGS_FACTORY.get_version();
    if (status == ServerStatus::UP) {
        server_status_metrics->server_is_up->labels({{"version", version_number}}).update(1);
    } else {
        server_status_metrics->server_is_up->labels({{"version", version_number}}).update(0);
    }
}
} // namespace nuclm
