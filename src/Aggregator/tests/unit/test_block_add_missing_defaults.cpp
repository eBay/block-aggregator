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

#include "application.hpp"

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/TableSchemaUpdateTracker.h>
#include <Aggregator/ProtobufBatchReader.h>
#include <Aggregator/DistributedLoaderLock.h>

#include <Serializable/ProtobufReader.h>

#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <Common/assert_cast.h>

#include "common/hashing.hpp"
#include "monitor/metrics_collector.hpp"

#include <Common/Exception.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <boost/functional/hash.hpp>

#include <thread>
#include <iostream>
#include <string>
#include <vector>

#include <csetjmp>
#include <csignal>
#include <cstdlib>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

namespace nuclm {

namespace ErrorCodes {
extern const int CANNOT_RETRIEVE_DEFINED_TABLES;
}

} // namespace nuclm

const std::string TEST_CONFIG_FILE_PATH_ENV_VAR = "TEST_CONFIG_FILE_PATH";

static std::string getConfigFilePath(const std::string& config_file) {
    const char* env_p = std::getenv(TEST_CONFIG_FILE_PATH_ENV_VAR.c_str());
    if (env_p == nullptr) {
        LOG(ERROR) << "cannot find  TEST_CONFIG_FILE_PATH environment variable....exit test execution...";
        exit(-1);
    }

    std::string path(env_p);
    path.append("/").append(config_file);

    return path;
}

static bool forceToRemoveZooKeeperTablePath(const std::string& table_name, const std::string& shard_id) {
    std::string table_path = "/clickhouse/tables/" + shard_id + "/" + table_name;
    bool result = true;
    try {
        std::shared_ptr<zkutil::ZooKeeper> zookeeper_instance =
            nuclm::LoaderZooKeeperSession::getInstance().getZooKeeper();
        zookeeper_instance->removeRecursive(table_path);
    } catch (Coordination::Exception& ex) {
        if (ex.code == Coordination::Error::ZNONODE) {
            LOG(WARNING) << "try to remove node: " << table_path << " that does not exist any more";
        } else {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            result = false;
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        result = false;
    }
    return result;
}

class ContextWrapper {
  public:
    ContextWrapper() :
            shared_context_holder(DB::Context::createShared()),
            context{DB::Context::createGlobal(shared_context_holder.get())} {
        context->makeGlobalContext();
    }

    DB::ContextMutablePtr getContext() { return context; }

    boost::asio::io_context& getIOContext() { return ioc; }

    ~ContextWrapper() { LOG(INFO) << "Global context wrapper is now deleted"; }

  private:
    DB::SharedContextHolder shared_context_holder;
    DB::ContextMutablePtr context;
    boost::asio::io_context ioc{1};
};

class AggregatorBlockAddMissingDefaultsTesting : public ::testing::Test {
  protected:
    static std::string GetShardId() { return std::string("01"); }

    // Per-test-suite set-up
    // Called before the first test in this test suite
    // Can be omitted if not needed
    // NOTE: this method is not called SetUpTestSuite, as what is described in:
    // https://github.com/google/googletest/blob/master/googletest/docs/advanced.md
    /// Fxied from: https://stackoverflow.com/questions/54468799/google-test-using-setuptestsuite-doesnt-seem-to-work
    static void SetUpTestCase() {
        LOG(INFO) << "SetUpTestCase invoked for AggregatorBlockAddMissingDefaultsTesting ...";
        shared_context = new ContextWrapper();
    }

    // Per-test-suite tear-down
    // Called after the last test in this test suite.
    // Can be omitted if not needed
    static void TearDownTestCase() {
        LOG(INFO) << "TearDownTestCase invoked for AggregatorBlockAddMissingDefaultsTesting...";
        delete shared_context;
        shared_context = nullptr;
    }

    // Define per-test set-up logic as usual
    virtual void SetUp() {}

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    static ContextWrapper* shared_context;
};

ContextWrapper* AggregatorBlockAddMissingDefaultsTesting::shared_context = nullptr;

/**
 * Note: this is based on the original table schema's number of the columns, totally we have 9 columns
 *
 *         "     flightYear UInt16"
           "     quarter UInt8"
           "     flightMonth UInt8"
           "     dayOfMonth UInt8"
           "     dayOfWeek UInt8"
           "     flightDate Date"
           "     captain Nullable(String)"
           "     code FixedString(4)"
           "     status String DEFAULT 'normal')"
 */
std::string prepareInsertQueryWithImplicitColumns(std::string table) {
    return "insert into " + table + " values (?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
}

/**
 * Note: this is based on the original table schema's number of the columns, totally we have 9 columns.
 *
 *         "     flightYear UInt16"
           "     quarter UInt8"
           "     flightMonth UInt8"
           "     dayOfMonth UInt8"
           "     dayOfWeek UInt8"
           "     flightDate Date"
           "     captain Nullable(String)"
           "     code FixedString(4)"
           "     status String DEFAULT 'normal')"
 */
std::string prepareInsertQueryWithExplicitColumns(const std::string& table) {
    std::string explicit_columns = " (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, "
                                   "`captain`, `code`, `status`) values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    return "insert into " + table + explicit_columns;
}

/**
 * Totally we will have 9+ 2 = 11 columns, for the insert query that is to be applied to the backend CH server.
 */
std::string prepareInsertQueryToClickHouseWithImplicitColumns(std::string table) {
    return "insert into " + table + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
}

/**
 * Totally we will have 9+2 = 11 columns, for the insert query that is to be applied to the backend CH server.
 */
std::string prepareInsertQueryToClickHouseWithExplicitColumns(std::string table) {
    std::string explicit_columns =
        " (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, `captain`,  `code`, "
        "`status`, `column_new_1`, `column_new_2`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    return "insert into " + table + explicit_columns;
}

/**
 * Have the message insertion that has non-default columns: dayOfMonth and dayOfWeek, missing, and also have all of the
 * defaults columns missing
 */
std::string prepareInsertQueryToClickHouseWithColumnsMissing(std::string table) {
    std::string explicit_columns =
        " (`flightYear`, `quarter`, `flightMonth`,  `flightDate`, `captain`,  `code`) values (?, ?, ?, ?, ?, ?)";
    return "insert into " + table + explicit_columns;
}

/**
 * The message that is generated based on the original table definition, which is:
 *
 * CREATE TABLE original_ontime_with_nullable (
  `flightYear` UInt16,
  `quarter` UInt8,
  `flightMonth` UInt8,
  `dayOfMonth` UInt8,
  `dayOfWeek` UInt8,
  `flightDate` Date,
   captain Nullable(String),
   code FixedString(4),
   status String DEFAULT 'normal'

)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/orginal_ontime_with_nullable', '{replica}')
          PARTITION BY (flightDate) PRIMARY KEY (flightYear, flightDate)
          ORDER BY (flightYear, flightDate) SETTINGS index_granularity=8192;

 * version index records the schema version number, 0, 1, 2, ...
 */
nucolumnar::aggregator::v1::SQLBatchRequest
generateBatchMessage(const std::string& table_name, size_t hash_code, size_t number_of_rows, size_t version_index,
                     bool with_implicit_columns, std::vector<uint16_t>& flight_year_array,
                     std::vector<uint8_t>& quarter_array, std::vector<uint8_t>& flight_month_array,
                     std::vector<uint8_t>& day_of_month_array, std::vector<uint8_t>& day_of_week_array,
                     std::vector<uint16_t>& flight_date_array, std::vector<std::optional<std::string>>& captain_array,
                     std::vector<std::string>& code_array, std::vector<std::string>& status_array) {
    LOG(INFO) << " OntimeTable Processor to generate one batch message with number of rows: " << number_of_rows;
    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    std::string shard_id = "1235";
    sqlBatchRequest.set_shard(shard_id);
    sqlBatchRequest.set_schema_hashcode(hash_code);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();

    std::string insert_sql;
    if (with_implicit_columns) {
        insert_sql = prepareInsertQueryWithImplicitColumns(table_name);
    } else {
        insert_sql = prepareInsertQueryWithExplicitColumns(table_name);
    }
    LOG(INFO) << " insert query for table: " << table_name << " is: " << insert_sql;

    sqlWithBatchBindings->set_sql(insert_sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    std::string serializedSqlBatchRequestInString;
    srand(time(NULL)); // create a random seed.

    // to make sure that the data will be ordered by the array sequence.
    int random_int_val = std::rand() % 10000000;
    for (size_t row = 0; row < number_of_rows; row++) {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1: flightYear
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        // val1->set_int_value(1000 + (std::rand() % 100));
        int flight_year_val = random_int_val % 5000;
        val1->set_int_value(flight_year_val);
        flight_year_array.push_back(flight_year_val);

        // value 2: quarter
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        int quarter_val = random_int_val % 4 + 1;
        val2->set_int_value(quarter_val);
        quarter_array.push_back(quarter_val);

        // value 3: flightMonth
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        int flight_month_val = random_int_val % 12 + 1;
        val3->set_int_value(flight_month_val);
        flight_month_array.push_back(flight_month_val);

        // value 4: dayOfMonth
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        int day_of_month_val = random_int_val % 30 + 1;
        val4->set_int_value(day_of_month_val);
        day_of_month_array.push_back(day_of_month_val);

        // value 5: dayOfWeek
        nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
        int day_of_week = random_int_val % 7 + 1;
        val5->set_int_value(day_of_week);
        day_of_week_array.push_back(day_of_week);

        // value 6: flightDate
        nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
        auto ts = new nucolumnar::datatypes::v1::TimestampP();
        auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
        ts->set_milliseconds(ms.count());
        val6->set_allocated_timestamp(ts);

        // convert to the days since the epoch.
        auto days_since_epoch = ms.count() / 1000 / 3600 / 24;
        flight_date_array.push_back(days_since_epoch);

        // value 7: captain, string
        nucolumnar::datatypes::v1::ValueP* val7 = bindingList.add_values();
        std::string captain_val = "Captain Phillips";
        val7->set_string_value(captain_val);
        captain_array.push_back(captain_val);

        // value 8: code, string
        // make different kafka cluster generate different message to avoid dedup
        nucolumnar::datatypes::v1::ValueP* val8 = bindingList.add_values();
        std::string version_index_str = std::to_string(version_index);
        std::string str_val = "LVS" + version_index_str;
        val8->set_string_value(str_val);
        code_array.push_back(str_val);

        // value 9: status, string
        nucolumnar::datatypes::v1::ValueP* val9 = bindingList.add_values();
        std::string status_val = "normal";
        val9->set_string_value(status_val);
        status_array.push_back(status_val);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);
        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);
        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
        pval4->CopyFrom(*val4);
        nucolumnar::datatypes::v1::ValueP* pval5 = dataBindingList->add_values();
        pval5->CopyFrom(*val5);
        nucolumnar::datatypes::v1::ValueP* pval6 = dataBindingList->add_values();
        pval6->CopyFrom(*val6);
        nucolumnar::datatypes::v1::ValueP* pval7 = dataBindingList->add_values();
        pval7->CopyFrom(*val7);
        nucolumnar::datatypes::v1::ValueP* pval8 = dataBindingList->add_values();
        pval8->CopyFrom(*val8);
        nucolumnar::datatypes::v1::ValueP* pval9 = dataBindingList->add_values();
        pval9->CopyFrom(*val9);

        // So that we can have (flightYear, flightDate) to be sorted in the same sequence as the arrays are populated.
        random_int_val++;
    }

    // serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    // return serializedSqlBatchRequestInString;
    return sqlBatchRequest;
}

/**
 * Only the following columns are populated:
 *
 * " (`flightYear`, `quarter`, `flightMonth`,  `flightDate`, `captain`,  `code`) values (?, ?, ?, ?, ?, ?)";
 *
 */
nucolumnar::aggregator::v1::SQLBatchRequest generateBatchMessageWithColumnsMissing(
    const std::string& table_name, size_t hash_code, size_t number_of_rows, size_t version_index,
    bool with_implicit_columns, std::vector<uint16_t>& flight_year_array, std::vector<uint8_t>& quarter_array,
    std::vector<uint8_t>& flight_month_array, std::vector<uint8_t>& day_of_month_array,
    std::vector<uint8_t>& day_of_week_array, std::vector<uint16_t>& flight_date_array,
    std::vector<std::optional<std::string>>& captain_array, std::vector<std::string>& code_array) {
    LOG(INFO) << " OntimeTable Processor to generate one batch message with number of rows: " << number_of_rows;
    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    std::string shard_id = "1235";
    sqlBatchRequest.set_shard(shard_id);
    sqlBatchRequest.set_schema_hashcode(hash_code);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();

    std::string insert_sql;
    if (with_implicit_columns) {
        insert_sql = prepareInsertQueryWithImplicitColumns(table_name);
    } else {
        insert_sql = prepareInsertQueryWithExplicitColumns(table_name);
    }
    LOG(INFO) << " insert query for table: " << table_name << " is: " << insert_sql;

    sqlWithBatchBindings->set_sql(insert_sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    std::string serializedSqlBatchRequestInString;
    srand(time(NULL)); // create a random seed.

    // to make sure that the data will be ordered by the array sequence.
    int random_int_val = std::rand() % 10000000;

    for (size_t row = 0; row < number_of_rows; row++) {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1: flightYear
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        // val1->set_int_value(1000 + (std::rand() % 100));
        int flight_year_val = random_int_val % 5000;
        val1->set_int_value(flight_year_val);
        flight_year_array.push_back(flight_year_val);

        // value 2: quarter
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        int quarter_val = random_int_val % 4 + 1;
        val2->set_int_value(quarter_val);
        quarter_array.push_back(quarter_val);

        // value 3: flightMonth
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        int flight_month_val = random_int_val % 12 + 1;
        val3->set_int_value(flight_month_val);
        flight_month_array.push_back(flight_month_val);

        // value 4: flightDate
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        auto ts = new nucolumnar::datatypes::v1::TimestampP();
        auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
        ts->set_milliseconds(ms.count());
        val4->set_allocated_timestamp(ts);

        // covert to the days since the epoch.
        auto days_since_epoch = ms.count() / 1000 / 3600 / 24;
        flight_date_array.push_back(days_since_epoch);

        // value 5: captain, string
        nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
        std::string captain_val = "Captain Phillips";
        val5->set_string_value(captain_val);
        captain_array.push_back(captain_val);

        // value 6: code, string
        // make different kafka cluster generate different message to avoid dedup
        nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
        std::string version_index_str = std::to_string(version_index);
        std::string str_val = "LVS" + version_index_str;
        val6->set_string_value(str_val);
        code_array.push_back(str_val);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);
        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);
        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
        pval4->CopyFrom(*val4);
        nucolumnar::datatypes::v1::ValueP* pval5 = dataBindingList->add_values();
        pval5->CopyFrom(*val5);
        nucolumnar::datatypes::v1::ValueP* pval6 = dataBindingList->add_values();
        pval6->CopyFrom(*val6);
    }

    // serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    // return serializedSqlBatchRequestInString;
    return sqlBatchRequest;
}

size_t getRowCountFromTable(const std::string& table_name, nuclm::AggregatorLoaderManager& manager,
                            DB::ContextMutablePtr context) {
    size_t total_row_count = 0;

    try {
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();
        std::string query = "select count (*) from " + table_name;
        DB::Block query_result;

        bool status = loader.executeTableSelectQuery(table_name, query, query_result);
        // the block header has the definition of:
        // name String String(size = 3), type String String(size = 3), default_type String String(size = 3),
        //       default_expression String String(size = 3), comment String String(size = 3),
        //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
        if (status) {
            std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

            std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
            // header definition for the table definition block:
            DB::Block sample_block;
            sm->loadSampleHeader(sample_block);

            DB::MutableColumns columns = query_result.mutateColumns();
            // only one column, with type of UInt64.
            auto& column_uint64_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]);
            DB::UInt64 count_query_result = column_uint64_0.getData()[0];

            LOG(INFO) << " total number of rows is: " << count_query_result;
            total_row_count = count_query_result;

        } else {
            std::string err_msg = "Aggregator Loader Manager cannot find table definition for table: " + table_name;
            LOG(ERROR) << err_msg;
            throw DB::Exception(err_msg, nuclm::ErrorCodes::CANNOT_RETRIEVE_DEFINED_TABLES);
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
    }

    return total_row_count;
}

static bool removeTableContent(DB::ContextMutablePtr context, boost::asio::io_context& ioc,
                               const std::string& table_name) {
    bool query_result = false;
    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();

        std::string query = "ALTER TABLE " + table_name + " DELETE WHERE 1=1;";

        query_result = loader.executeTableCreation(table_name, query);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();
        LOG(ERROR) << "with exception return code: " << code;
        query_result = false;
    }

    return query_result;
}

using DetailedRowInspector = std::function<void(DB::Block& /*query result*/, DB::Block& /*block header*/)>;

static bool inspectRowBasedContent(const std::string& table_name, const std::string& query_on_table,
                                   nuclm::AggregatorLoader& loader, const DetailedRowInspector& rowInspector) {
    // to retrieve array counter with 5 elements, and to retrieve default array(array(nullable(string))).
    bool query_status = false;
    try {
        // Retrieve the array value back.
        DB::Block query_result;

        bool status = loader.executeTableSelectQuery(table_name, query_on_table, query_result);
        LOG(INFO) << " status on querying table: " + table_name + " with query: " + query_on_table;
        if (status) {
            std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

            std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
            // header definition for the table definition block:
            DB::Block sample_block;
            sm->loadSampleHeader(sample_block);
            rowInspector(query_result, sample_block);
            query_status = true;
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code << " when retrieving rows for table: " << table_name;
    }

    return query_status;
}

/**
 * Sequence of actions:
 *    (1) create a table schema,  schema_version_1.
 *    (2) following the table schema to create the schema hash, and populate the rows: {rows_version_1}
 *    (3) construct a test-case-wide protobuf reader, that is with the table definition from schema_version_1;
 *
 *    (4) alter schema definition, to schema_version_2;
 *    (5) update the schema tracker with schema version 2
 *    (6) populate the rows still with rows_version_1, and schema_version_1;
 *    (7) consume the messages with the existing protobuf reader;
 *
 *    (8) alter schema definition, to schema version 3;
 *    (9) update the schema tracker with schema version 3;
 *    (9) populate the rows still with rows_version_1, and schema_version_1
 *    (9) consume the messages with the existing protobuf reader;
 *

 *    (5) then insert the constructed block that follows the newest schema
 *
 *    The insertion should be successful, as the inserted block will follow the newest schema.
 *
 *    NOTE a row-counter is introduced for better tracking purpose.
 *
 *    The test case works, we can see that: (1) at the end, the columns have been changed to 12 columns, and (2) we
 *    use the implicit loader->CH insert query statement.
 *
        I0701 18:15:51.856349 17641 test_dynamic_schema_update.cpp:1768] with schema 2, total number of rows in block
 holder: 9 I0701 18:15:51.856359 17641 test_dynamic_schema_update.cpp:1770] with schema 2, column names dumped in block
 holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code, status,
 column_new_1, column_new_2 I0701 18:15:51.856381 17641 test_dynamic_schema_update.cpp:1775] with schema 2, structure
 dumped in block holder: flightYear UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth UInt8 UInt8(size
 = 9), dayOfMonth UInt8 UInt8(size = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date UInt16(size = 9), captain
 Nullable(String) Nullable(size = 9, String(size = 9), UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code
 FixedString(4) FixedString(size = 9), status String String(size = 9), column_new_1 UInt64 UInt64(size = 9),
 column_new_2 String String(size = 9) I0701 18:15:51.856395 17641 AggregatorLoader.cpp:405]  loader received insert
 query: insert into original_ontime_with_nullable_test values (?, ?, ?, ?, ?, ?, ?, ?, ?,?) for table:
 original_ontime_with_nullable_test
 */
TEST_F(AggregatorBlockAddMissingDefaultsTesting, testTableWithOneSchemaUpdateWithImplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    size_t total_specified_number_of_rows_per_round = 3;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorBlockAddMissingDefaultsTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorBlockAddMissingDefaultsTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        std::string final_target_table_name = "block_default_missing_ontime_test_2";
        bool removed = removeTableContent(context, ioc, final_target_table_name);
        LOG(INFO) << "remove table content for table: " << final_target_table_name << " returned status: " << removed;
        ASSERT_TRUE(removed);

        nuclm::AggregatorLoaderManager manager(context, ioc);

        DB::Block global_block_holder;

        // The arrays that capture the columns to be populated.
        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        // to construct the block with version 0 to start with
        {
            std::string table_name_version_0 = "block_default_missing_ontime_test_0";

            const nuclm::TableColumnsDescription& table_definition_version_0 =
                manager.getTableColumnsDefinition(table_name_version_0);
            LOG(INFO) << " with version 0, table schema with definition: " << table_definition_version_0.str();

            size_t default_columns_count = table_definition_version_0.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition_version_0.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 0, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 8);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition_version_0.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder_version_0 = nuclm::SerializationHelper::getBlockDefinition(
                table_definition_version_0.getFullColumnTypesAndNamesDefinition());

            size_t hash_code_version_0 = table_definition_version_0.getSchemaHash();
            size_t version_index_0 = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                table_name_version_0, hash_code_version_0, total_specified_number_of_rows_per_round, version_index_0,
                true, flight_year_array, quarter_array, flight_month_array, day_of_month_array, day_of_week_array,
                flight_date_array, captain_array, code_array, status_array);
            std::string serialized_sqlbatchrequest_in_string_with_version_0 = sqlBatchRequest.SerializeAsString();
            std::string insert_query_following_schema_0 = prepareInsertQueryWithImplicitColumns(table_name_version_0);

            std::shared_ptr<nuclm::TableSchemaUpdateTracker> schema_tracker_ptr =
                std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name_version_0, table_definition_version_0,
                                                                  manager);

            // replace with the header block with the column names from the current schema.
            // NOTE: If I pick it to become empty block, the result is empty block. Why?
            global_block_holder = nuclm::SerializationHelper::getBlockDefinition(
                table_definition_version_0.getFullColumnTypesAndNamesDefinition());
            std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serialized_sqlbatchrequest_in_string_with_version_0, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            {
                size_t total_number_of_rows_holder_0 = global_block_holder.rows();
                LOG(INFO) << "version 0, total number of rows in global block holder: "
                          << total_number_of_rows_holder_0;
                std::string names_holder_0 = global_block_holder.dumpNames();
                LOG(INFO) << "version 0, column names dumped in global block holder : " << names_holder_0;

                ASSERT_EQ(total_number_of_rows_holder_0, total_specified_number_of_rows_per_round);
                ASSERT_EQ(global_block_holder.columns(), (size_t)9);
            }

            LOG(INFO) << "finish 1st batch of message consumption using schema 0 installed (step 1)";
        }

        // Now to move to version 1
        {

            std::string table_name_version_1 = "block_default_missing_ontime_test_1";

            const nuclm::TableColumnsDescription& table_definition_version_1 =
                manager.getTableColumnsDefinition(table_name_version_1);
            LOG(INFO) << " with version 1, table schema with definition: " << table_definition_version_1.str();

            size_t default_columns_count = table_definition_version_1.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 1, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 2); // only the number of the default columns being increased by 1.
            size_t ordinary_columns_count = table_definition_version_1.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 1, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 8);

            // migrate block To match latestSchema
            nuclm::ProtobufBatchReader::migrateBlockToMatchLatestSchema(global_block_holder, table_definition_version_1,
                                                                        context);

            {
                size_t total_number_of_rows_holder_1 = global_block_holder.rows();
                LOG(INFO) << "version 1, total number of rows in global block holder: "
                          << total_number_of_rows_holder_1;
                std::string names_holder_1 = global_block_holder.dumpNames();
                LOG(INFO) << "version 1, column names dumped in global block holder : " << names_holder_1;

                ASSERT_EQ(total_number_of_rows_holder_1, total_specified_number_of_rows_per_round);
                ASSERT_EQ(global_block_holder.columns(), (size_t)10);
            }

            LOG(INFO) << "finish 2nd batch of message consumption using schema 1 installed (step 2)";
        }

        {
            std::string table_name_version_2 = "block_default_missing_ontime_test_2";

            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name_version_2);
            LOG(INFO) << " with version 2, table schema with definition: " << table_definition_version_2.str();

            size_t default_columns_count = table_definition_version_2.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 2, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 3); // only the number of the default columns being increased by 1.
            size_t ordinary_columns_count = table_definition_version_2.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 2, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 8);

            // migrate block To match latestSchema
            nuclm::ProtobufBatchReader::migrateBlockToMatchLatestSchema(global_block_holder, table_definition_version_2,
                                                                        context);

            {
                size_t total_number_of_rows_holder_2 = global_block_holder.rows();
                LOG(INFO) << "version 2, total number of rows in global block holder: "
                          << total_number_of_rows_holder_2;
                std::string names_holder_2 = global_block_holder.dumpNames();
                LOG(INFO) << "version 2, column names dumped in global block holder : " << names_holder_2;

                ASSERT_EQ(total_number_of_rows_holder_2, total_specified_number_of_rows_per_round);
                ASSERT_EQ(global_block_holder.columns(), (size_t)11);
            }

            LOG(INFO) << "finish 3rd batch of message consumption using schema 2 installed (step 3)";
        }

        // finally load the schema 2 related block into the table of version 2.
        {
            std::string table_name_version_2 = "block_default_missing_ontime_test_2";

            std::string sql_statement = prepareInsertQueryToClickHouseWithExplicitColumns(table_name_version_2);
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            bool result = loader.load_buffer(table_name_version_2, sql_statement, global_block_holder);

            ASSERT_TRUE(result);

            size_t total_rows_loaded_in_current_batch = getRowCountFromTable(table_name_version_2, manager, context);
            LOG(INFO) << "******with schema 2, total rows inserted to table: " << table_name_version_2
                      << " is: " << total_rows_loaded_in_current_batch << "*******";
            ASSERT_EQ(total_rows_loaded_in_current_batch, total_specified_number_of_rows_per_round);

            LOG(INFO) << "finish loading message following schema 2 installed to the backend database (step 4)";
        }

        // Further to perform row-based content checking
        {
            std::string table_being_queried = "default." + final_target_table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     column_new_1, \n"  /* 6. UInt64, with default value of 0 */
                                         "     column_new_2 \n"   /* 7. String, with default value of "" */
                                         "from " +
                table_being_queried +
                "\n"
                "order by (flightYear, flightDate)";

            auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
                int column_index = 0;

                for (auto& p : columns_with_type_and_name) {
                    LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                              << " column type: " << p.type->getName() << " column name: " << p.name
                              << " number of rows: " << p.column->size();
                }

                DB::MutableColumns columns = query_result.mutateColumns();

                size_t number_of_columns = columns.size();
                ASSERT_EQ(number_of_columns, 8U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt64&>(*columns[6]);      // column_new_1;
                auto& column_7 = assert_cast<DB::ColumnString&>(*columns[7]);      // column_new_2;

                for (size_t i = 0; i < total_specified_number_of_rows_per_round; i++) {
                    // flight year retrieval
                    uint16_t flight_year_val = column_0.getData()[i];
                    LOG(INFO) << "retrieved flight year value: " << flight_year_val
                              << " with expected value: " << flight_year_array[i];
                    ASSERT_EQ(flight_year_val, flight_year_array[i]);

                    // quarter retrieval
                    uint8_t quarter_val = column_1.getData()[i];
                    // NOTE: converted to int32_t is necessary in order to print out the value on the log.
                    LOG(INFO) << "retrieved quarter value: " << (int32_t)quarter_val
                              << " with expected value: " << (int32_t)quarter_array[i];
                    ASSERT_EQ(quarter_val, quarter_array[i]);

                    // flight Date
                    uint16_t flight_date_val = column_2.getData()[i];
                    LOG(INFO) << "retrieved flight date value: " << flight_date_val
                              << " with expected value: " << flight_date_array[i];
                    int date_difference = (int)flight_date_val - (int)flight_date_array[i];
                    ASSERT_TRUE(std::abs(date_difference) < 2);

                    // captain retrieval.
                    auto column_3_nullable_nested_column = column_3.getNestedColumnPtr().get();
                    const DB::ColumnString& resulted_column_3_nullable_nested_column =
                        assert_cast<const DB::ColumnString&>(*column_3_nullable_nested_column);

                    ASSERT_FALSE(column_3.isNullAt(i));
                    if (column_3.isNullAt(i)) {
                        LOG(INFO) << "retrieved captain value is a NULL value";
                    } else {
                        auto column_3_real_value = resulted_column_3_nullable_nested_column.getDataAt(i);
                        std::string column_3_real_string_value(column_3_real_value.data, column_3_real_value.size);
                        LOG(INFO) << "retrieved captain value is: " << column_3_real_string_value;
                    }

                    // code retrieval
                    auto column_4_real_string = column_4.getDataAt(i);
                    std::string column_4_real_string_value(column_4_real_string.data, column_4_real_string.size);
                    LOG(INFO) << "retrieved fixed string code name: " << column_4_real_string_value
                              << " with expected value: " << code_array[i];
                    ASSERT_EQ(column_4_real_string_value, code_array[i]);

                    // status retrieval
                    auto column_5_string = column_5.getDataAt(i);
                    std::string column_5_real_string(column_5_string.data, column_5_string.size);
                    LOG(INFO) << "retrieved status name: " << column_5_real_string
                              << " with expected value: " << status_array[i];
                    ASSERT_EQ(column_5_real_string, status_array[i]);

                    // new_column_1, UInt64
                    uint64_t new_column_1_val = column_6.getData()[i];
                    uint64_t expected_new_column_1_val = 0;
                    LOG(INFO) << "retrieved column_new_1 value: " << new_column_1_val
                              << " with expected value: " << expected_new_column_1_val;
                    ASSERT_EQ(new_column_1_val, expected_new_column_1_val);

                    // new_column_2, String
                    auto column_7_real_string = column_7.getDataAt(i);
                    std::string column_7_real_string_value(column_7_real_string.data, column_7_real_string.size);
                    std::string expected_new_column_2_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_7_real_string_value
                              << " with expected value: " << expected_new_column_2_val;
                    ASSERT_EQ(column_7_real_string_value, expected_new_column_2_val);
                }
            };

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            bool query_status =
                inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
            ASSERT_TRUE(query_status);
        }

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * The missing columns will include: the columns without default values, and the columns with default expressions. For
 * schema 2 insertion, only the following columns are populated:
 *
 *  " (`flightYear`, `quarter`, `flightMonth`,  `flightDate`, `captain`,  `code`) values (?, ?, ?, ?, ?, ?)"
 *
 */
TEST_F(AggregatorBlockAddMissingDefaultsTesting, testTableWithOneSchemaUpdateWithMissingColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    size_t total_specified_number_of_rows_per_round = 3;
    std::string table_name_version_2 = "block_default_missing_ontime_test_2";

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorBlockAddMissingDefaultsTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorBlockAddMissingDefaultsTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        std::string final_target_table_name = table_name_version_2;
        bool removed = removeTableContent(context, ioc, final_target_table_name);
        LOG(INFO) << "remove table content for table: " << final_target_table_name << " returned status: " << removed;
        ASSERT_TRUE(removed);

        nuclm::AggregatorLoaderManager manager(context, ioc);

        DB::Block global_block_holder;

        // The arrays that capture the columns to be populated.
        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<std::string> code_array;

        // To construct the block with version 2 that contains the missing/defaults columns to be populated.
        {
            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name_version_2);
            LOG(INFO) << " with version 0, table schema with definition: " << table_definition_version_2.str();

            size_t default_columns_count = table_definition_version_2.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 3);
            size_t ordinary_columns_count = table_definition_version_2.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 0, size of ordinary columns count being: " << ordinary_columns_count;

            std::string sql_statement = prepareInsertQueryToClickHouseWithColumnsMissing(table_name_version_2);

            size_t hash_code_version_2 = table_definition_version_2.getSchemaHash();
            size_t version_index_2 = 2;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessageWithColumnsMissing(
                table_name_version_2, hash_code_version_2, total_specified_number_of_rows_per_round, version_index_2,
                true, flight_year_array, quarter_array, flight_month_array, day_of_month_array, day_of_week_array,
                flight_date_array, captain_array, code_array);
            std::string serialized_sqlbatchrequest_in_string_with_version_2 = sqlBatchRequest.SerializeAsString();

            std::shared_ptr<nuclm::TableSchemaUpdateTracker> schema_tracker_ptr =
                std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name_version_2, table_definition_version_2,
                                                                  manager);

            global_block_holder = nuclm::SerializationHelper::getBlockDefinition(
                table_definition_version_2.getFullColumnTypesAndNamesDefinition());
            std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serialized_sqlbatchrequest_in_string_with_version_2, schema_tracker_ptr, global_block_holder, context);

            // Get the per-insertion specific ordered columns definition.
            bool table_definition_followed = false;
            bool default_columns_missing = true;
            // The "true value" indicates that: the columns that are not covered in batched columns definition from
            // incoming message, do not have user-defined default expressions associated with.
            bool columns_not_covered_no_deflexpres = false;
            // Construct the column order_mapping for the returned columns definition also.
            std::vector<size_t> order_mapping;

            nuclm::ColumnTypesAndNamesTableDefinition batched_columns_definition =
                batchReader->determineColumnsDefinition(sql_statement, table_definition_followed,
                                                        default_columns_missing, columns_not_covered_no_deflexpres,
                                                        order_mapping, table_definition_version_2);

            LOG(INFO)
                << " columns not covered by those specified in incoming message have default expressions associated: "
                << (columns_not_covered_no_deflexpres ? "YES" : "NO");

            // The corresponding column deserializer based on the per-insertion specific ordered columns definition.
            nuclm::ColumnSerializers columnSerializers =
                nuclm::SerializationHelper::getColumnSerializers(batched_columns_definition);
            LOG(INFO) << "column serializer selected is: "
                      << nuclm::SerializationHelper::strOfColumnSerializers(columnSerializers)
                      << " with table definition being followed: " << table_definition_followed
                      << " with default columns missing: " << default_columns_missing;

            // Get the block header definition based on the per-insertion ordered columns definition for the current
            // batch.
            DB::Block current_sample_block = nuclm::SerializationHelper::getBlockDefinition(batched_columns_definition);
            DB::MutableColumns current_columns = current_sample_block.cloneEmptyColumns();

            nucolumnar::aggregator::v1::SQLBatchRequest deserialized_batch_request;
            deserialized_batch_request.ParseFromString(serialized_sqlbatchrequest_in_string_with_version_2);

            LOG(INFO) << " constructed protobuf-message has the following size: "
                      << deserialized_batch_request.ByteSizeLong()
                      << " and content: " << deserialized_batch_request.DebugString();

            size_t total_number_rows = deserialized_batch_request.nucolumnarencoding().batch_bindings_size();

            for (size_t rindex = 0; rindex < total_number_rows; rindex++) {
                const nucolumnar::aggregator::v1::DataBindingList& row =
                    deserialized_batch_request.nucolumnarencoding().batch_bindings(rindex);

                size_t number_of_columns = row.values().size();
                size_t expected_number_of_columns = columnSerializers.size();
                LOG(INFO) << "total number of columns available in received row: " << rindex
                          << " is: " << number_of_columns;
                LOG(INFO) << "total number of columns expected from the defined schema in received row: " << rindex
                          << " is: " << expected_number_of_columns;

                // The number of the columns need to follow what columnSerializer has provided.
                for (size_t cindex = 0; cindex < expected_number_of_columns; cindex++) {
                    const nucolumnar::datatypes::v1::ValueP& row_value = row.values(cindex);
                    nuclm::ProtobufReader reader(row_value);

                    // need to invoke the deserializer: SerializableDataTypePtr
                    LOG(INFO) << "to deserialize column: " << cindex;
                    bool row_added;
                    columnSerializers[cindex]->deserializeProtobuf(*current_columns[cindex], reader, true, row_added);
                    if (!row_added) {
                        LOG(ERROR) << "protobuf-reader deserialization for column index: " << cindex
                                   << " and column name" << current_columns[cindex]->getName() + " failed";
                    } else {
                        LOG_AGGRPROC(4) << "finish deserialize column: " << cindex;
                    }
                }
            }

            // Finally invoke the to-be-tested method. global_block_holder already is set to follow schema version 2.
            nuclm::ProtobufBatchReader::populateMissingColumnsByFillingDefaults(
                table_definition_version_2, batched_columns_definition, current_columns, global_block_holder, context);
        }

        // To check block structure is correct or not first by following schema 2.
        {
            size_t total_number_of_rows_holder_2 = global_block_holder.rows();
            LOG(INFO) << "By following version 2, total number of rows in global block holder: "
                      << total_number_of_rows_holder_2;
            std::string names_holder_2 = global_block_holder.dumpNames();
            LOG(INFO) << "By Following version 2, column names dumped in global block holder : " << names_holder_2;

            ASSERT_EQ(total_number_of_rows_holder_2, total_specified_number_of_rows_per_round);
            ASSERT_EQ(global_block_holder.columns(), (size_t)11);
        }

        // Then to load the schema 2 related block into the table of version 2.
        {
            std::string sql_statement = prepareInsertQueryToClickHouseWithExplicitColumns(table_name_version_2);

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            bool result_2 = loader.load_buffer(table_name_version_2, sql_statement, global_block_holder);

            ASSERT_TRUE(result_2);

            size_t total_rows_loaded_in_batch = getRowCountFromTable(table_name_version_2, manager, context);

            LOG(INFO) << "******with schema 2, total rows inserted to table: " << table_name_version_2
                      << " is: " << total_rows_loaded_in_batch << "*******";
            ASSERT_EQ(total_rows_loaded_in_batch, total_specified_number_of_rows_per_round);

            LOG(INFO) << "finish loading message following schema 2 installed to the backend database (step 4)";
        }

        // Further to perform row-based content checking
        {
            std::string table_being_queried = "default." + final_target_table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     column_new_1, \n"  /* 6. UInt64, with default value of 0 */
                                         "     column_new_2 \n"   /* 7. String, with default value of "" */
                                         "from " +
                table_being_queried +
                "\n"
                "order by (flightYear, flightDate)";

            auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
                int column_index = 0;

                for (auto& p : columns_with_type_and_name) {
                    LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                              << " column type: " << p.type->getName() << " column name: " << p.name
                              << " number of rows: " << p.column->size();
                }

                DB::MutableColumns columns = query_result.mutateColumns();

                size_t number_of_columns = columns.size();
                ASSERT_EQ(number_of_columns, 8U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt64&>(*columns[6]);      // column_new_1;
                auto& column_7 = assert_cast<DB::ColumnString&>(*columns[7]);      // column_new_2;

                for (size_t i = 0; i < total_specified_number_of_rows_per_round; i++) {
                    // flight year retrieval
                    uint16_t flight_year_val = column_0.getData()[i];
                    LOG(INFO) << "retrieved flight year value: " << flight_year_val
                              << " with expected value: " << flight_year_array[i];
                    ASSERT_EQ(flight_year_val, flight_year_array[i]);

                    // quarter retrieval
                    uint8_t quarter_val = column_1.getData()[i];
                    // NOTE: converted to int32_t is necessary in order to print out the value on the log.
                    LOG(INFO) << "retrieved quarter value: " << (int32_t)quarter_val
                              << " with expected value: " << (int32_t)quarter_array[i];
                    ASSERT_EQ(quarter_val, quarter_array[i]);

                    // flight Date
                    uint16_t flight_date_val = column_2.getData()[i];
                    LOG(INFO) << "retrieved flight date value: " << flight_date_val
                              << " with expected value: " << flight_date_array[i];
                    int date_difference = (int)flight_date_val - (int)flight_date_array[i];
                    ASSERT_TRUE(std::abs(date_difference) < 2);

                    // captain retrieval.
                    auto column_3_nullable_nested_column = column_3.getNestedColumnPtr().get();
                    const DB::ColumnString& resulted_column_3_nullable_nested_column =
                        assert_cast<const DB::ColumnString&>(*column_3_nullable_nested_column);

                    ASSERT_FALSE(column_3.isNullAt(i));
                    if (column_3.isNullAt(i)) {
                        LOG(INFO) << "retrieved captain value is a NULL value";
                    } else {
                        auto column_3_real_value = resulted_column_3_nullable_nested_column.getDataAt(i);
                        std::string column_3_real_string_value(column_3_real_value.data, column_3_real_value.size);
                        LOG(INFO) << "retrieved captain value is: " << column_3_real_string_value;
                    }

                    // code retrieval
                    auto column_4_real_string = column_4.getDataAt(i);
                    std::string column_4_real_string_value(column_4_real_string.data, column_4_real_string.size);
                    LOG(INFO) << "retrieved fixed string code name: " << column_4_real_string_value
                              << " with expected value: " << code_array[i];
                    ASSERT_EQ(column_4_real_string_value, code_array[i]);

                    // status retrieval
                    auto column_5_string = column_5.getDataAt(i);
                    std::string column_5_real_string(column_5_string.data, column_5_string.size);
                    std::string expected_status_val("normal");
                    LOG(INFO) << "retrieved status name: " << column_5_real_string
                              << " with expected value: " << expected_status_val;
                    ASSERT_EQ(column_5_real_string, expected_status_val);

                    // new_column_1, UInt64
                    uint64_t new_column_1_val = column_6.getData()[i];
                    uint64_t expected_new_column_1_val = 0;
                    LOG(INFO) << "retrieved column_new_1 value: " << new_column_1_val
                              << " with expected value: " << expected_new_column_1_val;
                    ASSERT_EQ(new_column_1_val, expected_new_column_1_val);

                    // new_column_2, String
                    auto column_7_real_string = column_7.getDataAt(i);
                    std::string column_7_real_string_value(column_7_real_string.data, column_7_real_string.size);
                    std::string expected_new_column_2_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_7_real_string_value
                              << " with expected value: " << expected_new_column_2_val;
                    ASSERT_EQ(column_7_real_string_value, expected_new_column_2_val);
                }
            };

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            bool query_status =
                inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
            ASSERT_TRUE(query_status);
        }

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

// Call RUN_ALL_TESTS() in main()
// invoke:  ./test_main_launcher --config_file  <config_file>
int main(int argc, char** argv) {
    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
