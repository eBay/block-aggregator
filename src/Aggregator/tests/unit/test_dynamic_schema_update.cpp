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

class AggregatorDynamicSchemaUpdateTesting : public ::testing::Test {
  protected:
    static constexpr int SLEEP_TIME_IN_MILLISECONDS = 20000;

    static std::string GetShardId() { return std::string("01"); }

    // Per-test-suite set-up
    // Called before the first test in this test suite
    // Can be omitted if not needed
    // NOTE: this method is not called SetUpTestSuite, as what is described in:
    // https://github.com/google/googletest/blob/master/googletest/docs/advanced.md
    /// Fxied from: https://stackoverflow.com/questions/54468799/google-test-using-setuptestsuite-doesnt-seem-to-work
    static void SetUpTestCase() {
        LOG(INFO) << "SetUpTestCase invoked for AggregatorDynamicSchemaUpdateTesting ...";
        shared_context = new ContextWrapper();
    }

    // Per-test-suite tear-down
    // Called after the last test in this test suite.
    // Can be omitted if not needed
    static void TearDownTestCase() {
        LOG(INFO) << "TearDownTestCase invoked for AggregatorDynamicSchemaUpdateTesting...";
        delete shared_context;
        shared_context = nullptr;
    }

    // Define per-test set-up logic as usual
    virtual void SetUp() {
        LOG(INFO) << "Setup for each test case in AggregatorDynamicSchemaUpdateTesting... ";
        // need to clean up the test table in case it is not cleaned up by the test cases normally, as we have
        // alter table calls in it.
        std::string path = getConfigFilePath("example_aggregator_config.json");
        LOG(INFO) << " JSON configuration file path is: " << path;
        ASSERT_TRUE(!path.empty());

        std::string table_name = "original_ontime_with_nullable_test";
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        try {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (query_result_on_table_definition) {
                std::string query_table_drop = "DROP TABLE " + table_name;
                bool query_result = loader.executeTableCreation(table_name, query_table_drop);
                if (query_result) {
                    LOG(INFO) << "with schema 2, successfully dropped the table: " << table_name;
                } else {
                    LOG(ERROR) << "with schema 2, failed to drop table for table: " << table_name;
                }

                ASSERT_TRUE(query_result);
            } else {
                LOG(INFO) << "table: " << table_name << " does not exist, no need to drop ";
            }

            // no matter whether the table exists or not, we try to remove the ZK path if it exists.
            std::string shard_id = AggregatorDynamicSchemaUpdateTesting::GetShardId();
            bool deleteZKTreeResult = forceToRemoveZooKeeperTablePath(table_name, shard_id);
            LOG(INFO) << "To delete ZooKeeper Table Path for table: " << table_name
                      << " with result (successful=1): " << deleteZKTreeResult;
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            auto code = DB::getCurrentExceptionCode();

            LOG(ERROR) << "with exception return code: " << code;
        }
    }

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    static ContextWrapper* shared_context;
};

ContextWrapper* AggregatorDynamicSchemaUpdateTesting::shared_context = nullptr;

struct CFieldT {
    CFieldT(const std::string& name_, const std::string& type_) :
            field_name(name_), field_type(type_), default_expression("") {}

    CFieldT(const std::string& name_, const std::string& type_, const std::string& default_expression_) :
            field_name(name_), field_type(type_), default_expression(default_expression_) {}

    std::string field_name;
    std::string field_type;
    std::string default_expression;

    size_t operator()(CFieldT const& s) const noexcept {
        size_t seed = 0;
        boost::hash_combine(seed, s.field_name);
        boost::hash_combine(seed, s.field_type);

        if (!s.default_expression.empty()) {
            boost::hash_combine(seed, s.default_expression);
        }

        if (s.default_expression.empty()) {
            LOG(INFO) << "in operator: for column with name: " << s.field_name << " and type: " << s.field_type
                      << " computed hash is: " << seed;
        } else {
            LOG(INFO) << "in operator: for column with name: " << s.field_name << " and type: " << s.field_type
                      << " and default expression: " << s.default_expression << " computed hash is: " << seed;
        }
        return seed;
    }

    friend size_t hash_value(CFieldT const& s) {
        size_t seed = 0;

        boost::hash_combine(seed, s.field_name);
        boost::hash_combine(seed, s.field_type);
        if (!s.default_expression.empty()) {
            boost::hash_combine(seed, s.default_expression);
        }

        if (s.default_expression.empty()) {
            LOG(INFO) << "in hash_value function: for column with name: " << s.field_name
                      << " and type: " << s.field_type << " computed hash is: " << seed;
        } else {
            LOG(INFO) << "in hash_value function: for column with name: " << s.field_name
                      << " and type: " << s.field_type << " and default expression: " << s.default_expression
                      << " computed hash is: " << seed;
        }
        return seed;
    }
};

struct CTableT {
    CTableT(const std::vector<CFieldT> columns_) : columns(columns_) {}
    std::vector<CFieldT> columns;
};

namespace std {
template <> struct hash<CFieldT> {
    size_t operator()(CFieldT const& s) const noexcept {
        size_t seed = 0;

        boost::hash_combine(seed, s.field_name);
        boost::hash_combine(seed, s.field_type);
        if (!s.default_expression.empty()) {
            boost::hash_combine(seed, s.default_expression);
        }

        if (s.default_expression.empty()) {
            LOG(INFO) << "operator to compute hash at the Field level with name: " << s.field_name
                      << " and type: " << s.field_type << "computed hash is: " << seed;
        } else {
            LOG(INFO) << "operator to compute hash at the Field level with name: " << s.field_name
                      << " and type: " << s.field_type << " and default expression: " << s.default_expression
                      << " computed hash is: " << seed;
        }
        return seed;
    }
};

template <> struct hash<CTableT> {
    size_t operator()(CTableT const& s) const noexcept {
        size_t seed = boost::hash_range(s.columns.begin(), s.columns.end());
        return seed;
    }
};

}; // namespace std

class schema_hash {
  public:
    static size_t hash(const CTableT& table) { return std::hash<CTableT>{}(table); }

    static size_t hash(const CFieldT& field) { return std::hash<CFieldT>{}(field); }

    static std::size_t hash_concat(const CTableT& table_definition) {
        std::ostringstream oss;
        auto columns_description = table_definition.columns;
        for (auto& cdef : columns_description) {
            oss << cdef.field_name << cdef.field_type;
            if (!cdef.default_expression.empty()) {
                oss << cdef.default_expression;
            }
        }
        auto hash_func = hashing::hash_function(hashing::algorithm_t::Murmur3_128);
        std::string s = oss.str();

        return hash_func(s.c_str(), s.size());
    }
};

class AugmentedTableSchemaUpdateTracker : public nuclm::TableSchemaUpdateTracker {
  public:
    AugmentedTableSchemaUpdateTracker(const std::string& table_name,
                                      const nuclm::TableColumnsDescription& initial_table_definition,
                                      const nuclm::AggregatorLoaderManager& loader_manager) :
            nuclm::TableSchemaUpdateTracker(table_name, initial_table_definition, loader_manager) {}

    void updateHashMapping(size_t hash_in_message, const nuclm::TableColumnsDescription& latest_schema) {
        nuclm::TableSchemaUpdateTracker::updateHashMapping(hash_in_message, latest_schema);
    }
};

/**
 * Note: this is based on the original table schema's number of the columns, totally we have 10 columns
 *
 *         "     flightYear UInt16"
           "     quarter UInt8"
           "     flightMonth UInt8"
           "     dayOfMonth UInt8"
           "     dayOfWeek UInt8"
           "     flightDate Date"
           "     captain Nullable(String)"
           "     rowCounter UInt16"
           "     code FixedString(4)"
           "     status String DEFAULT 'normal')"
 */
std::string prepareInsertQueryWithImplicitColumns(std::string table) {
    return "insert into " + table + " values (?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
}

/**
 * Note: this is based on the original table schema's number of the columns, totally we have 10 columns.
 *
 *         "     flightYear UInt16"
           "     quarter UInt8"
           "     flightMonth UInt8"
           "     dayOfMonth UInt8"
           "     dayOfWeek UInt8"
           "     flightDate Date"
           "     captain Nullable(String)"
           "     rowCounter UInt16"
           "     code FixedString(4)"
           "     status String DEFAULT 'normal')"
 */
std::string prepareInsertQueryWithExplicitColumns(const std::string& table) {
    std::string explicit_columns = " (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, "
                                   "`captain`, `rowCounter`, `code`, `status`) values (?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
    return "insert into " + table + explicit_columns;
}

std::string prepareInsertQueryWithExplicitColumnsShuffled(const std::string& table) {
    std::string explicit_columns = " (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, "
                                   "`status`, `code`, `rowCounter`, `captain`) values (?, ?, ?, ?, ?, ?, ?, ?, ?,?)";
    return "insert into " + table + explicit_columns;
}

/**
 * Totally we will have 10 + 2 columns, for the insert query that is to be applied to the backend CH server.
 */
std::string prepareInsertQueryToClickHouseWithImplicitColumns(std::string table) {
    return "insert into " + table + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
}

/**
 * Totally we will have 10 + 2 columns, for the insert query that is to be applied to the backend CH server.
 */
std::string prepareInsertQueryToClickHouseWithExplicitColumns(std::string table) {
    std::string explicit_columns =
        " (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, `captain`, `rowCounter`, "
        "`code`, `status`, `column_new_1`, `column_new_2`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    return "insert into " + table + explicit_columns;
}

/**
 * Totaly we will have 10 + 1 columns, for the insert query that is to be applied to the backend CH server, up to schema
 * version 1 (with column_new_1, but not including column_new_2).
 */
std::string prepareInsertQueryToClickHouseWithExplicitColumnsUptoSchema1(std::string table) {
    std::string explicit_columns =
        " (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, `captain`, `rowCounter`, "
        "`code`, `status`, `column_new_1`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
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
   rowCounter UInt16,
   code FixedString(4),
   status String DEFAULT 'normal'

)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/orginal_ontime_with_nullable', '{replica}')
          PARTITION BY (flightDate) PRIMARY KEY (flightYear, flightDate)
          ORDER BY (flightYear, flightDate) SETTINGS index_granularity=8192;

 * version index records the schema version number, 0, 1, 2, ...
 */
nucolumnar::aggregator::v1::SQLBatchRequest
generateBatchMessage(int& random_int_val, const std::string& table_name, size_t hash_code, size_t number_of_rows,
                     size_t version_index, bool with_implicit_columns, std::vector<uint16_t>& flight_year_array,
                     std::vector<uint8_t>& quarter_array, std::vector<uint8_t>& flight_month_array,
                     std::vector<uint8_t>& day_of_month_array, std::vector<uint8_t>& day_of_week_array,
                     std::vector<uint16_t>& flight_date_array, std::vector<std::optional<std::string>>& captain_array,
                     std::vector<uint16_t>& row_counter_array, std::vector<std::string>& code_array,
                     std::vector<std::string>& status_array) {
    static size_t row_counter = 0;
    LOG(INFO) << "version index: " << version_index
              << " OntimeTable Processor to generate one batch message with number of rows: " << number_of_rows;
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

        // value 8  row-counter, int16.
        nucolumnar::datatypes::v1::ValueP* val8 = bindingList.add_values();
        int row_counter_val = row_counter++;
        val8->set_int_value(row_counter_val);
        row_counter_array.push_back(row_counter_val);

        // value 9: code, string
        // make different kafka cluster generate different message to avoid dedup
        nucolumnar::datatypes::v1::ValueP* val9 = bindingList.add_values();
        std::string version_index_str = std::to_string(version_index);
        std::string str_val = "LVS" + version_index_str;
        val9->set_string_value(str_val);
        code_array.push_back(str_val);

        // value 10: status, string
        nucolumnar::datatypes::v1::ValueP* val10 = bindingList.add_values();
        std::string status_val = "normal";
        val10->set_string_value(status_val);
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
        nucolumnar::datatypes::v1::ValueP* pval10 = dataBindingList->add_values();
        pval10->CopyFrom(*val10);

        // So that we can have (flightYear, flightDate) to be sorted in the same sequence as the arrays are populated.
        random_int_val++;
    }

    // serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    // return serializedSqlBatchRequestInString;
    return sqlBatchRequest;
}

/**
 * The last 4 columns exchange the column ordering to become:
 *
       status String DEFAULT 'normal'
       code FixedString(4),
       rowCounter UInt16,
       captain Nullable(String),
 *
 */
nucolumnar::aggregator::v1::SQLBatchRequest generateBatchMessageShuffled(
    int& random_int_val, const std::string& table_name, size_t hash_code, size_t number_of_rows, size_t version_index,
    bool with_implicit_columns, std::vector<uint16_t>& flight_year_array, std::vector<uint8_t>& quarter_array,
    std::vector<uint8_t>& flight_month_array, std::vector<uint8_t>& day_of_month_array,
    std::vector<uint8_t>& day_of_week_array, std::vector<uint16_t>& flight_date_array,
    std::vector<std::optional<std::string>>& captain_array, std::vector<uint16_t>& row_counter_array,
    std::vector<std::string>& code_array, std::vector<std::string>& status_array) {
    static size_t row_counter = 0;
    LOG(INFO) << "version index: " << version_index
              << " OntimeTable Processor to generate one batch message with number of rows: " << number_of_rows;
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
        insert_sql = prepareInsertQueryWithExplicitColumnsShuffled(table_name);
    }
    LOG(INFO) << " insert query for table: " << table_name << " is: " << insert_sql;

    sqlWithBatchBindings->set_sql(insert_sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    std::string serializedSqlBatchRequestInString;
    srand(time(NULL)); // create a random seed.

    for (size_t row = 0; row < number_of_rows; row++) {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1: flightYear
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
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

        // value 10 --> 7: status, string
        nucolumnar::datatypes::v1::ValueP* val7 = bindingList.add_values();
        std::string status_val = "normal";
        val7->set_string_value(status_val);
        status_array.push_back(status_val);

        // value 9 --> 8: code, string
        // make different kafka cluster generate different message to avoid dedup
        nucolumnar::datatypes::v1::ValueP* val8 = bindingList.add_values();
        std::string version_index_str = std::to_string(version_index);
        std::string str_val = "LVS" + version_index_str;
        val8->set_string_value(str_val);
        code_array.push_back(str_val);

        // value 8 --> 9  row-counter, int32.
        nucolumnar::datatypes::v1::ValueP* val9 = bindingList.add_values();
        int row_counter_val = row_counter++;
        val9->set_int_value(row_counter_val);
        row_counter_array.push_back(row_counter_val);

        // value 7 --> 10: captain, string
        nucolumnar::datatypes::v1::ValueP* val10 = bindingList.add_values();
        std::string captain_val = "Captain Phillips";
        val10->set_string_value(captain_val);
        captain_array.push_back(captain_val);

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
        nucolumnar::datatypes::v1::ValueP* pval10 = dataBindingList->add_values();
        pval10->CopyFrom(*val10);

        // So that we can have (flightYear, flightDate) to be sorted in the same sequence as the arrays are populated.
        random_int_val++;
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

/**
 * The computed hash is: 15205413280148992465
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting, testHasfunctionComputation) {
    CFieldT field_1("flightYear", "UInt16");
    CFieldT field_2("quarter", "UInt8");
    CFieldT field_3("flightMonth", "UInt8");
    CFieldT field_4("dayOfMonth", "UInt8");
    CFieldT field_5("dayOfWeek", "UInt8");
    CFieldT field_6("flightDate", "Date");

    std::vector<CFieldT> fields;
    fields.push_back(field_1);
    fields.push_back(field_2);
    fields.push_back(field_3);
    fields.push_back(field_4);
    fields.push_back(field_5);
    fields.push_back(field_6);

    CTableT table_1(fields);

    size_t hash_value = schema_hash::hash_concat(table_1);
    LOG(INFO) << "computed hash value without default expression is: " << hash_value;
}

/**
 *  The computed hash is: 6082285610855790709
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting, testHasfunctionComputationWithDefaultExpression) {
    CFieldT field_1("flightYear", "UInt16");
    CFieldT field_2("quarter", "UInt8", "1");
    CFieldT field_3("flightMonth", "UInt8");
    CFieldT field_4("dayOfMonth", "UInt8");
    CFieldT field_5("dayOfWeek", "UInt8");
    CFieldT field_6("flightDate", "Date");

    std::vector<CFieldT> fields;
    fields.push_back(field_1);
    fields.push_back(field_2);
    fields.push_back(field_3);
    fields.push_back(field_4);
    fields.push_back(field_5);
    fields.push_back(field_6);

    CTableT table_1(fields);

    size_t hash_value = schema_hash::hash_concat(table_1);
    LOG(INFO) << "computed hash value with default expression is: " << hash_value;
}

TEST_F(AggregatorDynamicSchemaUpdateTesting, testHasfunctionComputationWithDefaultExpressionInCanonicalForm) {
    CFieldT field_1("flightYear", "UInt16");
    CFieldT field_2("quarter", "UInt8", "CAST('1', 'UInt8')");
    CFieldT field_3("flightMonth", "UInt8");
    CFieldT field_4("dayOfMonth", "UInt8");
    CFieldT field_5("dayOfWeek", "UInt8");
    CFieldT field_6("flightDate", "Date");

    std::vector<CFieldT> fields;
    fields.push_back(field_1);
    fields.push_back(field_2);
    fields.push_back(field_3);
    fields.push_back(field_4);
    fields.push_back(field_5);
    fields.push_back(field_6);

    CTableT table_1(fields);

    size_t hash_value = schema_hash::hash_concat(table_1);
    LOG(INFO) << "computed hash value with default expression in canonical form is: " << hash_value;
}

TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableSchemaUpdateTrackerWithSameHashAsSchemaWithSimpleHashInputSeq) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string sql_statement = "insert into simple_event_5 values(?, ?, ?)";

        for (const auto& table_name : table_names) {
            if (table_name == "simple_event_5") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t current_hash_code = table_definition.getSchemaHash();
                LOG(INFO) << "number of bytes to hold hash code is: " << sizeof(size_t);
                LOG(INFO) << " with schema hash: " << current_hash_code;
                ASSERT_TRUE(current_hash_code != 0);

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);

                // the current definition's hash
                size_t number_of_schema_retrievals = manager.getTableColumnsDefinitionRetrievalTimes(table_name);
                ASSERT_TRUE(number_of_schema_retrievals == 0);

                bool matched_1 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(current_hash_code);
                ASSERT_TRUE(matched_1);

                // repeat again
                bool matched_2 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(current_hash_code);
                ASSERT_TRUE(matched_2);

                // give a different hash code, and that should trigger the fetching of the schema
                current_hash_code = 1;
                LOG(INFO) << " current fake schema hash code is: " << current_hash_code;
                bool matched_3 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(current_hash_code);
                ASSERT_FALSE(matched_3);

                if (!matched_3) {
                    LOG(INFO) << "current schema hash code: " << current_hash_code << " forces schema retrieval";
                    bool new_schema_fetched = true;
                    schema_tracker_ptr->tryAddNewSchemaVersionForNewHash(current_hash_code, new_schema_fetched);
                    // we do not have new schema for this table. this simulates an old hahs code that is out-of-date.
                    // nevertheless, it forces the table retrieval for the first time it is seen.
                    ASSERT_FALSE(new_schema_fetched);

                    number_of_schema_retrievals = manager.getTableColumnsDefinitionRetrievalTimes(table_name);
                    ASSERT_EQ(number_of_schema_retrievals, (size_t)1);
                }

                // now to issue the same hash again
                LOG(INFO) << " current fake schema hash code is: " << current_hash_code;
                bool matched_4 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(current_hash_code);
                ASSERT_TRUE(matched_4);
            }
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableSchemaUpdateTrackerWithSameHashAsSchemaWithComplexHashInputSeq) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string target_table_name = "simple_event_5";
        std::string sql_statement = "insert into " + target_table_name + " values(?, ?, ?)";

        for (const auto& table_name : table_names) {
            if (table_name == target_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t current_hash_code = table_definition.getSchemaHash();
                LOG(INFO) << "number of bytes to hold hash code is: " << sizeof(size_t);
                LOG(INFO) << " with schema hash: " << current_hash_code;
                ASSERT_TRUE(current_hash_code != 0);

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);

                // the current definition's hash
                std::vector<size_t> hash_codes{1, 2, 3, 4, 5};

                size_t number_of_schema_retrievals = manager.getTableColumnsDefinitionRetrievalTimes(table_name);
                ASSERT_TRUE(number_of_schema_retrievals == 0);

                for (size_t i = 0; i < hash_codes.size(); i++) {
                    size_t hash_code = hash_codes[i];
                    bool matched_1 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(hash_code);
                    ASSERT_FALSE(matched_1);

                    if (!matched_1) {
                        LOG(INFO) << "current schema hash code: " << hash_code << " forces schema retrieval";
                        bool new_schema_fetched = true;
                        schema_tracker_ptr->tryAddNewSchemaVersionForNewHash(hash_code, new_schema_fetched);
                        // we do not have new schema for this table. this simulates an old hahs code that is
                        // out-of-date. nevertheless, it forces the table retrieval for the first time it is seen.
                        ASSERT_FALSE(new_schema_fetched);

                        number_of_schema_retrievals = manager.getTableColumnsDefinitionRetrievalTimes(table_name);
                        ASSERT_EQ(number_of_schema_retrievals, i + 1);
                    }
                }

                ASSERT_EQ(number_of_schema_retrievals, hash_codes.size());
                LOG(INFO) << "*******finish first round of hash code scanning to force schema retrieval";

                for (size_t i = 0; i < hash_codes.size(); i++) {
                    size_t hash_code = hash_codes[i];
                    bool matched_1 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(hash_code);
                    ASSERT_TRUE(matched_1);
                }
            }
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableSchemaUpdateTrackerWithRepeatedHashWithDifferentSchemaVersions) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string target_table_name = "simple_event_5";
        std::string sql_statement = "insert into " + target_table_name + " values(?, ?, ?)";

        nuclm::TableColumnsDescription higherVersionOfTableSchema(target_table_name);

        // to use the ontime table's schema to fake to be the higher version of simple_event_5 in this test.
        for (const auto& table_name : table_names) {
            if (table_name == "ontime") {
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();
                higherVersionOfTableSchema = table_definition;
            } else if (table_name == target_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t current_hash_code = table_definition.getSchemaHash();
                LOG(INFO) << "number of bytes to hold hash code is: " << sizeof(size_t);
                LOG(INFO) << " with schema hash: " << current_hash_code;
                ASSERT_TRUE(current_hash_code != 0);

                std::shared_ptr<AugmentedTableSchemaUpdateTracker> schema_tracker_ptr =
                    std::make_shared<AugmentedTableSchemaUpdateTracker>(table_name, table_definition, manager);

                size_t first_hash_code = 1;
                bool matched_1 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(first_hash_code);
                ASSERT_FALSE(matched_1);

                if (!matched_1) {
                    LOG(INFO) << "****current schema hash code: " << first_hash_code << " forces schema retrieval";
                    bool new_schema_fetched = true;
                    schema_tracker_ptr->tryAddNewSchemaVersionForNewHash(first_hash_code, new_schema_fetched);
                    // we do not have new schema for this table. this simulates an old hahs code that is out-of-date.
                    // nevertheless, it forces the table retrieval for the first time it is seen.
                    ASSERT_FALSE(new_schema_fetched);

                    size_t number_of_schema_retrievals = manager.getTableColumnsDefinitionRetrievalTimes(table_name);
                    ASSERT_EQ(number_of_schema_retrievals, 1);
                }

                // now do the fake update to version 2
                size_t second_hash_code = 2;
                schema_tracker_ptr->updateHashMapping(second_hash_code, higherVersionOfTableSchema);

                // then now to do the same hash code again
                bool matched_2 = schema_tracker_ptr->checkHashWithLatestSchemaVersion(first_hash_code);
                ASSERT_FALSE(matched_2);

                if (!matched_2) {
                    LOG(INFO) << "*****current schema hash code: " << first_hash_code << " forces schema retrieval";
                    bool new_schema_fetched = true;
                    schema_tracker_ptr->tryAddNewSchemaVersionForNewHash(first_hash_code, new_schema_fetched);
                    // with the faked schema being the current version, the real schema will be now fetched.
                    ASSERT_TRUE(new_schema_fetched);

                    size_t number_of_schema_retrievals = manager.getTableColumnsDefinitionRetrievalTimes(table_name);
                    LOG(INFO) << "*****total schema retrieval times: " << number_of_schema_retrievals;
                    ASSERT_EQ(number_of_schema_retrievals, (size_t)2);
                }

                // now total number of the schema version we have is three: the original one, the fake one, and the
                // original one again.
                size_t total_number_of_schema_versions = schema_tracker_ptr->getTotalNumberOfCapturedSchemas();
                LOG(INFO) << "*****total schema versions: " << total_number_of_schema_versions;
                ASSERT_EQ(total_number_of_schema_versions, (size_t)3);

                size_t latest_version = schema_tracker_ptr->getLatestSchemaVersion();
                LOG(INFO) << "*****latest schema versions: " << latest_version;
                ASSERT_EQ(latest_version, (size_t)2);
            }
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
 * To explicitly drop table at the end of the test.
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableSchemaUpdateWithAddIfNoExistenceAndThenDropTable) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (!query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name
                          << " does not have the table definition and thus proceed to create table";

                std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
                std::string query_table_creation = "create table " + table_name +
                    "(\n"
                    "     flightYear UInt16,\n"
                    "     quarter UInt8,\n"
                    "     flightMonth UInt8,\n"
                    "     dayOfMonth UInt8,\n"
                    "     dayOfWeek UInt8,\n"
                    "     flightDate Date,\n"
                    "     captain Nullable(String),\n"
                    "     rowCounter UInt16,\n"
                    "     code FixedString(4),\n"
                    "     status String DEFAULT 'normal')\n"
                    "\n"
                    "ENGINE = ReplicatedMergeTree('" +
                    zk_path +
                    "', '{replica}') \n"
                    "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                    "SETTINGS index_granularity=8192; ";

                bool query_result = loader.executeTableCreation(table_name, query_table_creation);
                if (query_result) {
                    LOG(INFO) << "created the table: " << table_name;
                } else {
                    LOG(ERROR) << "failed to create table for table: " << table_name;
                }
            } else {
                LOG(INFO) << "table: " << table_name << " has been already created and thus no need to create it again";
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to drop the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop = "DROP TABLE " + table_name;
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "successfully dropped the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to drop table for table: " << table_name;
            }

            ASSERT_TRUE(query_result);
            std::string shard_id = AggregatorDynamicSchemaUpdateTesting::GetShardId();
            bool deleteZKTreeResult = forceToRemoveZooKeeperTablePath(table_name, shard_id);
            LOG(INFO) << "To delete ZooKeeper Table Path for table: " << table_name
                      << " with result (successful=1): " << deleteZKTreeResult;
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
 * No need to drop table, as each test's setup will handle the drop of the table. This way, we can inspect the table
 * insertion results if necessary.
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableSchemaUpdateWithAddIfNoExistenceAndThenAlterTableToAddColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (!query_result_on_table_definition) {
                std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
                std::string query_table_creation = "create table " + table_name +
                    "(\n"
                    "     flightYear UInt16,\n"
                    "     quarter UInt8,\n"
                    "     flightMonth UInt8,\n"
                    "     dayOfMonth UInt8,\n"
                    "     dayOfWeek UInt8,\n"
                    "     flightDate Date,\n"
                    "     captain Nullable(String),\n"
                    "     rowCounter UInt16,\n"
                    "     code FixedString(4),\n"
                    "     status String DEFAULT 'normal')\n"
                    "\n"
                    "ENGINE = ReplicatedMergeTree('" +
                    zk_path +
                    "', '{replica}') \n"
                    "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                    "SETTINGS index_granularity=8192; ";

                bool query_result = loader.executeTableCreation(table_name, query_table_creation);
                if (query_result) {
                    LOG(INFO) << "created the table: " << table_name;
                } else {
                    LOG(ERROR) << "failed to create table for table: " << table_name;
                }
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "successfully altered the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to alter table for table: " << table_name;
            }

            ASSERT_TRUE(query_result);
        }

        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "successfully altered the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to alter table for table: " << table_name;
            }

            ASSERT_TRUE(query_result);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            // now retrieve the updated schema
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " size of default columns count is: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 3);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;
            ASSERT_TRUE(ordinary_columns_count == 9);
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
 * No need to drop table, as each test's setup will handle the drop of the table. This way, we can inspect the table
 * insertion results if necessary.
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableWithOriginalTableSchemaCreatedIfNoExistence) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (!query_result_on_table_definition) {
                std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
                std::string query_table_creation = "create table " + table_name +
                    " (\n"
                    "     flightYear UInt16,\n"
                    "     quarter UInt8,\n"
                    "     flightMonth UInt8,\n"
                    "     dayOfMonth UInt8,\n"
                    "     dayOfWeek UInt8,\n"
                    "     flightDate Date,\n"
                    "     captain Nullable(String),\n"
                    "     rowCounter UInt16,\n"
                    "     code FixedString(4),\n"
                    "     status String DEFAULT 'normal')\n"
                    "\n"
                    "ENGINE = ReplicatedMergeTree('" +
                    zk_path +
                    "', '{replica}') \n"
                    "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                    "SETTINGS index_granularity=8192; ";

                bool query_result = loader.executeTableCreation(table_name, query_table_creation);
                if (query_result) {
                    LOG(INFO) << "created the table: " << table_name;
                } else {
                    LOG(ERROR) << "failed to create table for table: " << table_name;
                }

                ASSERT_TRUE(query_result);
            } else {
                LOG(INFO) << "table: " << table_name
                          << " does have the table definition and no need to create the table";
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows = 3;

        // all of the data being generated and to be checked.
        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " size of default columns count is: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            size_t hash_code = table_definition.getSchemaHash();
            size_t number_of_rows = total_specified_number_of_rows;
            size_t cluster_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code, number_of_rows, cluster_index, true, flight_year_array,
                quarter_array, flight_month_array, day_of_month_array, day_of_week_array, flight_date_array,
                captain_array, row_counter_array, code_array, status_array);
            std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

            nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
            nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                                   context);

            bool serialization_status = batchReader.read();
            ASSERT_TRUE(serialization_status);

            // for full table insertion that includes all of the columns.
            std::string query = prepareInsertQueryWithImplicitColumns(table_name);
            LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
            loader.init();

            size_t total_number_of_rows_holder = block_holder.rows();
            LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
            size_t total_number_of_columns = block_holder.columns();
            LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;
            std::string names_holder = block_holder.dumpNames();
            LOG(INFO) << "column names dumped in block holder : " << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, number_of_rows);
            ASSERT_EQ(total_number_of_columns, (size_t)10);

            std::string structure = block_holder.dumpStructure();
            LOG(INFO) << "structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, block_holder);

            ASSERT_TRUE(result);
        }
        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******total rows inserted to table : " << table_name << " is: " << total_row_count
                      << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows);
        }
        // further inspect the row content
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter \n"     /* 6. UInt16 */
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
                ASSERT_EQ(number_of_columns, 7U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;

                for (size_t i = 0; i < total_specified_number_of_rows; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);
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
 * The only difference to the previous test on: testTableSchemaUpdateWithAddAndThenAlterAndThenDropNewTable, is that in
 * this tests, the hash code is deliberately set to some incorrect number, say 0, and it will force the retrieval of the
 * table schema.
 *
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting, testTableWithOriginalTableSchemaCreatedWithStaleSchemaHash) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (!query_result_on_table_definition) {
                std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
                std::string query_table_creation = "create table " + table_name +
                    " (\n"
                    "     flightYear UInt16,\n"
                    "     quarter UInt8,\n"
                    "     flightMonth UInt8,\n"
                    "     dayOfMonth UInt8,\n"
                    "     dayOfWeek UInt8,\n"
                    "     flightDate Date,\n"
                    "     captain Nullable(String),\n"
                    "     rowCounter UInt16,\n"
                    "     code FixedString(4),\n"
                    "     status String DEFAULT 'normal')\n"
                    "\n"
                    "ENGINE = ReplicatedMergeTree('" +
                    zk_path +
                    "', '{replica}') \n"
                    "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                    "SETTINGS index_granularity=8192; ";

                bool query_result = loader.executeTableCreation(table_name, query_table_creation);
                if (query_result) {
                    LOG(INFO) << "created the table: " << table_name;
                } else {
                    LOG(ERROR) << "failed to create table for table: " << table_name;
                }

                ASSERT_TRUE(query_result);
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows = 3;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " size of default columns count is: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            // size_t hash_code = table_definition.getSchemaHash();
            // so we have the stale hash, which prompts the protobuf-reader to fetch the latest one.
            size_t hash_code = 0;
            size_t number_of_rows = total_specified_number_of_rows;
            size_t cluster_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code, number_of_rows, cluster_index, true, flight_year_array,
                quarter_array, flight_month_array, day_of_month_array, day_of_week_array, flight_date_array,
                captain_array, row_counter_array, code_array, status_array);
            std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

            nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
            nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                                   context);

            bool serialization_status = batchReader.read();
            ASSERT_TRUE(serialization_status);

            // for full table insertion that includes all of the columns.
            std::string query = prepareInsertQueryWithImplicitColumns(table_name);
            LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
            loader.init();

            size_t total_number_of_rows_holder = block_holder.rows();
            LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
            size_t total_number_of_columns = block_holder.columns();
            LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;
            std::string names_holder = block_holder.dumpNames();
            LOG(INFO) << "column names dumped in block holder : " << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, number_of_rows);
            ASSERT_EQ(total_number_of_columns, (size_t)10);

            std::string structure = block_holder.dumpStructure();
            LOG(INFO) << "structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, block_holder);

            ASSERT_TRUE(result);
        }
        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******total rows inserted to table : " << table_name << " is: " << total_row_count
                      << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows);
        }

        // further to validate row-based content
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /* 3. Nullable(String) */
                                         "     code,\n"           /* 4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter \n"     /* 6. UInt16 */
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
                ASSERT_EQ(number_of_columns, 7U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;

                for (size_t i = 0; i < total_specified_number_of_rows; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);
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
 * Sequence of actions:
 *    (1) create a table schema;
 *    (2) following the table schema to create the schema hash, and populate the rows.
 *    (3) alter table by adding two columns at the end of the table schema definition, along with default values
 specified.
 *
 *    (4) retrieve the new schema, and construct the protobuf reader with the new schema, and the message that is
 populated before.
 *        note that the block holder here is constructed with the new schema (further tests need to test whether the
 block holder
 *        can migrate itself.
 *
 *    (5) then insert the constructed block that follows the new schema.
 *
 *    The insertion should be successful, as the inserted block will follow the new schema.
 *
 *    The test case works, we can see that: (1) at the end, the columns have been changed to 12 columns, and (2) we
 *    use the implicit loader->CH insert query statement.
 *
        I0701 18:10:17.217826 17514 test_dynamic_schema_update.cpp:1497] with schema in DB, total number of rows in
 block holder: 3 I0701 18:10:17.217839 17514 test_dynamic_schema_update.cpp:1499] with schema in DB, column names dumped
 in block holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code,
 status, column_new_1, column_new_2 I0701 18:10:17.217867 17514 test_dynamic_schema_update.cpp:1504] with schema in DB,
 structure dumped in block holder: flightYear UInt16 UInt16(size = 3), quarter UInt8 UInt8(size = 3), flightMonth UInt8
 UInt8(size = 3), dayOfMonth UInt8 UInt8(size = 3), dayOfWeek UInt8 UInt8(size = 3), flightDate Date UInt16(size = 3),
 captain Nullable(String) Nullable(size = 3, String(size = 3), UInt8(size = 3)), rowCounter UInt16 UInt16(size = 3),
 code FixedString(4) FixedString(size = 3), status String String(size = 3), column_new_1 UInt64 UInt64(size = 3),
 column_new_2 String String(size = 3) I0701 18:10:17.217881 17514 AggregatorLoader.cpp:405]  loader received insert
 query: insert into original_ontime_with_nullable_test values (?, ?, ?, ?, ?, ?, ?, ?, ?,?) for table:
 original_ontime_with_nullable_test
 *
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting,
       testTableWithOriginalTableSchemaButWithSchemaUpdateAtInsertionWithImplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (!query_result_on_table_definition) {
                std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
                std::string query_table_creation = "create table " + table_name +
                    " (\n"
                    "     flightYear UInt16,\n"
                    "     quarter UInt8,\n"
                    "     flightMonth UInt8,\n"
                    "     dayOfMonth UInt8,\n"
                    "     dayOfWeek UInt8,\n"
                    "     flightDate Date,\n"
                    "     captain Nullable(String),\n"
                    "     rowCounter UInt16,\n"
                    "     code FixedString(4),\n"
                    "     status String DEFAULT 'normal')\n"
                    "\n"
                    "ENGINE = ReplicatedMergeTree('" +
                    zk_path +
                    "', '{replica}') \n"
                    "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                    "SETTINGS index_granularity=8192; ";

                bool query_result = loader.executeTableCreation(table_name, query_table_creation);
                if (query_result) {
                    LOG(INFO) << "created the table: " << table_name;
                } else {
                    LOG(ERROR) << "failed to create table for table: " << table_name;
                }

                ASSERT_TRUE(query_result);
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milli-seconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows = 3;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        std::string insert_query_following_original_schema;
        std::string serializedSqlBatchRequestInString;
        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " schema 0, table definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " schema 0, size of default columns count is: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " schema 0, size of ordinary columns count is: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            //
            {
                size_t total_number_of_rows_holder_0 = block_holder.rows();
                LOG(INFO) << "schema 0, total number of rows in block holder: " << total_number_of_rows_holder_0;
                std::string names_holder_0 = block_holder.dumpNames();
                LOG(INFO) << "schema 0, column names dumped in block holder : " << names_holder_0;
            }
            //

            size_t hash_code = table_definition.getSchemaHash();
            size_t number_of_rows = total_specified_number_of_rows;
            size_t cluster_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code, number_of_rows, cluster_index, true, flight_year_array,
                quarter_array, flight_month_array, day_of_month_array, day_of_week_array, flight_date_array,
                captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
            insert_query_following_original_schema = prepareInsertQueryWithImplicitColumns(table_name);
        }

        // now update the schema, with two columns and default values added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 0 --> schema 1, successfully altered the table: " << table_name;
            } else {
                LOG(ERROR) << "schema 0 --> schema 1, failed to alter table for table: " << table_name;
            }
        }

        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "with schema 1 in DB, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 1 --> schema 2, successfully altered the table: " << table_name;
            } else {
                LOG(ERROR) << "schema 1 --> schema 2, failed to alter table for table: " << table_name;
            }
        }

        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "with schema 2 in DB, sleep " << sleeptime_on_ms
                  << " milliseconds before to fetch the new table definition for the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            // Now prepare the proto-buf reader from the schema and to see whether it can be fetched successfully.
            // Need to use "false" on the second parameter of getTableColumnsDefinition to get the most recent schema
            const nuclm::TableColumnsDescription& table_definition =
                manager.getTableColumnsDefinition(table_name, false);
            LOG(INFO) << " schema 2, with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " schema 2, size of default columns count is: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 3);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " schema 2, size of ordinary columns count is: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            // the new block holder definition
            DB::Block block_holder =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            //
            {
                size_t total_number_of_rows_holder_2 = block_holder.rows();
                LOG(INFO) << "schema 2, total number of rows in block holder: " << total_number_of_rows_holder_2;
                std::string names_holder_2 = block_holder.dumpNames();
                LOG(INFO) << "schema 2, column names dumped in block holder : " << names_holder_2;
            }
            //

            nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
            nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                                   context);

            bool serialization_status = batchReader.read();
            ASSERT_TRUE(serialization_status);

            // for full table insertion that includes all of the columns.
            std::string query = insert_query_following_original_schema;
            LOG(INFO) << "with schema 2 in DB, chosen table: " << table_name
                      << "with loader to CH insert query: " << query;

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            {
                size_t total_number_of_rows_holder_2 = block_holder.rows();
                LOG(INFO) << "with schema 2 in DB, total number of rows in block holder: "
                          << total_number_of_rows_holder_2;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "with schema 2 in DB, total number of columns in block holder: "
                          << total_number_of_columns;

                std::string names_holder_2 = block_holder.dumpNames();
                LOG(INFO) << "with schema 2 in DB, column names dumped in block holder : " << names_holder_2;

                ASSERT_EQ(total_number_of_rows_holder_2, total_specified_number_of_rows);
                ASSERT_EQ(total_number_of_columns, (size_t)12);
            }

            std::string structure = block_holder.dumpStructure();
            LOG(INFO) << "with schema 2 in DB, structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, block_holder);
            ASSERT_TRUE(result);
        }

        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******total rows inserted to table : " << table_name << " is: " << total_row_count
                      << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows);
        }
        // further perform row-base content validation
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter \n"     /* 6. UInt16 */
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
                ASSERT_EQ(number_of_columns, 7U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;

                for (size_t i = 0; i < total_specified_number_of_rows; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);
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
TEST_F(AggregatorDynamicSchemaUpdateTesting,
       testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithImplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            DB::Block sample_block;
            bool query_result_on_table_definition = loader.getTableDefinition(table_name, sample_block);

            if (query_result_on_table_definition) {
                LOG(INFO) << "table: " << table_name << " does have the table definition";
            } else {
                LOG(INFO) << "table: " << table_name << " does not have the table definition";
            }

            if (!query_result_on_table_definition) {
                std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
                std::string query_table_creation = "create table " + table_name +
                    " (\n"
                    "     flightYear UInt16,\n"
                    "     quarter UInt8,\n"
                    "     flightMonth UInt8,\n"
                    "     dayOfMonth UInt8,\n"
                    "     dayOfWeek UInt8,\n"
                    "     flightDate Date,\n"
                    "     captain Nullable(String),\n"
                    "     rowCounter UInt16,\n"
                    "     code FixedString(4),\n"
                    "     status String DEFAULT 'normal')\n"
                    "\n"
                    "ENGINE = ReplicatedMergeTree('" +
                    zk_path +
                    "', '{replica}') \n"
                    "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                    "SETTINGS index_granularity=8192; ";

                bool query_result = loader.executeTableCreation(table_name, query_table_creation);
                if (query_result) {
                    LOG(INFO) << "created the table: " << table_name;
                } else {
                    LOG(ERROR) << "failed to create table for table: " << table_name;
                }

                ASSERT_TRUE(query_result);
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows_per_round = 3;
        std::string insert_query_following_schema_0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion0;
        size_t hash_code_version_0 = 0;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with version 0, table schema with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 0, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder_version_0 =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            hash_code_version_0 = table_definition.getSchemaHash();
            size_t version_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, true, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion0 = sqlBatchRequest.SerializeAsString();
            insert_query_following_schema_0 = prepareInsertQueryWithImplicitColumns(table_name);
        }
        // block holder becomes global across this test case.
        const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
        DB::Block global_block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

        // schema tracker is initialized along with the original table schema definition.
        std::shared_ptr<AugmentedTableSchemaUpdateTracker> schema_tracker_ptr =
            std::make_shared<AugmentedTableSchemaUpdateTracker>(table_name, table_definition, manager);

        std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = nullptr;
        {
            // push the message into protobuf reader.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion0, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            {
                size_t total_number_of_rows_holder_0 = global_block_holder.rows();
                LOG(INFO) << "version 0, total number of rows in global block holder: "
                          << total_number_of_rows_holder_0;
                std::string names_holder_0 = global_block_holder.dumpNames();
                LOG(INFO) << "version 0, column names dumped in global block holder : " << names_holder_0;
            }

            LOG(INFO) << "finish 1st batch of message consumption using schema 0 installed (step 3)";
        }

        // now update the schema, with one column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 0 --> schema 1, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 0 --> schema 1, successfully altered the table: " << table_name << "(step 4)";
            } else {
                LOG(ERROR) << "schema 0 --> schema 1, failed to alter table for table: " << table_name << "(step 4)";
            }
        }

        std::string serializedSqlBatchRequestInStringWithSchemaVersion1;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "with schema 1, the following is message with schema 0 but read by schema 1's reader";
            // NOTE: how to force the schema upgrade to be known to the reader, as the passed-in message is still with
            // the old schema Let's do the forceful upgrade to schema tracker.
            const nuclm::TableColumnsDescription& table_definition_version_1 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_1 = table_definition_version_1.getSchemaHash();
            schema_tracker_ptr->updateHashMapping(hash_code_version_1, table_definition_version_1);
            // but the message is still with hash code 0.
            size_t version_index = 1;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, true, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion1 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion1, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            size_t total_number_of_rows_holder_1 = global_block_holder.rows();
            LOG(INFO) << "version 1, total number of rows in global block holder: " << total_number_of_rows_holder_1;
            std::string names_holder_1 = global_block_holder.dumpNames();
            LOG(INFO) << "version 1, column names dumped in global block holder : " << names_holder_1;

            LOG(INFO) << "finish 2nd batch of message consumption using schema 0 installed (step 7)";
        }

        // now further update the schema, with one more column and default value added to the end.

        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 1 --> schema 2, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 1 --> schema 2, successfully altered the table: " << table_name << "(step 8)";
            } else {
                LOG(ERROR) << "schema 1 --> schema 2, failed to alter table for table: " << table_name << "(step 8)";
            }
        }

        // now further add message that is with the old schema, schema 0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion2;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "With schema 2, the following is message with schema 0 but read by schema 2's reader";
            //? how to force the schema upgrade to be known to the reader, as the passed-in message is still with the
            //old schema?
            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_2 = table_definition_version_2.getSchemaHash();
            // NOTE: how to force the schema upgrade to be known to the proto-buf reader, as the passed-in message is
            // still with the old schema??? to forcefully update the schema
            schema_tracker_ptr->updateHashMapping(hash_code_version_2, table_definition_version_2);
            size_t version_index = 2;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, true, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion2 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion2, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "finish 3rd batch of message consumption using schema 0 installed";
        }
        size_t rounds = 3;
        {

            // for full table insertion that includes all of the columns.
            std::string query = insert_query_following_schema_0;
            LOG(INFO) << "with schema 2, chosen table: " << table_name << "with insert query: " << query;

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            size_t total_number_of_rows_holder = global_block_holder.rows();
            LOG(INFO) << "with schema 2, total number of rows in block holder: " << total_number_of_rows_holder;
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 2, total number of columns in block holder: " << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 2, column names dumped in block holder : " << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, total_specified_number_of_rows_per_round * rounds);
            ASSERT_EQ(total_number_of_columns, (size_t)12);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 2, structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, global_block_holder);
            ASSERT_TRUE(result);
        }

        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******with schema 2, total rows inserted to table : " << table_name
                      << " is: " << total_row_count << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows_per_round * rounds);
        }

        // further perform row-based content validation
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter, \n"    /* 6. UInt16 */
                                         "     column_new_1, \n"  /* newly introduced column, UInt64*/
                                         "     column_new_2 \n"   /* newly introduced column, String*/
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
                ASSERT_EQ(number_of_columns, 9U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;
                auto& column_7 = assert_cast<DB::ColumnUInt64&>(*columns[7]);      // column_new_1
                auto& column_8 = assert_cast<DB::ColumnString&>(*columns[8]);      // column_new_2

                for (size_t i = 0; i < total_specified_number_of_rows_per_round * rounds; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);

                    // column_new_1, UInt64
                    uint64_t column_new_1_val = column_7.getData()[i];
                    // convert to int in order to show it on the console.
                    uint64_t expected_column_new_1_val = 0U;
                    LOG(INFO) << "retrieved column_new_1 value: " << (int)row_counter_val
                              << " with expected value: " << expected_column_new_1_val;
                    ASSERT_EQ(column_new_1_val, expected_column_new_1_val);

                    // column_new_2, String
                    auto column_8_real_string = column_8.getDataAt(i);
                    std::string column_8_real_string_value(column_8_real_string.data, column_8_real_string.size);
                    std::string expected_column_8_string_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_8_real_string_value
                              << " with expected value: " << expected_column_8_string_val;
                    ASSERT_EQ(column_8_real_string_value, expected_column_8_string_val);
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
 * Same test implementation with the test case of:
 testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithImplicitColumns,
 * but just with always outdated schema hash in the message.
 *
 * For each message constructed, the schema hash is always to be set at 0.
 *
 * The test case works, we can see that: (1) at the end, the columns have been changed to 12 columns, and (2) we
 *    use the implicit loader->CH insert query statement.
 *
    I0701 18:32:45.306056 17915 test_dynamic_schema_update.cpp:2023] with schema 2, total number of rows in block
 holder: 9 I0701 18:32:45.306071 17915 test_dynamic_schema_update.cpp:2025] with schema 2, column names dumped in block
 holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code, status,
 column_new_1, column_new_2 I0701 18:32:45.306102 17915 test_dynamic_schema_update.cpp:2030] with schema 2, structure
 dumped in block holder: flightYear UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth UInt8 UInt8(size
 = 9), dayOfMonth UInt8 UInt8(size = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date UInt16(size = 9), captain
 Nullable(String) Nullable(size = 9, String(size = 9), UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code
 FixedString(4) FixedString(size = 9), status String String(size = 9), column_new_1 UInt64 UInt64(size = 9),
 column_new_2 String String(size = 9) I0701 18:32:45.306116 17915 AggregatorLoader.cpp:405]  loader received insert
 query: insert into original_ontime_with_nullable_test values (?, ?, ?, ?, ?, ?, ?, ?, ?,?) for table:
 original_ontime_with_nullable_test I0701 18:32:45.306196 17915 AggregatorLoader.cpp:56] insert query query expression
 is: INSERT INTO original_ontime_with_nullable_test VALUE
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting,
       testTableWithOriginalTableSchemaAndOutdatedHashButWithMultipleSchemaUpdateAtInsertionsWithImplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
            std::string query_table_creation = "create table " + table_name +
                " (\n"
                "     flightYear UInt16,\n"
                "     quarter UInt8,\n"
                "     flightMonth UInt8,\n"
                "     dayOfMonth UInt8,\n"
                "     dayOfWeek UInt8,\n"
                "     flightDate Date,\n"
                "     captain Nullable(String),\n"
                "     rowCounter UInt16,\n"
                "     code FixedString(4),\n"
                "     status String DEFAULT 'normal')\n"
                "\n"
                "ENGINE = ReplicatedMergeTree('" +
                zk_path +
                "', '{replica}') \n"
                "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                "SETTINGS index_granularity=8192; ";

            bool query_result = loader.executeTableCreation(table_name, query_table_creation);
            if (query_result) {
                LOG(INFO) << "created the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to create table for table: " << table_name;
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows_per_round = 3;
        std::string insert_query_following_schema_0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion0;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with schema version 0, table schema with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " with schema version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with schema version 0, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder_version_0 =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            size_t hash_code_version = 0;
            size_t version_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version, total_specified_number_of_rows_per_round, version_index,
                true, flight_year_array, quarter_array, flight_month_array, day_of_month_array, day_of_week_array,
                flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion0 = sqlBatchRequest.SerializeAsString();
            insert_query_following_schema_0 = prepareInsertQueryWithImplicitColumns(table_name);
        }
        // block holder becomes global across this test case.
        const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
        DB::Block global_block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

        // schema tracker is initialized along with the original table schema definition.
        std::shared_ptr<AugmentedTableSchemaUpdateTracker> schema_tracker_ptr =
            std::make_shared<AugmentedTableSchemaUpdateTracker>(table_name, table_definition, manager);

        std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = nullptr;
        {
            // push the message into protobuf reader.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion0, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "with schema 0, finish 1st batch of message consumption using schema 0 installed (step 3)";
        }

        // now update the schema, with one column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 0 --> schema 1, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 0 --> schema 1, successfully altered the table: " << table_name << "(step 4)";
            } else {
                LOG(ERROR) << "schema 0 --> schema 1, failed to alter table for table: " << table_name << "(step 4)";
            }
        }

        std::string serializedSqlBatchRequestInStringWithSchemaVersion1;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "with schema 1, the following is message with schema 0 but read by schema 1's reader";
            // NOTE: how to force the schema upgrade to be known to the reader, as the passed-in message is still with
            // the old schema Let's do the forceful upgrade to schema tracker.
            const nuclm::TableColumnsDescription& table_definition_version_1 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_1 = table_definition_version_1.getSchemaHash();
            schema_tracker_ptr->updateHashMapping(hash_code_version_1, table_definition_version_1);
            // but the message is still with hash code 0.
            size_t hash_code_version = 0;
            size_t version_index = 1;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version, total_specified_number_of_rows_per_round, version_index,
                true, flight_year_array, quarter_array, flight_month_array, day_of_month_array, day_of_week_array,
                flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion1 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion1, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "with schema 1, finish 2nd batch of message consumption using schema 0 installed (step 7)";
        }

        // now further update the schema, with one more column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 1 --> schema 2, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 1 --> schema 2, successfully altered the table: " << table_name << "(step 8)";
            } else {
                LOG(ERROR) << "schema 1 --> schema 2, failed to alter table for table: " << table_name << "(step 8)";
            }
        }

        // now further add message that is with the old schema, schema 0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion2;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "With schema 2, the following is message with schema 0 but read by schema 2's reader";
            //? how to force the schema upgrade to be known to the reader, as the passed-in message is still with the
            //old schema?
            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_2 = table_definition_version_2.getSchemaHash();
            // NOTE: how to force the schema upgrade to be known to the proto-buf reader, as the passed-in message is
            // still with the old schema??? to forcefully update the schema
            schema_tracker_ptr->updateHashMapping(hash_code_version_2, table_definition_version_2);
            size_t version_index = 2;
            size_t hash_code_version = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version, total_specified_number_of_rows_per_round, version_index,
                true, flight_year_array, quarter_array, flight_month_array, day_of_month_array, day_of_week_array,
                flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion2 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion2, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "with schema 2, finish 3rd batch of message consumption using schema 0 installed";
        }
        size_t rounds = 3;
        {

            // for full table insertion that includes all of the columns.
            std::string query = insert_query_following_schema_0;
            LOG(INFO) << "with schema 2, chosen table: " << table_name << "with insert query: " << query;

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            size_t total_number_of_rows_holder = global_block_holder.rows();
            LOG(INFO) << "with schema 2, total number of rows in block holder: " << total_number_of_rows_holder;
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 2, total number of columns in block holder: " << total_number_of_columns;
            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 2, column names dumped in block holder : " << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, total_specified_number_of_rows_per_round * rounds);
            ASSERT_EQ(total_number_of_columns, (size_t)12);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 2, structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, global_block_holder);
            ASSERT_TRUE(result);
        }

        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******with schema 2, total rows inserted to table : " << table_name
                      << " is: " << total_row_count << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows_per_round * rounds);
        }
        // further to examine row-based content
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter, \n"    /* 6. UInt16 */
                                         "     column_new_1, \n"  /* newly introduced column, UInt64*/
                                         "     column_new_2 \n"   /* newly introduced column, String*/
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
                ASSERT_EQ(number_of_columns, 9U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;
                auto& column_7 = assert_cast<DB::ColumnUInt64&>(*columns[7]);      // column_new_1
                auto& column_8 = assert_cast<DB::ColumnString&>(*columns[8]);      // column_new_2

                for (size_t i = 0; i < total_specified_number_of_rows_per_round * rounds; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);

                    // column_new_1, UInt64
                    uint64_t column_new_1_val = column_7.getData()[i];
                    // convert to int in order to show it on the console.
                    uint64_t expected_column_new_1_val = 0U;
                    LOG(INFO) << "retrieved column_new_1 value: " << (int)row_counter_val
                              << " with expected value: " << expected_column_new_1_val;
                    ASSERT_EQ(column_new_1_val, expected_column_new_1_val);

                    // column_new_2, String
                    auto column_8_real_string = column_8.getDataAt(i);
                    std::string column_8_real_string_value(column_8_real_string.data, column_8_real_string.size);
                    std::string expected_column_8_string_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_8_real_string_value
                              << " with expected value: " << expected_column_8_string_val;
                    ASSERT_EQ(column_8_real_string_value, expected_column_8_string_val);
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
 * Similar to the test case of:
 testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithImplicitColumns,
 * but with explicit columns.
 *
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
 *    From the test execution result, if the aggregator to CH insert-query statement follows the explicit columns used
 *    in the kafka message, which follows schema 0 definition, then we will get the following exception:
 *
        I0701 18:35:55.109158 18106 test_dynamic_schema_update.cpp:2299] with schema 2 and explicit columns, total
 number of rows in block holder: 9 I0701 18:35:55.109169 18106 test_dynamic_schema_update.cpp:2301] with schema 2 and
 explicit columns, column names dumped in block holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek,
 flightDate, captain, rowCounter, code, status, column_new_1, column_new_2 I0701 18:35:55.109200 18106
 test_dynamic_schema_update.cpp:2306] with schema 2 and explicit columns, structure dumped in block holder: flightYear
 UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth UInt8 UInt8(size = 9), dayOfMonth UInt8 UInt8(size
 = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date UInt16(size = 9), captain Nullable(String) Nullable(size = 9,
 String(size = 9), UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code FixedString(4) FixedString(size = 9),
 status String String(size = 9), column_new_1 UInt64 UInt64(size = 9), column_new_2 String String(size = 9) I0701
 18:35:55.109215 18106 AggregatorLoader.cpp:405]  loader received insert query: insert into
 original_ontime_with_nullable_test (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`,
 `captain`, `rowCounter`, `code`, `status`) values (?, ?, ?, ?, ?, ?, ?, ?, ?,?) for table:
 original_ontime_with_nullable_test I0701 18:35:55.109331 18106 AggregatorLoader.cpp:56] insert query query expression
 is: INSERT INTO original_ontime_with_nullable_test (flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek,
 flightDate, captain, rowCounter, code, status) VALUES

 *      E0701 18:35:55.143951 18106 AggregatorLoader.cpp:115] Received exception Code: 10. DB::Exception: Received
 from 10.169.98.238:9000. DB::Exception: Not found column column_new_1 in block. There are only columns: flightYear,
 quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code, status.


 *    Solution is to: use the implicit column insertion statement, or use the explicit columns that follows schema 2.
 *    For example, with explicit columns that follows schema 2, we have the following logs:
 *
        I0701 18:41:58.354871 18315 test_dynamic_schema_update.cpp:2314] with schema 2 and explicit columns, total
 number of rows in block holder: 9 I0701 18:41:58.354882 18315 test_dynamic_schema_update.cpp:2316] with schema 2 and
 explicit columns, column names dumped in block holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek,
 flightDate, captain, rowCounter, code, status, column_new_1, column_new_2 I0701 18:41:58.354912 18315
 test_dynamic_schema_update.cpp:2321] with schema 2 and explicit columns, structure dumped in block holder: flightYear
 UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth UInt8 UInt8(size = 9), dayOfMonth UInt8 UInt8(size
 = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date UInt16(size = 9), captain Nullable(String) Nullable(size = 9,
 String(size = 9), UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code FixedString(4) FixedString(size = 9),
 status String String(size = 9), column_new_1 UInt64 UInt64(size = 9), column_new_2 String String(size = 9) I0701
 18:41:58.354925 18315 AggregatorLoader.cpp:405]  loader received insert query: insert into
 original_ontime_with_nullable_test (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`,
 `captain`, `rowCounter`, `code`, `status`, `column_new_1`, `column_new_2`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
 for table: original_ontime_with_nullable_test I0701 18:41:58.355044 18315 AggregatorLoader.cpp:56] insert query query
 expression is: INSERT INTO original_ontime_with_nullable_test (flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek,
 flightDate, captain, rowCounter, code, status, column_new_1, column_new_2) VALUES
 *
 */
TEST_F(AggregatorDynamicSchemaUpdateTesting,
       testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithExplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
            std::string query_table_creation = "create table " + table_name +
                " (\n"
                "     flightYear UInt16,\n"
                "     quarter UInt8,\n"
                "     flightMonth UInt8,\n"
                "     dayOfMonth UInt8,\n"
                "     dayOfWeek UInt8,\n"
                "     flightDate Date,\n"
                "     captain Nullable(String),\n"
                "     rowCounter UInt16,\n"
                "     code FixedString(4),\n"
                "     status String DEFAULT 'normal')\n"
                "\n"
                "ENGINE = ReplicatedMergeTree('" +
                zk_path +
                "', '{replica}') \n"
                "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                "SETTINGS index_granularity=8192; ";

            bool query_result = loader.executeTableCreation(table_name, query_table_creation);
            if (query_result) {
                LOG(INFO) << "created the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to create table for table: " << table_name;
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows_per_round = 3;
        std::string insert_query_following_schema_0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion0;
        size_t hash_code_version_0 = 0;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with version 0, table schema with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 0, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder_version_0 =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            hash_code_version_0 = table_definition.getSchemaHash();
            size_t version_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion0 = sqlBatchRequest.SerializeAsString();
            insert_query_following_schema_0 = prepareInsertQueryWithExplicitColumns(table_name);
        }
        // block holder becomes global across this test case.
        const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
        DB::Block global_block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

        // schema tracker is initialized along with the original table schema definition.
        std::shared_ptr<AugmentedTableSchemaUpdateTracker> schema_tracker_ptr =
            std::make_shared<AugmentedTableSchemaUpdateTracker>(table_name, table_definition, manager);

        std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = nullptr;
        {
            // push the message into protobuf reader.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion0, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "finish 1st batch of message consumption using schema 0 installed (step 3)";
        }

        // now update the schema, with one column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 0 --> schema 1, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 0 --> schema 1, successfully altered the table: " << table_name << "(step 4)";
            } else {
                LOG(ERROR) << "schema 0 --> schema 1, failed to alter table for table: " << table_name << "(step 4)";
            }
        }

        std::string serializedSqlBatchRequestInStringWithSchemaVersion1;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "with schema 1, the following is message with schema 0 but read by schema 1's reader";
            // NOTE: how to force the schema upgrade to be known to the reader, as the passed-in message is still with
            // the old schema Let's do the forceful upgrade to schema tracker.
            const nuclm::TableColumnsDescription& table_definition_version_1 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_1 = table_definition_version_1.getSchemaHash();
            schema_tracker_ptr->updateHashMapping(hash_code_version_1, table_definition_version_1);
            // but the message is still with hash code 0.
            size_t version_index = 1;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);

            serializedSqlBatchRequestInStringWithSchemaVersion1 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion1, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "finish 2nd batch of message consumption using schema 0 installed (step 7)";
        }

        // now further update the schema, with one more column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 1 --> schema 2, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 1 --> schema 2, successfully altered the table: " << table_name << "(step 8)";
            } else {
                LOG(ERROR) << "schema 1 --> schema 2, failed to alter table for table: " << table_name << "(step 8)";
            }
        }

        // now further add message that is with the old schema, schema 0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion2;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "With schema 2, the following is message with schema 0 but read by schema 2's reader";
            //? how to force the schema upgrade to be known to the reader, as the passed-in message is still with the
            //old schema?
            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_2 = table_definition_version_2.getSchemaHash();
            // NOTE: how to force the schema upgrade to be known to the proto-buf reader, as the passed-in message is
            // still with the old schema??? to forcefully update the schema
            schema_tracker_ptr->updateHashMapping(hash_code_version_2, table_definition_version_2);
            size_t version_index = 2;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion2 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion2, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "finish 3rd batch of message consumption using schema 0 installed";
        }
        size_t rounds = 3;
        {

            // for full table column definition that includes all of the columns with schema version 2.
            std::string query = prepareInsertQueryToClickHouseWithExplicitColumns(table_name);
            LOG(INFO) << "with schema 2, chosen table: " << table_name
                      << "with insert query with explicit columns: " << query;

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            size_t total_number_of_rows_holder = global_block_holder.rows();
            LOG(INFO) << "with schema 2 and explicit columns, total number of rows in block holder: "
                      << total_number_of_rows_holder;
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 2 and explicit columns, total number of columns in block holder: "
                      << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 2 and explicit columns, column names dumped in block holder : " << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, total_specified_number_of_rows_per_round * rounds);
            ASSERT_EQ(total_number_of_columns, (size_t)12);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 2 and explicit columns, structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, global_block_holder);
            ASSERT_TRUE(result);
        }

        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******with schema 2 and explicit columns, total rows inserted to table : " << table_name
                      << " is: " << total_row_count << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows_per_round * rounds);
        }

        // further perform row-based content validation
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter, \n"    /* 6. UInt16 */
                                         "     column_new_1, \n"  /* newly introduced column, UInt64*/
                                         "     column_new_2 \n"   /* newly introduced column, String*/
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
                ASSERT_EQ(number_of_columns, 9U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;
                auto& column_7 = assert_cast<DB::ColumnUInt64&>(*columns[7]);      // column_new_1
                auto& column_8 = assert_cast<DB::ColumnString&>(*columns[8]);      // column_new_2

                for (size_t i = 0; i < total_specified_number_of_rows_per_round * rounds; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);

                    // column_new_1, UInt64
                    uint64_t column_new_1_val = column_7.getData()[i];
                    // convert to int in order to show it on the console.
                    uint64_t expected_column_new_1_val = 0U;
                    LOG(INFO) << "retrieved column_new_1 value: " << (int)row_counter_val
                              << " with expected value: " << expected_column_new_1_val;
                    ASSERT_EQ(column_new_1_val, expected_column_new_1_val);

                    // column_new_2, String
                    auto column_8_real_string = column_8.getDataAt(i);
                    std::string column_8_real_string_value(column_8_real_string.data, column_8_real_string.size);
                    std::string expected_column_8_string_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_8_real_string_value
                              << " with expected value: " << expected_column_8_string_val;
                    ASSERT_EQ(column_8_real_string_value, expected_column_8_string_val);
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
 * Similar to the test case of:
 testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithExplicitColumns,
 * but with explicit columns being shuffled according to the schema.
 *
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
 *    From the test execution, if loader to clckhouse server insert query statement follows the one from kafka message
 *    that follows schema 0, then we will receive the following exception:
 *
 *    E0701 18:46:14.330003 18444 AggregatorLoader.cpp:115] Received exception Code: 10. DB::Exception: Received
 from 10.169.98.238:9000. DB::Exception: Not found column column_new_1 in block. There are only columns: flightYear,
 quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, status, code, rowCounter, captain.

 *    Solution is to: use the implicit column insertion statement, or use the explicit columns that follows schema 2.
 *    For example, with implicit columns that follows schema 2, we have the following logs:
 *
        I0701 18:54:38.953346 18749 test_dynamic_schema_update.cpp:2598] with schema 2 and implicit columns, total
 number of rows in block holder: 9 I0701 18:54:38.953356 18749 test_dynamic_schema_update.cpp:2600] with schema 2 and
 implicit columns, column names dumped in block holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek,
 flightDate, captain, rowCounter, code, status, column_new_1, column_new_2 I0701 18:54:38.953382 18749
 test_dynamic_schema_update.cpp:2605] with schema 2 and implicit columns, structure dumped in block holder: flightYear
 UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth UInt8 UInt8(size = 9), dayOfMonth UInt8 UInt8(size
 = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date UInt16(size = 9), captain Nullable(String) Nullable(size = 9,
 String(size = 9), UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code FixedString(4) FixedString(size = 9),
 status String String(size = 9), column_new_1 UInt64 UInt64(size = 9), column_new_2 String String(size = 9) I0701
 18:54:38.953392 18749 AggregatorLoader.cpp:405]  loader received insert query: insert into
 original_ontime_with_nullable_test values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) for table:
 original_ontime_with_nullable_test I0701 18:54:38.953450 18749 AggregatorLoader.cpp:56] insert query query expression
 is: INSERT INTO original_ontime_with_nullable_test VALUES

 */
TEST_F(AggregatorDynamicSchemaUpdateTesting,
       testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithExplicitColumnsAndShuffled) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
            std::string query_table_creation = "create table " + table_name +
                " (\n"
                "     flightYear UInt16,\n"
                "     quarter UInt8,\n"
                "     flightMonth UInt8,\n"
                "     dayOfMonth UInt8,\n"
                "     dayOfWeek UInt8,\n"
                "     flightDate Date,\n"
                "     captain Nullable(String),\n"
                "     rowCounter UInt16,\n"
                "     code FixedString(4),\n"
                "     status String DEFAULT 'normal')\n"
                "\n"
                "ENGINE = ReplicatedMergeTree('" +
                zk_path +
                "', '{replica}') \n"
                "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                "SETTINGS index_granularity=8192; ";

            bool query_result = loader.executeTableCreation(table_name, query_table_creation);
            if (query_result) {
                LOG(INFO) << "created the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to create table for table: " << table_name;
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows_per_round = 3;
        std::string insert_query_following_schema_0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion0;
        size_t hash_code_version_0 = 0;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with version 0, table schema with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 0, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder_version_0 =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            hash_code_version_0 = table_definition.getSchemaHash();
            size_t version_index = 0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessageShuffled(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion0 = sqlBatchRequest.SerializeAsString();
            insert_query_following_schema_0 = prepareInsertQueryWithExplicitColumnsShuffled(table_name);
        }
        // block holder becomes global across this test case.
        const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
        DB::Block global_block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

        // schema tracker is initialized along with the original table schema definition.
        std::shared_ptr<AugmentedTableSchemaUpdateTracker> schema_tracker_ptr =
            std::make_shared<AugmentedTableSchemaUpdateTracker>(table_name, table_definition, manager);

        std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = nullptr;
        {
            // push the message into protobuf reader.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion0, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "finish 1st batch of message consumption using schema 0 installed (step 3)";
        }

        // now update the schema, with one column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 0 --> schema 1, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 0 --> schema 1, successfully altered the table: " << table_name << "(step 4)";
            } else {
                LOG(ERROR) << "schema 0 --> schema 1, failed to alter table for table: " << table_name << "(step 4)";
            }
        }

        std::string serializedSqlBatchRequestInStringWithSchemaVersion1;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "with schema 1, the following is message with schema 0 but read by schema 1's reader";
            // NOTE: how to force the schema upgrade to be known to the reader, as the passed-in message is still with
            // the old schema Let's do the forceful upgrade to schema tracker.
            const nuclm::TableColumnsDescription& table_definition_version_1 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_1 = table_definition_version_1.getSchemaHash();
            schema_tracker_ptr->updateHashMapping(hash_code_version_1, table_definition_version_1);
            // but the message is still with hash code 0.
            size_t version_index = 1;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessageShuffled(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion1 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion1, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "finish 2nd batch of message consumption using schema 0 installed (step 7)";
        }

        // now further update the schema, with one more column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 1 --> schema 2, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 1 --> schema 2, successfully altered the table: " << table_name << "(step 8)";
            } else {
                LOG(ERROR) << "schema 1 --> schema 2, failed to alter table for table: " << table_name << "(step 8)";
            }
        }

        // now further add message that is with the old schema, schema 0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion2;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "With schema 2, the following is message with schema 0 but read by schema 2's reader";
            //? how to force the schema upgrade to be known to the reader, as the passed-in message is still with the
            //old schema?
            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_2 = table_definition_version_2.getSchemaHash();
            // NOTE: how to force the schema upgrade to be known to the proto-buf reader, as the passed-in message is
            // still with the old schema??? to forcefully update the schema
            schema_tracker_ptr->updateHashMapping(hash_code_version_2, table_definition_version_2);
            size_t version_index = 2;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessageShuffled(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion2 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion2, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            LOG(INFO) << "with schema 2, finish 3rd batch of message consumption using schema 0 installed";
        }
        size_t rounds = 3;
        {

            // use the implicit column definition for loader to clickhouse insert query statement.
            std::string query = prepareInsertQueryToClickHouseWithImplicitColumns(table_name);
            LOG(INFO) << "with schema 2, chosen table: " << table_name
                      << "with insert query with explicit columns: " << query;

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            size_t total_number_of_rows_holder = global_block_holder.rows();
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 2 and implicit columns, total number of rows in block holder: "
                      << total_number_of_rows_holder;
            LOG(INFO) << "with schema 2 and implicit columns, total number of columns in block holder: "
                      << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 2 and implicit columns, column names dumped in block holder : " << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, total_specified_number_of_rows_per_round * rounds);
            ASSERT_EQ(total_number_of_columns, (size_t)12);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 2 and implicit columns, structure dumped in block holder: " << structure;
            bool result = loader.load_buffer(table_name, query, global_block_holder);
            ASSERT_TRUE(result);
        }

        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******with schema 2 and implicit columns, total rows inserted to table : " << table_name
                      << " is: " << total_row_count << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows_per_round * rounds);
        }

        // further check with row-based content
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter, \n"    /* 6. UInt16 */
                                         "     column_new_1, \n"  /* newly introduced column, UInt64*/
                                         "     column_new_2 \n"   /* newly introduced column, String*/
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
                ASSERT_EQ(number_of_columns, 9U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;
                auto& column_7 = assert_cast<DB::ColumnUInt64&>(*columns[7]);      // column_new_1
                auto& column_8 = assert_cast<DB::ColumnString&>(*columns[8]);      // column_new_2

                for (size_t i = 0; i < total_specified_number_of_rows_per_round * rounds; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);

                    // column_new_1, UInt64
                    uint64_t column_new_1_val = column_7.getData()[i];
                    // convert to int in order to show it on the console.
                    uint64_t expected_column_new_1_val = 0U;
                    LOG(INFO) << "retrieved column_new_1 value: " << (int)row_counter_val
                              << " with expected value: " << expected_column_new_1_val;
                    ASSERT_EQ(column_new_1_val, expected_column_new_1_val);

                    // column_new_2, String
                    auto column_8_real_string = column_8.getDataAt(i);
                    std::string column_8_real_string_value(column_8_real_string.data, column_8_real_string.size);
                    std::string expected_column_8_string_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_8_real_string_value
                              << " with expected value: " << expected_column_8_string_val;
                    ASSERT_EQ(column_8_real_string_value, expected_column_8_string_val);
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
 * Similar to the test case of:
 testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithExplicitColumns
 * but at step 3, the most recent schema is not retrieved and thus we should expect to see the exception due to schema
 mismatch
 * when we issue the loader to CH server insert query statement that has the explicit columns and the columns do not
 fully match
 * the schema in the DB.
 *
 * On the other two options regarding the loader to CH server insert query statement that works, we either use implicit
 * insert-query statement, or use the explicit columns that matches the full schema at the DB, even though the
 constructed
 * block do not have the full columns (following schema 1) matching the latest schema at DB.
 *
 *
 *
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
 *    Follow the in-line comments on the different ways to issue the insert-query to clickhouse. We have the 3 options
 *    to issue the insert-query statement to clickhouse:
 *
 *    Option 1: std::string insert_query_to_ch = prepareInsertQueryToClickHouseWithExplicitColumns(table_name); that is,
 *    full column specification that follows schema version 2 (latest one), and block's column definition is with schema
 version 1
 *
 *    Option 2: std::string insert_query_to_ch = prepareInsertQueryToClickHouseWithImplicitColumns(table_name); that is,
 *    full column specification is used from DB that follows schema version 2 (latest one), and block's column
 definition is with
 *    schema version 1
 *
 *    Option 3: std::string insert_query_to_ch = prepareInsertQueryWithExplicitColumns (table_name); that is, we use
 schema version 0
 *    to specify the columns that we need to insert, which is different from the DB schema (with schema version 2), and
 block's
 *    column definition is with schema version 1
 *
 *    Option 4: std::string insert_query_to_ch =
 prepareInsertQueryToClickHouseWithExplicitColumnsUptoSchema1(table_name); that is,
 *    we use schema version 1 to specify the columns to be inserted, and block's column definition is with schema
 version 1,
 *    even though the DB backend schema is with schema version 2.
 *
 *    Thus only Option 3 (use the old explicit column definition) insert query that follows the stale column definition
 *    from the kafka message that has out-dated schema and not consistent with the block definitions (following schema
 version 1)
 *    will lead to the exception of: "NOT_FOUND_COLUMN_IN_BLOCK"
 *
 *
 *    Option    Block Construction with  Insert-Query To CH with    Backend DB stored Schema  Block Insertion OK or not
                Column definitions       Columns definitions
                following schema         following schema

      Option 1       V1                   V2                         V2                       OK
      Option 2       V1                   Implicit                   V2                       OK
      Option 3       V1                   V0                         V2                       NO
      Option 4       V1                   V1                         V2                       OK

 */
TEST_F(AggregatorDynamicSchemaUpdateTesting,
       testTableWithOriginalTableSchemaButWithMultipleSchemaUpdateAtInsertionsWithExplicitColumnsAndExceptionThrown) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorDynamicSchemaUpdateTesting::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorDynamicSchemaUpdateTesting::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::string table_name = "original_ontime_with_nullable_test";
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
            std::string query_table_creation = "create table " + table_name +
                " (\n"
                "     flightYear UInt16,\n"
                "     quarter UInt8,\n"
                "     flightMonth UInt8,\n"
                "     dayOfMonth UInt8,\n"
                "     dayOfWeek UInt8,\n"
                "     flightDate Date,\n"
                "     captain Nullable(String),\n"
                "     rowCounter UInt16,\n"
                "     code FixedString(4),\n"
                "     status String DEFAULT 'normal')\n"
                "\n"
                "ENGINE = ReplicatedMergeTree('" +
                zk_path +
                "', '{replica}') \n"
                "PARTITION BY flightDate PRIMARY KEY (flightYear, flightDate) ORDER BY(flightYear, flightDate) \n"
                "SETTINGS index_granularity=8192; ";

            bool query_result = loader.executeTableCreation(table_name, query_table_creation);
            if (query_result) {
                LOG(INFO) << "created the table: " << table_name;
            } else {
                LOG(ERROR) << "failed to create table for table: " << table_name;
            }
        }

        size_t sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "sleep  " << sleeptime_on_ms << " milliseconds before to load data to the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));

        size_t total_specified_number_of_rows_per_round = 3;
        std::string insert_query_following_schema_0;
        std::string serializedSqlBatchRequestInStringWithSchemaVersion0;
        size_t hash_code_version_0 = 0;

        std::vector<uint16_t> flight_year_array;
        std::vector<uint8_t> quarter_array;
        std::vector<uint8_t> flight_month_array;
        std::vector<uint8_t> day_of_month_array;
        std::vector<uint8_t> day_of_week_array;
        std::vector<uint16_t> flight_date_array;
        std::vector<std::optional<std::string>> captain_array;
        std::vector<uint16_t> row_counter_array;
        std::vector<std::string> code_array;
        std::vector<std::string> status_array;

        srand(time(NULL)); // create a random seed.
        // to make sure that the data will be ordered by the array sequence.
        int random_int_val = std::rand() % 10000000;

        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with version 0, table schema with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " with version 0, size of default columns count being: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 1);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " with version 0, size of ordinary columns count being: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 9);
            nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                table_definition.getFullColumnTypesAndNamesDefinition();

            DB::Block block_holder_version_0 =
                nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

            hash_code_version_0 = table_definition.getSchemaHash();
            size_t version_index = 0;

            LOG(INFO) << "with version 0, hash code to be used to construct message is: " << hash_code_version_0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion0 = sqlBatchRequest.SerializeAsString();
            insert_query_following_schema_0 = prepareInsertQueryWithExplicitColumns(table_name);
        }
        // block holder becomes global across this test case.
        const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
        DB::Block global_block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());

        // schema tracker is initialized along with the original table schema definition.
        std::shared_ptr<AugmentedTableSchemaUpdateTracker> schema_tracker_ptr =
            std::make_shared<AugmentedTableSchemaUpdateTracker>(table_name, table_definition, manager);

        std::shared_ptr<nuclm::ProtobufBatchReader> batchReader = nullptr;
        {
            // push the message into protobuf reader.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion0, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            // inspect the block.
            size_t total_number_of_rows_holder = global_block_holder.rows();
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 0 and explicit columns of 10 columns, total number of rows in block holder: "
                      << total_number_of_rows_holder;
            LOG(INFO) << "with schema 0 and explicit columns of 10 columns, total number of columns in block holder: "
                      << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 0 and explicit columns of 10 columns, column names dumped in block holder : "
                      << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, (size_t)3);
            ASSERT_EQ(total_number_of_columns, (size_t)10);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 0 and explicit columns 0f 10 columns, structure dumped in block holder: "
                      << structure;

            LOG(INFO) << "finish 1st batch of message consumption using schema 0 installed (step 3)";
        }

        // now update the schema, with one column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 0 --> schema 1, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_1 UInt64 DEFAULT '0' AFTER status ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 0 --> schema 1, successfully altered the table: " << table_name << "(step 4)";
            } else {
                LOG(ERROR) << "schema 0 --> schema 1, failed to alter table for table: " << table_name << "(step 4)";
            }
        }

        std::string serializedSqlBatchRequestInStringWithSchemaVersion1;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.
            LOG(INFO) << "with schema 1, the following is message with schema 0 but read by schema 1's reader";
            // NOTE: how to force the schema upgrade to be known to the reader, as the passed-in message is still with
            // the old schema Let's do the forceful upgrade to schema tracker.
            const nuclm::TableColumnsDescription& table_definition_version_1 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_1 = table_definition_version_1.getSchemaHash();
            schema_tracker_ptr->updateHashMapping(hash_code_version_1, table_definition_version_1);
            // but the message is still with hash code 0.
            size_t version_index = 1;

            // still use version 0, even though hash mapping has been updated
            LOG(INFO) << "with version 1, hash code to be used to construct message is still with version 0: "
                      << hash_code_version_0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion1 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion1, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            // Inspect the block after schema update to version 1
            size_t total_number_of_rows_holder = global_block_holder.rows();
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 1 and explicit columns of 10 columns, total number of rows in block holder: "
                      << total_number_of_rows_holder;
            LOG(INFO) << "with schema 1 and explicit columns of 10 columns, total number of columns in block holder: "
                      << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 1 and explicit columns of 10 columns, column names dumped in block holder : "
                      << names_holder;

            // NOTE: should this be 6, as this is increasing....
            ASSERT_EQ(total_number_of_rows_holder, (size_t)6);
            ASSERT_EQ(total_number_of_columns, (size_t)11);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 1 and explicit columns 0f 10 columns, structure dumped in block holder: "
                      << structure;

            LOG(INFO) << "finish 2nd batch of message consumption using schema 0 installed (step 5)";
        }

        // now further update the schema, with one more column and default value added to the end.
        sleeptime_on_ms = SLEEP_TIME_IN_MILLISECONDS;
        LOG(INFO) << "schema 1 --> schema 2, sleep " << sleeptime_on_ms
                  << " milliseconds before to alter the table: " << table_name;

        std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
        {
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();
            std::string query_table_drop =
                "ALTER TABLE " + table_name + " ADD COLUMN column_new_2 String DEFAULT '' AFTER column_new_1 ";
            bool query_result = loader.executeTableCreation(table_name, query_table_drop);
            if (query_result) {
                LOG(INFO) << "schema 1 --> schema 2, successfully altered the table: " << table_name << "(step 6)";
            } else {
                LOG(ERROR) << "schema 1 --> schema 2, failed to alter table for table: " << table_name << "(step 6)";
            }
        }

        // now further add message that is with the old schema, schema 0;
        // and we do not update the latest schema.
        std::string serializedSqlBatchRequestInStringWithSchemaVersion2;
        {
            // then perform the message consumption again
            // push the message into protobuf reader.

            // NOTE: the follow schema update code is commented out. so that the reader will use the schema that is
            // updated after the first table alter is done.

            // LOG(INFO) << "With schema 2, the following is message with schema 0 but read by schema 2's reader";
            //? how to force the schema upgrade to be known to the reader, as the passed-in message is still with the
            //old schema?
            const nuclm::TableColumnsDescription& table_definition_version_2 =
                manager.getTableColumnsDefinition(table_name, false);
            size_t hash_code_version_2 = table_definition_version_2.getSchemaHash();
            // NOTE: how to force the schema upgrade to be known to the proto-buf reader, as the passed-in message is
            // still with the old schema???

            // To forcefully update the schema
            schema_tracker_ptr->updateHashMapping(hash_code_version_2, table_definition_version_2);
            size_t version_index = 2;

            // still use version 0, even though hash mapping has been updated. version 0 has been used earlier in this
            // test case, and as a result, hash version 0 points to schema version 1. But now version changes to version
            // 2, and thus hash version 0 does not point to latest schema now. It will force the fetching and then hash
            // version 0 will point to version 2 of the schema, which is the latest schema version.
            LOG(INFO) << "with version 2, hash code to be used to construct message is still version 0: "
                      << hash_code_version_0;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest = generateBatchMessage(
                random_int_val, table_name, hash_code_version_0, total_specified_number_of_rows_per_round,
                version_index, false, flight_year_array, quarter_array, flight_month_array, day_of_month_array,
                day_of_week_array, flight_date_array, captain_array, row_counter_array, code_array, status_array);
            serializedSqlBatchRequestInStringWithSchemaVersion2 = sqlBatchRequest.SerializeAsString();

            // still use the old schema tracker with version 1, to mimic the buffer code in the aggregator.
            batchReader = std::make_shared<nuclm::ProtobufBatchReader>(
                serializedSqlBatchRequestInStringWithSchemaVersion2, schema_tracker_ptr, global_block_holder, context);

            bool serialization_status = batchReader->read();
            ASSERT_TRUE(serialization_status);

            // Inspect the block after schema update to version 1
            size_t total_number_of_rows_holder = global_block_holder.rows();
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 2 and explicit columns of 10 columns, total number of rows in block holder: "
                      << total_number_of_rows_holder;
            LOG(INFO) << "with schema 2 and explicit columns of 10 columns, total number of columns in block holder: "
                      << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 2 and explicit columns of 10 columns, column names dumped in block holder : "
                      << names_holder;

            // NOTE: The row number still get increased and the number of the columns will point to the latest version,
            // version 2.
            ASSERT_EQ(total_number_of_rows_holder, (size_t)9);
            ASSERT_EQ(total_number_of_columns, (size_t)12);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 1 and explicit columns 0f 10 columns, structure dumped in block holder: "
                      << structure;

            LOG(INFO) << "finish 3rd batch of message consumption using schema 0 installed (step 7)";
        }
        size_t rounds = 3;
        {
            // NOTE: in all of the options below, the constructed block only has the column definitions that follow
            // schema 1, not latest schema 2.

            // for full table insertion that includes all of the columns, which should include the two new schema
            // columns. or just use the implicit columns Option 1: works and no exception thrown, even though we have
            // the columns that are still missing according to the latest schema in DB.
            // I0701 17:27:02.731815 15401 test_dynamic_schema_update.cpp:2824] with schema 2 and explicit columns,
            // structure dumped in block holder: flightYear UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9),
            // flightMonth UInt8 UInt8(size = 9), dayOfMonth UInt8 UInt8(size = 9), dayOfWeek UInt8 UInt8(size = 9),
            // flightDate Date UInt16(size = 9), captain Nullable(String) Nullable(size = 9, String(size = 9),
            // UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code FixedString(4) FixedString(size = 9), status
            // String String(size = 9), column_new_1 UInt64 UInt64(size = 9)
            //
            // std::string insert_query_to_ch = prepareInsertQueryToClickHouseWithExplicitColumns(table_name);

            // Option 2: implicit schema. works and no exceptin thrown.
            //
            // I0701 17:33:04.396054 15529 test_dynamic_schema_update.cpp:2825] with schema 2 and explicit columns,
            // total number of rows in block holder: 9 I0701 17:33:04.396068 15529 test_dynamic_schema_update.cpp:2827]
            // with schema 2 and explicit columns, column names dumped in block holder : flightYear, quarter,
            // flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code, status, column_new_1 I0701
            // 17:33:04.396100 15529 test_dynamic_schema_update.cpp:2832] with schema 2 and explicit columns, structure
            // dumped in block holder: flightYear UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth
            // UInt8 UInt8(size = 9), dayOfMonth UInt8 UInt8(size = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date
            // UInt16(size = 9), captain Nullable(String) Nullable(size = 9, String(size = 9), UInt8(size = 9)),
            // rowCounter UInt16 UInt16(size = 9), code FixedString(4) FixedString(size = 9), status String String(size =
            // 9), column_new_1 UInt64 UInt64(size = 9) I0701 17:33:04.396114 15529 AggregatorLoader.cpp:405]  loader
            // received insert query: insert into original_ontime_with_nullable_test values (?, ?, ?, ?, ?, ?, ?, ?, ?,
            // ?, ?, ?) for table: original_ontime_with_nullable_test
            //
            // std::string insert_query_to_ch = prepareInsertQueryToClickHouseWithImplicitColumns(table_name);

            // Option 3: use the insert query statement that comes with the kafka message that is out-dated in terms of
            // schema. I0701 17:42:35.061540 16397 test_dynamic_schema_update.cpp:2834] with schema 2 and explicit
            // columns, total number of rows in block holder: 9
            // I0701 17:42:35.061551 16397 test_dynamic_schema_update.cpp:2836] with schema 2 and explicit columns,
            // column names dumped in block holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate,
            // captain, rowCounter, code, status, column_new_1 I0701 17:42:35.061581 16397
            // test_dynamic_schema_update.cpp:2841] with schema 2 and explicit columns, structure dumped in block holder:
            // flightYear UInt16 UInt16(size = 9), quarter UInt8 UInt8(size = 9), flightMonth UInt8 UInt8(size = 9),
            // dayOfMonth UInt8 UInt8(size = 9), dayOfWeek UInt8 UInt8(size = 9), flightDate Date UInt16(size = 9),
            // captain Nullable(String) Nullable(size = 9, String(size = 9), UInt8(size = 9)), rowCounter UInt16
            // UInt16(size = 9), code FixedString(4) FixedString(size = 9), status String String(size = 9), column_new_1
            // UInt64 UInt64(size = 9) I0701 17:42:35.061596 16397 AggregatorLoader.cpp:405]  loader received insert
            // query: insert into original_ontime_with_nullable_test (`flightYear`, `quarter`, `flightMonth`,
            // `dayOfMonth`, `dayOfWeek`, `flightDate`, `captain`, `rowCounter`, `code`, `status`) values (?, ?, ?, ?, ?,
            // ?, ?, ?, ?,?) for table: original_ontime_with_nullable_test I0701 17:42:35.061712 16397
            // AggregatorLoader.cpp:56] insert query query expression is: INSERT INTO original_ontime_with_nullable_test
            // (flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code, status)
            // VALUES
            //
            // and we receive the exception:
            //
            // E0701 17:42:35.097337 16397 AggregatorLoader.cpp:115] Received exception Code: 10. DB::Exception:
            // Received from 10.169.98.238:9000. DB::Exception: Not found column column_new_1 in block. There are only
            // columns: flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code,
            // status.
            // std::string insert_query_to_ch = prepareInsertQueryWithExplicitColumns (table_name);

            // Option 4: use the same schema as the constructed block, that is, following schema 1, not the latest one
            // schema 2, and we have loader to clickhouse insert-query statement following the explicit column up to
            // schema 1. I0702 00:22:56.170050  2664 test_dynamic_schema_update.cpp:2940] with schema 2 and explicit
            // columns, total number of rows in block holder: 9 I0702 00:22:56.170065  2664
            // test_dynamic_schema_update.cpp:2942] with schema 2 and explicit columns, column names dumped in block
            // holder : flightYear, quarter, flightMonth, dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code,
            // status, column_new_1 I0702 00:22:56.170097  2664 test_dynamic_schema_update.cpp:2947] with schema 2 and
            // explicit columns, structure dumped in block holder: flightYear UInt16 UInt16(size = 9), quarter UInt8
            // UInt8(size = 9), flightMonth UInt8 UInt8(size = 9), dayOfMonth UInt8 UInt8(size = 9), dayOfWeek UInt8
            // UInt8(size = 9), flightDate Date UInt16(size = 9), captain Nullable(String) Nullable(size = 9, String(size
            // = 9), UInt8(size = 9)), rowCounter UInt16 UInt16(size = 9), code FixedString(4) FixedString(size = 9),
            // status String String(size = 9), column_new_1 UInt64 UInt64(size = 9) I0702 00:22:56.170114  2664
            // AggregatorLoader.cpp:405]  loader received insert query: insert into original_ontime_with_nullable_test
            // (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, `dayOfWeek`, `flightDate`, `captain`, `rowCounter`,
            // `code`, `status`, `column_new_1`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) for table:
            // original_ontime_with_nullable_test I0702 00:22:56.170233  2664 AggregatorLoader.cpp:56] insert query query
            // expression is: INSERT INTO original_ontime_with_nullable_test (flightYear, quarter, flightMonth,
            // dayOfMonth, dayOfWeek, flightDate, captain, rowCounter, code, status, column_new_1) VALUES
            //
            //

            // Since the current global block holder has version 2, we will need to construct the query statement that
            // need to cover all of the columns. std::string insert_query_to_ch =
            // prepareInsertQueryToClickHouseWithExplicitColumnsUptoSchema1(table_name);
            std::string insert_query_to_ch = prepareInsertQueryToClickHouseWithExplicitColumns(table_name);
            LOG(INFO) << "with schema 2, chosen table: " << table_name
                      << "with insert query with explicit columns: " << insert_query_to_ch;

            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            // NOTE: The columns have update to column_new_2 already
            size_t total_number_of_rows_holder = global_block_holder.rows();
            LOG(INFO) << "with schema 2 and schema update to latest version, total number of rows in block holder: "
                      << total_number_of_rows_holder;
            size_t total_number_of_columns = global_block_holder.columns();
            LOG(INFO) << "with schema 2 and schema update to latest version, total number of columns in block holder: "
                      << total_number_of_columns;

            std::string names_holder = global_block_holder.dumpNames();
            LOG(INFO) << "with schema 2 and schema update to latest version, column names dumped in block holder : "
                      << names_holder;

            ASSERT_EQ(total_number_of_rows_holder, total_specified_number_of_rows_per_round * rounds);
            ASSERT_EQ(total_number_of_columns, (size_t)12);

            std::string structure = global_block_holder.dumpStructure();
            LOG(INFO) << "with schema 2 and schema update to latest version , structure dumped in block holder: "
                      << structure;
            // NOTE: the insert purposely has 11 columns explicitly, even though the columns have 12 columns. Does it
            // work?
            bool result = loader.load_buffer(table_name, insert_query_to_ch, global_block_holder);
            ASSERT_TRUE(result);

            // It is OK to be loaded, even though the DB schema has evolved to version 2.
        }

        // get current table row count
        {
            size_t total_row_count = getRowCountFromTable(table_name, manager, context);
            LOG(INFO) << "******with schema 2 and explicit columns of 11, total rows inserted to table : " << table_name
                      << " is: " << total_row_count << "*******";
            ASSERT_EQ(total_row_count, total_specified_number_of_rows_per_round * rounds);
        }

        // further examine row-based content
        {
            std::string table_being_queried = "default." + table_name;
            std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                         "     quarter,\n"        /* 1. UInt8 */
                                         "     flightDate,\n"     /* 2. Date */
                                         "     captain,\n"        /*  3. Nullable(String) */
                                         "     code,\n"           /*  4.  FixedString(4) */
                                         "     status, \n"        /* 5. String, with default value of "normal" */
                                         "     rowCounter, \n"    /* 6. UInt16 */
                                         "     column_new_1, \n"  /* newly introduced column, UInt64*/
                                         "     column_new_2 \n"   /* newly introduced column, String*/
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
                ASSERT_EQ(number_of_columns, 9U);

                auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
                auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
                auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
                auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
                auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
                auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;
                auto& column_6 = assert_cast<DB::ColumnUInt16&>(*columns[6]);      // rowCounter;
                auto& column_7 = assert_cast<DB::ColumnUInt64&>(*columns[7]);      // column_new_1
                auto& column_8 = assert_cast<DB::ColumnString&>(*columns[8]);      // column_new_2

                for (size_t i = 0; i < total_specified_number_of_rows_per_round * rounds; i++) {
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

                    // rowCounter, UInt16
                    uint16_t row_counter_val = column_6.getData()[i];
                    // convert to int in order to show it on the console.
                    LOG(INFO) << "retrieved row-counter value: " << (int)row_counter_val
                              << " with expected value: " << (int)row_counter_array[i];
                    ASSERT_EQ(row_counter_val, row_counter_array[i]);

                    // column_new_1, UInt64
                    uint64_t column_new_1_val = column_7.getData()[i];
                    // convert to int in order to show it on the console.
                    uint64_t expected_column_new_1_val = 0U;
                    LOG(INFO) << "retrieved column_new_1 value: " << (int)row_counter_val
                              << " with expected value: " << expected_column_new_1_val;
                    ASSERT_EQ(column_new_1_val, expected_column_new_1_val);

                    // column_new_2, String
                    auto column_8_real_string = column_8.getDataAt(i);
                    std::string column_8_real_string_value(column_8_real_string.data, column_8_real_string.size);
                    std::string expected_column_8_string_val("");
                    LOG(INFO) << "retrieved column_new_2 value: " << column_8_real_string_value
                              << " with expected value: " << expected_column_8_string_val;
                    ASSERT_EQ(column_8_real_string_value, expected_column_8_string_val);
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
