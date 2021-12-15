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

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>
#include <Aggregator/TableColumnsDescription.h>
#include <Aggregator/AggregatorLoaderManager.h>

#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/SerializableDataTypeLowCardinality.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>

#include <nucolumnar/aggregator/v1/nucolumnaraggregator.pb.h>
#include <nucolumnar/datatypes/v1/columnartypes.pb.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

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

class SerializerForProtobufWithLowCardinalityRelatedTest : public ::testing::Test {
  protected:
    // Per-test-suite set-up
    // Called before the first test in this test suite
    // Can be omitted if not needed
    // NOTE: this method is not called SetUpTestSuite, as what is described in:
    // https://github.com/google/googletest/blob/master/googletest/docs/advanced.md
    /// Fxied from: https://stackoverflow.com/questions/54468799/google-test-using-setuptestsuite-doesnt-seem-to-work
    static void SetUpTestCase() {
        LOG(INFO) << "SetUpTestCase invoked..." << std::endl;
        shared_context = new ContextWrapper();
    }

    // Per-test-suite tear-down
    // Called after the last test in this test suite.
    // Can be omitted if not needed
    static void TearDownTestCase() {
        LOG(INFO) << "TearDownTestCase invoked..." << std::endl;
        delete shared_context;
        shared_context = nullptr;
    }

    // Define per-test set-up logic as usual
    virtual void SetUp() {
        //...
    }

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    static ContextWrapper* shared_context;
};

ContextWrapper* SerializerForProtobufWithLowCardinalityRelatedTest::shared_context = nullptr;

/**
 * For primitive types: family name and name are identical
 *
 *  family name identified for FixedString(8) is: FixedString
 *  name identified for FixedString(8) is: FixedString(8)
 *
 *  family name identified for Nullable (String) is: Nullable
 *   name identified for Nullable (String)) is: Nullable(String)
 *
 */
TEST_F(SerializerForProtobufWithLowCardinalityRelatedTest, testSerializerConstructor) {
    bool failed = false;
    try {
        auto& data_type_factory = nuclm::SerializableDataTypeFactory::instance();
        auto typeFromString_1 = [&data_type_factory](const std::string& str) { return data_type_factory.get(str); };
        nuclm::SerializableDataTypePtr serializableDataTypePtr = typeFromString_1("String");

        auto& true_data_type_factory = DB::DataTypeFactory::instance();
        auto trueTypeFromString_2 = [&true_data_type_factory](const std::string& str) {
            return true_data_type_factory.get(str);
        };
        DB::DataTypePtr trueDataTypePtr = trueTypeFromString_2("String");
        nuclm::SerializableDataTypePtr resulted_ptr =
            std::make_shared<nuclm::SerializableDataTypeLowCardinality>(serializableDataTypePtr, trueDataTypePtr);

        LOG(INFO) << "constructed serializer object has name: " << resulted_ptr->getName();
        LOG(INFO) << "constructed serializer object has family name: " << resulted_ptr->getFamilyName();
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}
/**
 * one row, with first UInt64 value to be NULL.
 *
 * create table simple_event_18 (
     Counter UInt64,
     Host LowCardinality (String),
     Colo LowCardinality (String))

     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_18', '{replica}') ORDER BY(Host, Counter)
 SETTINGS index_granularity=8192;

     insert into simple_event_18 (`Counter`, `Host`, `Colo`) VALUES (2000, 'graphdb-1', 'LVS');
 */
TEST_F(SerializerForProtobufWithLowCardinalityRelatedTest, testSerializationOnLowCardinalityumberWithSingleRow) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = SerializerForProtobufWithLowCardinalityRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = SerializerForProtobufWithLowCardinalityRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table = "simple_event_18";
    std::string sql = "insert into simple_event_18 values(?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
    {
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(1);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("a");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("x");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counter", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "LowCardinality(String)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "LowCardinality(String)"));

    bool failed = false;
    try {
        nuclm::ColumnSerializers serializers =
            nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
        size_t total_number_of_columns = serializers.size();
        for (size_t i = 0; i < total_number_of_columns; i++) {
            std::string family_name = serializers[i]->getFamilyName();
            std::string name = serializers[i]->getName();

            LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
            LOG(INFO) << "name identified for Column: " << i << " is: " << name;
        }

        DB::Block block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
        std::string names = block_holder.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(table, table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        batchReader.read();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names;

        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder: " << structure;
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * LowCardinality on Integer, and String, but not on Date.
 *
 * create table simple_event_20 (
     Counter UInt64,
     Host LowCardinality (String),
     Colo LowCardinality (String),
     FlightDate Date)

   ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_20', '{replica}') ORDER BY(Host, Counter)
 SETTINGS index_granularity=8192;

   insert into simple_event_20 (`Counter`, `Host`, `Colo`, `FlightDate`) VALUES (2000, 'graphdb-1', 'LVS',
 '2020-06-30');

 */
TEST_F(SerializerForProtobufWithLowCardinalityRelatedTest, testSerializationOnNumberAndStringMultipleRows) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = SerializerForProtobufWithLowCardinalityRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = SerializerForProtobufWithLowCardinalityRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table = "simple_event_20";
    std::string sql = "insert into simple_event_20 values(?, ?, ?, ?)"; // with date at the end.
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;

    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(1);

        // value 2, NOT NULL
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("a");

        // value 3, NOT NULL
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("x");

        // date
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        val4->mutable_timestamp()->set_milliseconds(10);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
        pval4->CopyFrom(*val4);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(2);

        // value 2, NOT NULL
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("b");

        // value 3, NOT NULL
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("y");

        // date
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        val4->mutable_timestamp()->set_milliseconds(10);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
        pval4->CopyFrom(*val4);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(2);
        // value 2, not null.
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("c");
        // value 3, not null.
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("z");

        // date
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        val4->mutable_timestamp()->set_milliseconds(10);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
        pval4->CopyFrom(*val4);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counter", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "LowCardinality(String)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "LowCardinality(String)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("FlightDate", "Date"));

    bool failed = false;
    try {
        nuclm::ColumnSerializers serializers =
            nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
        size_t total_number_of_columns = serializers.size();
        for (size_t i = 0; i < total_number_of_columns; i++) {
            std::string family_name = serializers[i]->getFamilyName();
            std::string name = serializers[i]->getName();

            LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
            LOG(INFO) << "name identified for Column: " << i << " is: " << name;
        }

        DB::Block block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
        std::string names = block_holder.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(table, table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        batchReader.read();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names;

        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder: " << structure;
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
