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

#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/AggregatorLoaderManager.h>

#include <Core/Defines.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/ColumnsDescription.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>

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

class AggregatorLoaderDateTime64SerializerRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderDateTime64SerializerRelatedTest::shared_context = nullptr;

/** Load one row will all of the columns are explicitly available. And the result is:
 *  SELECT *
            FROM simple_event_56

        ┌─Counter─┬───────────ShortTime─┬────────────────LongTime─┬────────LongTimeNullable─┐
        │  123456 │ 2021-01-06 19:04:21 │ 2021-01-06 19:04:21.048 │ 2021-01-06 20:04:21.048 │
        └─────────┴─────────────────────┴─────────────────────────┴─────────────────────────┘
 */
TEST_F(AggregatorLoaderDateTime64SerializerRelatedTest, InsertARowWithoutNullableDateTime64) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDateTime64SerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDateTime64SerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into simple_event_56 values(?, ?, ?, ?);";
    std::string table_name = "simple_event_56";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_event_56 values(?, ?, ?, ?);";
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

    srand(time(NULL));
    {
        // value 1, Counter
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        long rand_long_val = rand() % 1000000;
        val1->set_long_value(rand_long_val);

        // value 2, short time
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        {
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            ts->set_milliseconds(ms.count());
            val2->set_allocated_timestamp(ts);
        }

        // value 3, long time,
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        {
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            ts->set_milliseconds(ms.count());
            val3->set_allocated_timestamp(ts);
        }

        // value 4, long time nullable
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        {
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            ts->set_milliseconds(ms.count());
            val4->set_allocated_timestamp(ts);
        }

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

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counter", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("ShortTime", "DateTime"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("LongTime", "DateTime64(3)"));
    table_definition.addColumnDescription(
        nuclm::TableColumnDescription("LongTimeNullable", "Nullable(DateTime64(3, 'America/Phoenix'))"));

    DB::Block block_holder =
        nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
    std::string names = block_holder.dumpNames();
    LOG(INFO) << "column names dumped : " << names;

    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                           context);

    nuclm::ColumnSerializers serializers =
        nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
    size_t total_number_of_columns = serializers.size();
    for (size_t i = 0; i < total_number_of_columns; i++) {
        std::string family_name = serializers[i]->getFamilyName();
        std::string name = serializers[i]->getName();

        LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
        LOG(INFO) << "name identified for Column: " << i << " is: " << name;
    }

    batchReader.read();

    size_t total_number_of_rows_holder = block_holder.rows();
    LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
    std::string names_holder = block_holder.dumpNames();
    LOG(INFO) << "column names dumped in block holder : " << names;

    std::string structure = block_holder.dumpStructure();
    LOG(INFO) << "structure dumped in block holder: " << structure;
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);
}

/**
 *  With the nullable column to be set as null. The result is for the inserted columns:
 *
        ┌─Counter─┬───────────ShortTime─┬────────────────LongTime─┬─LongTimeNullable─┐
        │  123456 │ 2021-01-06 19:13:49 │ 2021-01-06 19:13:49.363 │             ᴺᵁᴸᴸ │
        └─────────┴─────────────────────┴─────────────────────────┴──────────────────┘
 *
 */
TEST_F(AggregatorLoaderDateTime64SerializerRelatedTest, InsertARowWithNullableDateTime64) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDateTime64SerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDateTime64SerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into simple_event_56 values(?, ?, ?, ?);";
    std::string table_name = "simple_event_56";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_event_56 values(?, ?, ?, ?);";
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

    srand(time(NULL));
    {
        // value 1, Counter
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        long rand_long_val = rand() % 1000000;
        val1->set_long_value(rand_long_val);

        // value 2, short time
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        {
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            ts->set_milliseconds(ms.count());
            val2->set_allocated_timestamp(ts);
        }

        // value 3, long time,
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        {
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            ts->set_milliseconds(ms.count());
            val3->set_allocated_timestamp(ts);
        }

        // value 4, long time nullable
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        {
            nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;
            val4->set_null_value(nullValueP);
        }

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

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counter", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("ShortTime", "DateTime"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("LongTime", "DateTime64(3)"));
    table_definition.addColumnDescription(
        nuclm::TableColumnDescription("LongTimeNullable", "Nullable(DateTime64(3, 'America/Phoenix'))"));

    DB::Block block_holder =
        nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
    std::string names = block_holder.dumpNames();
    LOG(INFO) << "column names dumped : " << names;

    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                           context);

    nuclm::ColumnSerializers serializers =
        nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
    size_t total_number_of_columns = serializers.size();
    for (size_t i = 0; i < total_number_of_columns; i++) {
        std::string family_name = serializers[i]->getFamilyName();
        std::string name = serializers[i]->getName();

        LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
        LOG(INFO) << "name identified for Column: " << i << " is: " << name;
    }

    batchReader.read();

    size_t total_number_of_rows_holder = block_holder.rows();
    LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
    std::string names_holder = block_holder.dumpNames();
    LOG(INFO) << "column names dumped in block holder : " << names;

    std::string structure = block_holder.dumpStructure();
    LOG(INFO) << "structure dumped in block holder: " << structure;
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
