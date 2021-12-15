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
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

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

class AggregatorLoaderSerializerRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderSerializerRelatedTest::shared_context = nullptr;

/**
 * For primitive types: family name and name are identical
 *
 *  family name identified for FixedString(8) is: FixedString
 *  name identified for FixedString(8) is: FixedString(8)
 *
 *  family name identified for Nullable (String) is: Nullable
 *   name identified for Nullable (String)) is: Nullable(String)
 */

TEST_F(AggregatorLoaderSerializerRelatedTest, InsertARowToCHServerWithBlockConstructedFromMessageWithSingleRow) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_5";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_event_5;
    std::string query = "insert into " + table_name + " (`Count`, `Host`, `Colo`) VALUES (1000, 'graphdb-1', 'LVS');";

    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into " + table_name + " values(?, ?, ?)";
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
    int rand_init_val = rand() % 10000000;
    long long_count_val = rand_init_val;
    std::string host_name = "nudata-abc-9";
    std::string colo_name = "nudata-xyz-9";

    {
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(rand_init_val);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value(host_name);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value(colo_name);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Count", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));

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
    ASSERT_EQ(total_number_of_columns, 3U);

    for (size_t i = 0; i < total_number_of_columns; i++) {
        std::string family_name = serializers[i]->getFamilyName();
        std::string name = serializers[i]->getName();

        LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
        LOG(INFO) << "name identified for Column: " << i << " is: " << name;
    }

    batchReader.read();

    size_t total_number_of_rows_holder = block_holder.rows();
    LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
    ASSERT_EQ(total_number_of_rows_holder, 1U);

    std::string names_holder = block_holder.dumpNames();
    LOG(INFO) << "column names dumped in block holder : " << names;

    std::string structure = block_holder.dumpStructure();
    LOG(INFO) << "structure dumped in block holder: " << structure;
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Count , \n" /*  0. UInt64*/
                                 "     Host,\n"      /* 1.  String */
                                 "     Colo \n"      /* 2. String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (Host, Count)";

    auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
        size_t total_number_of_rows_in_query_result = query_result.rows();
        LOG(INFO) << "retrieved query result has number of the rows: " << total_number_of_rows_in_query_result;

        const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
        int column_index = 0;

        for (auto& p : columns_with_type_and_name) {
            LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                      << " column type: " << p.type->getName() << " column name: " << p.name
                      << " number of rows: " << p.column->size();
        }

        DB::MutableColumns columns = query_result.mutateColumns();

        size_t number_of_columns = columns.size();
        ASSERT_EQ(number_of_columns, 3U);

        if (total_number_of_rows_in_query_result != 1) {
            LOG(WARNING) << "retrieved query result has number of the rows > expected 1 row";
        }

        ASSERT_EQ(total_number_of_rows_in_query_result, 1U);

        auto& column_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]); // Count
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]); // Host
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]); // Colo

        // Count retrieval
        uint64_t val = column_0.getData()[0];
        LOG(INFO) << "retrieved Count: " << val << " with expected value: " << long_count_val;
        ASSERT_EQ(val, (uint64_t)long_count_val);

        // Host value retrieval.
        auto column_1_string = column_1.getDataAt(0); // We only have 1 row.
        std::string column_1_real_string(column_1_string.data, column_1_string.size);
        LOG(INFO) << "retrieved host name is: " << column_1_real_string << " with expected value: " << host_name;
        ASSERT_EQ(column_1_real_string, host_name);

        // Colo value retrieval
        auto column_2_real_string = column_2.getDataAt(0);
        std::string column_2_real_string_value(column_2_real_string.data, column_2_real_string.size);
        LOG(INFO) << "retrieved colo name: " << column_2_real_string_value << " with expected value: " << colo_name;
        ASSERT_EQ(column_2_real_string_value, colo_name);
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

TEST_F(AggregatorLoaderSerializerRelatedTest, InsertARowToCHServerWithBlockConstructedFromMessageWithMultipleRows) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_5";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_event_5;
    std::string query = "insert into " + table_name + " (`Count`, `Host`, `Colo`) VALUES (1000, 'graphdb-1', 'LVS');";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into " + table_name + " values(?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    srand(time(NULL));
    int rand_init_val = rand() % 10000000;
    std::vector<long> long_count_array;
    std::vector<std::string> host_name_array;
    std::vector<std::string> colo_name_array;

    size_t rows = 3;

    // need to make sure that host name ordered in ascending, so that the array sequence via push_back is correct

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        int long_val = rand_init_val++;
        val1->set_long_value(long_val);
        long_count_array.push_back(long_val);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        std::string host_name_val("nudata-nudata-abc-1");
        val2->set_string_value(host_name_val);
        host_name_array.push_back(host_name_val);

        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        std::string colo_name_val("nudata-nudata-xyz-1");
        val3->set_string_value(colo_name_val);
        colo_name_array.push_back(colo_name_val);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }
    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        int long_val = rand_init_val++;
        val1->set_long_value(long_val);
        long_count_array.push_back(long_val);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        std::string host_name_val("nudata-nudata-abc-2");
        val2->set_string_value(host_name_val);
        host_name_array.push_back(host_name_val);

        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        std::string colo_name_val("nudata-nudata-xyz-2");
        val3->set_string_value(colo_name_val);
        colo_name_array.push_back(colo_name_val);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        int long_val = rand_init_val++;
        val1->set_long_value(long_val);
        long_count_array.push_back(long_val);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        std::string host_name_val("nudata-nudata-abc-3");
        val2->set_string_value(host_name_val);
        host_name_array.push_back(host_name_val);

        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        std::string colo_name_val("nudata-nudata-xyz-3");
        val3->set_string_value(colo_name_val);
        colo_name_array.push_back(colo_name_val);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Count", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));

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
        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                           context);
    batchReader.read();

    size_t total_number_of_rows_holder = block_holder.rows();
    LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
    std::string names_holder = block_holder.dumpNames();
    LOG(INFO) << "column names dumped in block holder : " << names;

    std::string structure = block_holder.dumpStructure();
    LOG(INFO) << "structure dumped in block holder: " << structure;
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Count , \n" /*  0. UInt64*/
                                 "     Host,\n"      /* 1.  String */
                                 "     Colo \n"      /* 2. String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (Host, Count)";

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
        ASSERT_EQ(number_of_columns, 3U);

        auto& column_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]); // Count
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]); // Host
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]); // Colo

        // Count retrieval
        for (size_t i = 0; i < rows; i++) {
            uint64_t val = column_0.getData()[i];
            LOG(INFO) << "retrieved Count: " << val << " with expected value: " << long_count_array[i];
            ASSERT_EQ(val, (uint64_t)long_count_array[i]);

            // Host value retrieval.
            auto column_1_string = column_1.getDataAt(i);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            LOG(INFO) << "retrieved host name is: " << column_1_real_string
                      << " with expected value: " << host_name_array[i];
            ASSERT_EQ(column_1_real_string, host_name_array[i]);

            // Colo value retrieval
            auto column_2_real_string = column_2.getDataAt(i);
            std::string column_2_real_string_value(column_2_real_string.data, column_2_real_string.size);
            LOG(INFO) << "retrieved colo name: " << column_2_real_string_value
                      << " with expected value: " << colo_name_array[i];
            ASSERT_EQ(column_2_real_string_value, colo_name_array[i]);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * Use xdr_test to test out missing columns
 */

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
