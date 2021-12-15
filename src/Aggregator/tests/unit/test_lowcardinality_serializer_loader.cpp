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
#include <Columns/ColumnLowCardinality.h>
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

class AggregatorLoaderLowCardinalitySerializerRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderLowCardinalitySerializerRelatedTest::shared_context = nullptr;

/**
 * For primitive types: family name and name are identical
 *
 *  family name identified for FixedString(8) is: FixedString
 *  name identified for FixedString(8) is: FixedString(8)
 *
 *  family name identified for Nullable (String) is: Nullable
 *   name identified for Nullable (String)) is: Nullable(String)
 */

TEST_F(AggregatorLoaderLowCardinalitySerializerRelatedTest, InsertARowWithLowCardinalityString) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderLowCardinalitySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderLowCardinalitySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_lowcardinality_event_5";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into " + table_name +
        " (`Host`, `Colo`, `EventName`, `Count`, `Duration`, `Description`) VALUES ('graphdb-1', 'LVS', 'RESTART', 1,  "
        "1000,  NULL);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into " + table_name + " values(?, ?, ?, ?, ?, ?)";
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
    std::string host_name = "graphdb-50001";
    std::string colo_name = "lvs";
    std::string event_name = "RESTART";
    long rand_long_val = 0;
    std::optional<float> duration_val = 0;
    std::optional<std::string> description;
    {
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_string_value(host_name);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value(colo_name);

        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value(event_name);

        // value 4
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        rand_long_val = rand() % 1000000;
        LOG(INFO) << "generated random  long number: " << rand_long_val;
        val4->set_long_value(rand_long_val);

        // value 5
        nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
        int rand_int_val = rand() % 1000000;
        LOG(INFO) << "generated random int number:  " << rand_int_val;
        val5->set_double_value(rand_int_val);
        duration_val = rand_int_val;

        // value 6, NULL.
        nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
        nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;
        val6->set_null_value(nullValueP);
        description = std::nullopt;

        // copying
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

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "LowCardinality(FixedString(8))"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("EventName", "LowCardinality(FixedString(8))"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Count", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Duration", "Nullable(Float32)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Description", "Nullable(String)"));

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

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Host , \n"    /*  0. String */
                                 "     Colo,\n"        /* 1. LowCardinality(FixedString(8))  */
                                 "     EventName,\n"   /* 2. LowCardinality(FixedString(8))*/
                                 "     Count, \n"      /*  3. UInt64 */
                                 "     Duration, \n"   /*  4. Nullable(Float32) */
                                 "     Description \n" /*  5. Nullable(String) */
                                 "from " +
        table_being_queried +
        "\n"
        "order by (Host, Colo, EventName)";
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
        ASSERT_EQ(number_of_columns, 6U);

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);         // Host
        auto& column_1 = assert_cast<DB::ColumnLowCardinality&>(*columns[1]); // Colo
        auto& column_2 = assert_cast<DB::ColumnLowCardinality&>(*columns[2]); // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);         // Count
        auto& column_4 = assert_cast<DB::ColumnNullable&>(*columns[4]);       // Duration
        auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);       // Duration

        {
            // Host value retrieval
            auto column_0_string = column_0.getDataAt(0);
            std::string column_0_real_string(column_0_string.data, column_0_string.size);
            LOG(INFO) << "retrieved host name is: " << column_0_real_string;
            ASSERT_EQ(column_0_real_string, host_name);

            // Colo value retrieval
            auto column_1_real_string = column_1.getDataAt(0);
            std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
            LOG(INFO) << "retrieved low cardinality (fixed string 8) colo name: " << column_1_real_string_value
                      << " with original value: " << colo_name;
            size_t found = column_1_real_string_value.find(colo_name);
            ASSERT_TRUE(found != std::string::npos);

            // Event name retrieval
            auto column_2_string = column_2.getDataAt(0);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);
            LOG(INFO) << "retrieved low cardinality (fixed string 8) event name: " << column_2_real_string
                      << " with original value: " << event_name;
            found = column_2_real_string.find(event_name);
            ASSERT_TRUE(found != std::string::npos);

            // Count value retrieval
            uint64_t val = column_3.getData()[0];
            LOG(INFO) << "retrieved Count: " << val << " with expected value: " << rand_long_val;
            ASSERT_EQ(val, (uint64_t)rand_long_val);

            // Duration value retrieval
            auto column_4_nullable_nested_column = column_4.getNestedColumnPtr().get();
            const DB::ColumnFloat32& resulted_column_4_nullable_nested_column =
                assert_cast<const DB::ColumnFloat32&>(*column_4_nullable_nested_column);

            ASSERT_FALSE(column_4.isNullAt(0)); // we set it to be a non-null value.
            if (column_4.isNullAt(0)) {
                LOG(INFO) << "retrieved duration is a NULL value";
            } else {
                auto column_4_real_value = resulted_column_4_nullable_nested_column.getData()[0];
                LOG(INFO) << "retrieved duration: " << column_4_real_value;
                ASSERT_EQ(column_4_real_value, *duration_val);
            }

            // Description retrieval
            auto column_5_nullable_nested_column = column_5.getNestedColumnPtr().get();
            const DB::ColumnString& resulted_column_5_nullable_nested_column =
                assert_cast<const DB::ColumnString&>(*column_5_nullable_nested_column);

            ASSERT_TRUE(column_5.isNullAt(0));
            if (column_5.isNullAt(0)) {
                LOG(INFO) << "retrieved description is a NULL value";
            } else {
                auto column_5_real_value = resulted_column_5_nullable_nested_column.getDataAt(0);
                std::string column_5_real_string_value(column_5_real_value.data, column_5_real_value.size);
                LOG(INFO) << "retrieved description is: " << column_5_real_string_value;
            }
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

TEST_F(AggregatorLoaderLowCardinalitySerializerRelatedTest, InsertMultipleRowsWithLowCardinalityStringValues) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderLowCardinalitySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderLowCardinalitySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_lowcardinality_event_5";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2
    std::string query = "insert into " + table_name +
        " (`Host`, `Colo`, `EventName`, `Count`, `Duration`, `Description`) VALUES ('graphdb-1', 'LVS', 'RESTART', 1,  "
        "1000,  NULL);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into " + table_name + " values(?, ?, ?, ?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    std::string serializedSqlBatchRequestInString;

    size_t rows = 5;
    srand(time(NULL));
    std::string host_name = "graphdb-50001";
    std::string colo_name = "lvs";
    std::string event_name = "RESTART";

    long initial_long_value = rand() % 1000000;
    std::vector<long> long_value_array;

    int initial_duration_float_value = rand() % 1000000;
    std::vector<std::optional<float>> duration_val_array;
    std::vector<std::optional<std::string>> description_array;

    {
        LOG(INFO) << "to construct and serialized a message";

        nucolumnar::aggregator::v1::DataBindingList bindingList;
        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
        sqlBatchRequest.set_shard(shard);
        sqlBatchRequest.set_table(table);

        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
            sqlBatchRequest.mutable_nucolumnarencoding();
        sqlWithBatchBindings->set_sql(sql);
        sqlWithBatchBindings->mutable_batch_bindings();

        for (size_t r = 0; r < rows; r++) {
            nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();

            // val1
            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
            val1->set_string_value(host_name);

            // val2
            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
            val2->set_string_value(colo_name);

            // value 3
            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
            val3->set_string_value(event_name);

            // value 4
            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
            long rand_long_val = initial_long_value + r;
            LOG(INFO) << "generated random  long number: " << rand_long_val;
            val4->set_long_value(rand_long_val);
            long_value_array.push_back(rand_long_val);

            // value 5
            nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
            int rand_int_val = initial_duration_float_value + r;
            LOG(INFO) << "generated random int number:  " << rand_int_val;
            val5->set_double_value(rand_int_val);
            duration_val_array.push_back(rand_int_val);

            // value 6, NULL.
            nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;
            nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
            val6->set_null_value(nullValueP);
            description_array.push_back(std::nullopt);

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

        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    }

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "LowCardinality(FixedString(8))"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("EventName", "LowCardinality(FixedString(8))"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Count", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Duration", "Nullable(Float32)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Description", "Nullable(String)"));

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

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Host , \n"    /*  0. String */
                                 "     Colo,\n"        /* 1. LowCardinality(FixedString(8))  */
                                 "     EventName,\n"   /* 2. LowCardinality(FixedString(8))*/
                                 "     Count, \n"      /*  3. UInt64 */
                                 "     Duration, \n"   /*  4. Nullable(Float32) */
                                 "     Description \n" /*  5. Nullable(String) */
                                 "from " +
        table_being_queried +
        "\n"
        "order by (Host, Colo, EventName)";

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
        ASSERT_EQ(number_of_columns, 6U);

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);         // Host
        auto& column_1 = assert_cast<DB::ColumnLowCardinality&>(*columns[1]); // Colo
        auto& column_2 = assert_cast<DB::ColumnLowCardinality&>(*columns[2]); // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);         // Count
        auto& column_4 = assert_cast<DB::ColumnNullable&>(*columns[4]);       // Duration
        auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);       // Duration

        for (size_t i = 0; i < rows; i++) {
            // Host value retrieval
            auto column_0_string = column_0.getDataAt(i);
            std::string column_0_real_string(column_0_string.data, column_0_string.size);
            LOG(INFO) << "retrieved host name is: " << column_0_real_string;
            ASSERT_EQ(column_0_real_string, host_name);

            // Colo value retrieval
            auto column_1_real_string = column_1.getDataAt(i);
            std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
            LOG(INFO) << "retrieved low cardinality (fixed string 8) colo name: " << column_1_real_string_value
                      << " with original value: " << colo_name;
            size_t found = column_1_real_string_value.find(colo_name);
            ASSERT_TRUE(found != std::string::npos);

            // Event name retrieval
            auto column_2_string = column_2.getDataAt(i);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);
            LOG(INFO) << "retrieved low cardinality (fixed string 8) event name: " << column_2_real_string
                      << " with original value: " << event_name;
            found = column_2_real_string.find(event_name);
            ASSERT_TRUE(found != std::string::npos);

            // Count value retrieval
            uint64_t val = column_3.getData()[i];
            LOG(INFO) << "retrieved Count: " << val << " with expected value: " << long_value_array[i];
            ASSERT_EQ(val, (uint64_t)long_value_array[i]);

            // Duration value retrieval
            auto column_4_nullable_nested_column = column_4.getNestedColumnPtr().get();
            const DB::ColumnFloat32& resulted_column_4_nullable_nested_column =
                assert_cast<const DB::ColumnFloat32&>(*column_4_nullable_nested_column);

            ASSERT_FALSE(column_4.isNullAt(i)); // we set it to be a non-null value.
            if (column_4.isNullAt(i)) {
                LOG(INFO) << "retrieved duration is a NULL value";
            } else {
                auto column_4_real_value = resulted_column_4_nullable_nested_column.getData()[i];
                LOG(INFO) << "retrieved duration: " << column_4_real_value
                          << " with expected value: " << *duration_val_array[i];
                ASSERT_EQ(column_4_real_value, *duration_val_array[i]);
            }

            // Description retrieval
            auto column_5_nullable_nested_column = column_5.getNestedColumnPtr().get();
            const DB::ColumnString& resulted_column_5_nullable_nested_column =
                assert_cast<const DB::ColumnString&>(*column_5_nullable_nested_column);

            ASSERT_TRUE(column_5.isNullAt(i));
            if (column_5.isNullAt(i)) {
                LOG(INFO) << "retrieved description is a NULL value";
            } else {
                auto column_5_real_value = resulted_column_5_nullable_nested_column.getDataAt(i);
                std::string column_5_real_string_value(column_5_real_value.data, column_5_real_value.size);
                LOG(INFO) << "retrieved description is: " << column_5_real_string_value
                          << " with expected value: " << *description_array[i];
            }
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
