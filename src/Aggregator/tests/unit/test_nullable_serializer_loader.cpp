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

#include <Common/Exception.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

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

class AggregatorLoaderNullableSerializerRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderNullableSerializerRelatedTest::shared_context = nullptr;

/**
 * For primitive types: family name and name are identical
 *
 *  family name identified for FixedString(8) is: FixedString
 *  name identified for FixedString(8) is: FixedString(8)
 *
 *  family name identified for Nullable (String) is: Nullable
 *   name identified for Nullable (String)) is: Nullable(String)
 */

TEST_F(AggregatorLoaderNullableSerializerRelatedTest, InsertARowWithNullableString) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_nullable_event_2";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into simple_nullable_event_2 (`Host`, `Colo`, `EventName`, `Count`, `Duration`, "
                        "`Description`) VALUES ('graphdb-1', 'LVS', 'RESTART', 1,  1000,  NULL);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_nullable_event_2 values(?, ?, ?, ?, ?, ?)";
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
    std::string colo_name = "colo-lvs";
    std::string event_name = "RESTART";
    long counter = 0;
    std::optional<double> duration;
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
        long rand_long_val = rand() % 1000000;
        LOG(INFO) << "generated random  long number: " << rand_long_val;
        counter = rand_long_val;
        val4->set_long_value(rand_long_val);

        // value 5
        nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
        int rand_int_val = rand() % 1000000;
        LOG(INFO) << "generated random int number:  " << rand_int_val;
        duration = rand_int_val;
        val5->set_double_value(rand_int_val);

        // value 6, NULL.
        nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
        nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;
        description = std::nullopt;
        val6->set_null_value(nullValueP);

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
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "FixedString(8)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("EventName", "String"));
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
    std::string query_on_table = "select Host , \n"     /*  0. String */
                                 "     Colo,\n"         /* 1. FixedString(8) */
                                 "     EventName,\n"    /* 2. String */
                                 "     Count,\n"        /*  3. UInt64 */
                                 "     Duration,\n"     /*  4. Nullable(Float32) */
                                 "      Description \n" /* 5. Nullable(String) */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (Host, Colo, EventName)";

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

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);      // Host
        auto& column_1 = assert_cast<DB::ColumnFixedString&>(*columns[1]); // Colo
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);      // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);      // Count
        auto& column_4 = assert_cast<DB::ColumnNullable&>(*columns[4]);    // Nullable(Float32)
        auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);    // Nullable(String)

        // Host value retrieval.
        auto column_0_string = column_0.getDataAt(0); // We only have 1 row.
        std::string column_0_real_string(column_0_string.data, column_0_string.size);
        LOG(INFO) << "retrieved host name is: " << column_0_real_string;
        ASSERT_EQ(column_0_real_string, host_name);

        // Colo value retrieval
        auto column_1_real_string = column_1.getDataAt(0);
        std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
        LOG(INFO) << "retrieved fixed string colo name: " << column_1_real_string_value;
        ASSERT_EQ(column_1_real_string_value, colo_name);

        // EventName retrieval
        auto column_2_string = column_2.getDataAt(0); // We only have 1 row.
        std::string column_2_real_string(column_2_string.data, column_2_string.size);
        LOG(INFO) << "retrieved event name: " << column_2_real_string;
        ASSERT_EQ(column_2_real_string, event_name);

        // Count retrieval
        uint64_t val = column_3.getData()[0];
        LOG(INFO) << "retrieved Count: " << val << " with expected value: " << counter;
        ASSERT_EQ(val, (uint64_t)counter);

        // Duration retrieval
        auto column_4_nullable_nested_column = column_4.getNestedColumnPtr().get();
        const DB::ColumnFloat32& resulted_column_4_nullable_nested_column =
            assert_cast<const DB::ColumnFloat32&>(*column_4_nullable_nested_column);

        ASSERT_FALSE(column_4.isNullAt(0)); // we set it to be a non-null value.
        if (column_4.isNullAt(0)) {
            LOG(INFO) << "retrieved duration is a NULL value";
        } else {
            auto column_4_real_value = resulted_column_4_nullable_nested_column.getData()[0];
            LOG(INFO) << "retrieved duration: " << column_4_real_value;
            ASSERT_EQ(column_4_real_value, *duration);
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
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

TEST_F(AggregatorLoaderNullableSerializerRelatedTest, InsertARowWithNullableFloatValueAndNullString) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_nullable_event_2";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_event_5;
    std::string query = "insert into simple_nullable_event_2 (`Host`, `Colo`, `EventName`, `Count`, `Duration`, "
                        "`Description`) VALUES ('graphdb-1', 'LVS', 'RESTART', 1,  1000,  NULL);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_nullable_event_2 values(?, ?, ?, ?, ?, ?)";
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
    std::string colo_name = "colo-lvs";
    std::string event_name = "RESTART";
    long counter = 0;
    std::optional<double> duration;
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
        long rand_long_val = rand() % 1000000;
        LOG(INFO) << "generated random  long number: " << rand_long_val;
        counter = rand_long_val;
        val4->set_long_value(rand_long_val);

        nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;
        // value 5, NULL
        nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
        val5->set_null_value(nullValueP);
        duration = std::nullopt;

        // value 6, NULL.
        nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
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
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "FixedString(8)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("EventName", "String"));
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
    std::string query_on_table = "select Host , \n"     /*  0. String */
                                 "     Colo,\n"         /* 1. FixedString(8) */
                                 "     EventName,\n"    /* 2. String */
                                 "     Count,\n"        /*  3. UInt64 */
                                 "     Duration,\n"     /*  4. Nullable(Float32) */
                                 "      Description \n" /* 5. Nullable(String) */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (Host, Colo, EventName)";

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

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);      // Host
        auto& column_1 = assert_cast<DB::ColumnFixedString&>(*columns[1]); // Colo
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);      // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);      // Count
        auto& column_4 = assert_cast<DB::ColumnNullable&>(*columns[4]);    // Nullable(Float32)
        auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);    // Nullable(String)

        // Host value retrieval.
        auto column_0_string = column_0.getDataAt(0); // We only have 1 row.
        std::string column_0_real_string(column_0_string.data, column_0_string.size);
        LOG(INFO) << "retrieved host name is: " << column_0_real_string;
        ASSERT_EQ(column_0_real_string, host_name);

        // Colo value retrieval
        auto column_1_real_string = column_1.getDataAt(0);
        std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
        LOG(INFO) << "retrieved fixed string colo name: " << column_1_real_string_value;
        ASSERT_EQ(column_1_real_string_value, colo_name);

        // EventName retrieval
        auto column_2_string = column_2.getDataAt(0); // We only have 1 row.
        std::string column_2_real_string(column_2_string.data, column_2_string.size);
        LOG(INFO) << "retrieved event name: " << column_2_real_string;
        ASSERT_EQ(column_2_real_string, event_name);

        // Count retrieval
        uint64_t val = column_3.getData()[0];
        LOG(INFO) << "retrieved Count: " << val << " with expected value: " << counter;
        ASSERT_EQ(val, (uint64_t)counter);

        // Duration retrieval
        auto column_4_nullable_nested_column = column_4.getNestedColumnPtr().get();
        const DB::ColumnFloat32& resulted_column_4_nullable_nested_column =
            assert_cast<const DB::ColumnFloat32&>(*column_4_nullable_nested_column);

        ASSERT_TRUE(column_4.isNullAt(0)); // we set it to be a non-null value.
        if (column_4.isNullAt(0)) {
            LOG(INFO) << "retrieved duration is a NULL value";
        } else {
            auto column_4_real_value = resulted_column_4_nullable_nested_column.getData()[0];
            LOG(INFO) << "retrieved duration: " << column_4_real_value;
            ASSERT_EQ(column_4_real_value, *duration);
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
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

TEST_F(AggregatorLoaderNullableSerializerRelatedTest, InsertMultipleRowsWithNullableFloatValueAndNullString) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_nullable_event_2";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2
    std::string query = "insert into simple_nullable_event_2 (`Host`, `Colo`, `EventName`, `Count`, `Duration`, "
                        "`Description`) VALUES ('graphdb-1', 'LVS', 'RESTART', 1,  1000,  NULL);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_nullable_event_2 values(?, ?, ?, ?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    std::string serializedSqlBatchRequestInString;

    size_t rows = 5;
    srand(time(NULL));

    std::string host_name = "graphdb-50001";
    std::string colo_name = "colo-lvs";
    std::string event_name = "RESTART";
    std::vector<long> counter_array;
    std::optional<double> duration;
    std::optional<std::string> description;

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
            long rand_long_val = rand() % 1000000;
            LOG(INFO) << "generated random  long number: " << rand_long_val;
            counter_array.push_back(rand_long_val);
            val4->set_long_value(rand_long_val);

            nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;
            // value 5, NULL
            nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
            val5->set_null_value(nullValueP);
            duration = std::nullopt;

            // value 6, NULL.
            nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
            val6->set_null_value(nullValueP);
            description = std::nullopt;

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
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "FixedString(8)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("EventName", "String"));
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
    std::string query_on_table = "select Host , \n"     /*  0. String */
                                 "     Colo,\n"         /* 1. FixedString(8) */
                                 "     EventName,\n"    /* 2. String */
                                 "     Count,\n"        /*  3. UInt64 */
                                 "     Duration,\n"     /*  4. Nullable(Float32) */
                                 "      Description \n" /* 5. Nullable(String) */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (Host, Colo, EventName)";

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

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);      // Host
        auto& column_1 = assert_cast<DB::ColumnFixedString&>(*columns[1]); // Colo
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);      // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);      // Count
        auto& column_4 = assert_cast<DB::ColumnNullable&>(*columns[4]);    // Nullable(Float32)
        auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);    // Nullable(String)

        for (size_t i = 0; i < rows; i++) {
            // Host value retrieval.
            auto column_0_string = column_0.getDataAt(i); // we have 5 rows now.
            std::string column_0_real_string(column_0_string.data, column_0_string.size);
            LOG(INFO) << "retrieved host name is: " << column_0_real_string;
            ASSERT_EQ(column_0_real_string, host_name);

            // Colo value retrieval
            auto column_1_real_string = column_1.getDataAt(i);
            std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
            LOG(INFO) << "retrieved fixed string colo name: " << column_1_real_string_value;
            ASSERT_EQ(column_1_real_string_value, colo_name);

            // EventName retrieval
            auto column_2_string = column_2.getDataAt(i);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);
            LOG(INFO) << "retrieved event name: " << column_2_real_string;
            ASSERT_EQ(column_2_real_string, event_name);

            // Count retrieval
            uint64_t val = column_3.getData()[i];
            LOG(INFO) << "retrieved Count: " << val << " with expected value: " << counter_array[i];
            ASSERT_EQ(val, (uint64_t)counter_array[i]);

            // Duration retrieval
            auto column_4_nullable_nested_column = column_4.getNestedColumnPtr().get();
            const DB::ColumnFloat32& resulted_column_4_nullable_nested_column =
                assert_cast<const DB::ColumnFloat32&>(*column_4_nullable_nested_column);

            ASSERT_TRUE(column_4.isNullAt(i)); // we set it to be a non-null value.
            if (column_4.isNullAt(i)) {
                LOG(INFO) << "retrieved duration is a NULL value ";
            } else {
                auto column_4_real_value = resulted_column_4_nullable_nested_column.getData()[i];
                LOG(INFO) << "retrieved duration: " << column_4_real_value;
                ASSERT_EQ(column_4_real_value, *duration);
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
                LOG(INFO) << "retrieved description is: " << column_5_real_string_value;
            }
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

TEST_F(AggregatorLoaderNullableSerializerRelatedTest, InsertSingleRowWithNullStringAndOrderMatched) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "ontime_with_nullable ";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2
    std::string query = "insert into ontime_with_nullable (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, "
                        "`dayOfWeek`, `flightDate`, `captain`, `code`, `status`)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = query; // so this one has the columns specified, and in this test, the order follows what is
                             // specified in the table definition.
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    std::string serializedSqlBatchRequestInString;

    size_t rows = 1;
    std::vector<uint16_t> flight_year_array;
    std::vector<uint8_t> quarter_array;
    std::vector<uint8_t> flight_month_array;
    std::vector<uint8_t> day_of_month_array;
    std::vector<uint8_t> day_of_week_array;
    std::vector<uint16_t> days_since_epoch_array;
    std::string captain_value = "Captain Phillips";
    std::string code_value = "LVSD"; // need to have 4 characters for the testing, as the type is FixedString(4);
    std::string status_value = "normal_set_value";

    int flight_year_start_value = std::rand() % 32000;
    srand(time(NULL));
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

        LOG(INFO) << " flight-year-start value is: " << flight_year_start_value;

        for (size_t r = 0; r < rows; r++) {
            nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
            // value 1: flightYear
            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
            // val1->set_int_value(1000 + (std::rand() % 100));
            uint16_t flight_year_value = flight_year_start_value + r;
            val1->set_int_value(flight_year_value);
            flight_year_array.push_back(flight_year_value);

            // value 2: quarter
            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
            uint8_t quarter_value = 1 + (std::rand() % 4);
            val2->set_int_value(quarter_value);
            quarter_array.push_back(quarter_value);
            // NOTE: converted to int32_t is necessary in order to print out the value on the log.
            LOG(INFO) << "populate quarter value: " << (int32_t)quarter_value;

            // value 3: flightMonth
            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
            uint8_t flight_month_value = 1 + (std::rand() % 12);
            val3->set_int_value(flight_month_value);
            flight_month_array.push_back(flight_month_value);

            // value 4: dayOfMonth
            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
            uint8_t day_of_month_value = 1 + (std::rand() % 30);
            val4->set_int_value(day_of_month_value);
            day_of_month_array.push_back(day_of_month_value);

            // value 5: dayOfWeek
            nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
            uint8_t day_of_week_value = 1 + (std::rand() % 7);
            val5->set_int_value(day_of_week_value);
            day_of_week_array.push_back(day_of_week_value);

            // value 6: flightDate
            nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

            ts->set_milliseconds(ms.count());
            val6->set_allocated_timestamp(ts);
            days_since_epoch_array.push_back(days_since_epoch);

            // value 7: captain, string
            nucolumnar::datatypes::v1::ValueP* val7 = bindingList.add_values();
            val7->set_string_value(captain_value);

            // value 8: code, string
            nucolumnar::datatypes::v1::ValueP* val8 = bindingList.add_values();
            val8->set_string_value(code_value);

            // value 9: status, string
            nucolumnar::datatypes::v1::ValueP* val9 = bindingList.add_values();
            val9->set_string_value(status_value);

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
        }

        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    }

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("flightYear", "UInt16"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("quarter", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("flightMonth", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("dayOfMonth", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("dayOfWeek", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("flightDate", "Date"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("captain", "Nullable(String)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("code", "FixedString (4)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("status", "String"));

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
    std::string query_on_table = "select flightYear , \n" /*  0. UInt16 */
                                 "     quarter,\n"        /* 1. UInt8 */
                                 "     flightDate,\n"     /* 2. Date */
                                 "     captain,\n"        /*  3. Nullable(String) */
                                 "     code,\n"           /*  4.  FixedString(4) */
                                 "     status \n"         /* 5. String, with default value of "normal" */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (flightYear, flightDate)";

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

        auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
        auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
        auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
        auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
        auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
        auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;

        for (size_t i = 0; i < rows; i++) {
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
                      << " with expected value: " << days_since_epoch_array[i];
            int time_diff = (int)flight_date_val - (int)days_since_epoch_array[i];
            ASSERT_TRUE(std::abs(time_diff) < 2);

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
            LOG(INFO) << "retrieved fixed string code name: " << column_4_real_string_value;
            ASSERT_EQ(column_4_real_string_value, code_value);

            // status retrieval
            auto column_5_string = column_5.getDataAt(i);
            std::string column_5_real_string(column_5_string.data, column_5_string.size);
            LOG(INFO) << "retrieved status name: " << column_5_real_string;
            ASSERT_EQ(column_5_real_string, status_value);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

TEST_F(AggregatorLoaderNullableSerializerRelatedTest, InsertSingleRowWithNullStringAndOrderNotMatched) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderNullableSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "ontime_with_nullable ";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2, with query's column ordering different from what is specified in the table
    // definition.
    std::string query = "insert into ontime_with_nullable (`flightYear`, `quarter`, `flightDate`, `flightMonth`, "
                        "`dayOfMonth`, `dayOfWeek`, `captain`, `code`, `status`) values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
    // std::string query = "insert into ontime_with_nullable (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`,
    // `dayOfWeek`, `flightDate`, `captain`, `code`, `status`)  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = query; // so this one has the columns specified, and in this test, the order follows what is
                             // specified in the table definition.
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    std::string serializedSqlBatchRequestInString;

    size_t rows = 1;
    std::vector<uint16_t> flight_year_array;
    std::vector<uint8_t> quarter_array;
    std::vector<uint8_t> flight_month_array;
    std::vector<uint8_t> day_of_month_array;
    std::vector<uint8_t> day_of_week_array;
    std::vector<uint16_t> days_since_epoch_array;
    std::string captain_value = "Captain Phillips";
    std::string code_value = "LVSD"; // need to have 4 characters for the testing, as the type is FixedString(4);
    std::string status_value = "normal_set_value";

    int flight_year_start_value = std::rand() % 32000;
    srand(time(NULL));
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

        LOG(INFO) << " flight-year-start value is: " << flight_year_start_value;

        for (size_t r = 0; r < rows; r++) {
            nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
            // value 1: flightYear
            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
            uint16_t flight_year_value = flight_year_start_value + r;
            val1->set_int_value(flight_year_value);
            flight_year_array.push_back(flight_year_value);

            // value 2: quarter
            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
            uint8_t quarter_value = 1 + (std::rand() % 4);
            val2->set_int_value(quarter_value);
            quarter_array.push_back(quarter_value);
            // NOTE: converted to int32_t is necessary in order to print out the value on the log.
            LOG(INFO) << "populate quarter value: " << (int32_t)quarter_value;

            // value 3: fligthtDate
            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
            auto ts = new nucolumnar::datatypes::v1::TimestampP();
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch());
            auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

            ts->set_milliseconds(ms.count());
            val3->set_allocated_timestamp(ts);
            days_since_epoch_array.push_back(days_since_epoch);

            // value 4: flightMonth
            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
            uint8_t flight_month_value = 1 + (std::rand() % 12);
            val4->set_int_value(flight_month_value);
            flight_month_array.push_back(flight_month_value);

            // value 5: dayOfMonth
            nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
            uint8_t day_of_month_value = 1 + (std::rand() % 30);
            val5->set_int_value(day_of_month_value);
            day_of_month_array.push_back(day_of_month_value);

            // value 6: dayOfWeek
            nucolumnar::datatypes::v1::ValueP* val6 = bindingList.add_values();
            uint8_t day_of_week_value = 1 + (std::rand() % 7);
            val6->set_int_value(day_of_week_value);
            day_of_week_array.push_back(day_of_week_value);

            // value 7: captain, string
            nucolumnar::datatypes::v1::ValueP* val7 = bindingList.add_values();
            val7->set_string_value(captain_value);

            // value 8: code, string
            nucolumnar::datatypes::v1::ValueP* val8 = bindingList.add_values();
            val8->set_string_value(code_value);

            // value 9: status, string
            nucolumnar::datatypes::v1::ValueP* val9 = bindingList.add_values();
            val9->set_string_value(status_value);

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
        }

        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    }

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("flightYear", "UInt16"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("quarter", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("flightMonth", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("dayOfMonth", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("dayOfWeek", "UInt8"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("flightDate", "Date"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("captain", "Nullable(String)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("code", "FixedString (4)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("status", "String"));

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
    std::string query_on_table = "select flightYear , \n" /*  0. UInt64 */
                                 "     quarter,\n"        /* 1. UInt8 */
                                 "     flightDate,\n"     /* 2. Date */
                                 "     captain,\n"        /*  3. Nullable(String) */
                                 "     code,\n"           /*  4.  FixedString(4) */
                                 "     status \n"         /* 5. String, with default value of "normal" */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY (flightYear, flightDate)";

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

        auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]);      // flightYear
        auto& column_1 = assert_cast<DB::ColumnUInt8&>(*columns[1]);       // quarter
        auto& column_2 = assert_cast<DB::ColumnUInt16&>(*columns[2]);      // flightDate
        auto& column_3 = assert_cast<DB::ColumnNullable&>(*columns[3]);    // captain
        auto& column_4 = assert_cast<DB::ColumnFixedString&>(*columns[4]); // code
        auto& column_5 = assert_cast<DB::ColumnString&>(*columns[5]);      // status;

        for (size_t i = 0; i < rows; i++) {
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
                      << " with expected value: " << days_since_epoch_array[i];
            int time_diff = (int)flight_date_val - (int)days_since_epoch_array[i];
            ASSERT_TRUE(std::abs(time_diff) < 2);

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
            LOG(INFO) << "retrieved fixed string code name: " << column_4_real_string_value;
            ASSERT_EQ(column_4_real_string_value, code_value);

            // status retrieval
            auto column_5_string = column_5.getDataAt(i);
            std::string column_5_real_string(column_5_string.data, column_5_string.size);
            LOG(INFO) << "retrieved status name: " << column_5_real_string;
            ASSERT_EQ(column_5_real_string, status_value);
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
