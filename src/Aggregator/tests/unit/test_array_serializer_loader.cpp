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
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnNullable.h>

#include <Functions/FunctionHelpers.h>

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>
#include <Aggregator/DistributedLoaderLock.h>

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

namespace nuclm {
namespace ErrorCodes {
extern const int COLUMN_DEFINITION_NOT_CORRECT;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
} // namespace ErrorCodes
} // namespace nuclm

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

static DB::DataTypePtr resolveArrayType(const DB::DataTypePtr& argument) {
    if (!DB::isArray(argument))
        throw DB::Exception("Illegal type " + argument->getName() + ", expected Array",
                            nuclm::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    DB::DataTypePtr nested_type = argument;
    while (isArray(nested_type))
        nested_type = DB::checkAndGetDataType<DB::DataTypeArray>(nested_type.get())->getNestedType();
    return std::make_shared<DB::DataTypeArray>(nested_type);
}

static DB::ColumnPtr arrayFlattenFunction(const DB::IColumn& column, size_t input_rows_count) {
    const DB::ColumnArray* src_col = DB::checkAndGetColumn<DB::ColumnArray>(&column);

    if (!src_col) {
        throw DB::Exception("Illegal column " + column.getName() + " in argument of function 'arrayFlatten'",
                            nuclm::ErrorCodes::COLUMN_DEFINITION_NOT_CORRECT);
    }

    const DB::IColumn::Offsets& src_offsets = src_col->getOffsets();

    DB::ColumnArray::ColumnOffsets::MutablePtr result_offsets_column;
    const DB::IColumn::Offsets* prev_offsets = &src_offsets;
    const DB::IColumn* prev_data = &src_col->getData();

    while (const DB::ColumnArray* next_col = DB::checkAndGetColumn<DB::ColumnArray>(prev_data)) {
        if (!result_offsets_column)
            result_offsets_column = DB::ColumnArray::ColumnOffsets::create(input_rows_count);

        DB::IColumn::Offsets& result_offsets = result_offsets_column->getData();

        const DB::IColumn::Offsets* next_offsets = &next_col->getOffsets();

        for (size_t i = 0; i < input_rows_count; ++i)
            result_offsets[i] =
                (*next_offsets)[(*prev_offsets)[i] - 1]; /// -1 array subscript is Ok, see PaddedPODArray

        prev_offsets = &result_offsets;
        prev_data = &next_col->getData();
    }

    return DB::ColumnArray::create(prev_data->getPtr(),
                                   result_offsets_column ? std::move(result_offsets_column) : src_col->getOffsetsPtr());
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

class AggregatorLoaderArraySerializerRelatedTest : public ::testing::Test {
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
    virtual void SetUp() {}

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    static ContextWrapper* shared_context;
};

ContextWrapper* AggregatorLoaderArraySerializerRelatedTest::shared_context = nullptr;

/**
 * SELECT * FROM simple_event_62

    ┌─Counters───────────────┬─Host─────┬─Colo─────┐
    │ [252837,252838,252839] │ abc12345 │ xyz12345 │
    └────────────────────────┴──────────┴──────────┘
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest, InsertOneRowWithSimpleArray) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    // for table: simple_nullable_event_2;
    std::string table_name = "simple_event_62";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    std::string query = "insert into simple_event_62 values(?, ?, ?);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_event_62 values(?, ?, ?);";
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

    std::vector<long> array_counter;
    std::string string_value_1 = "abc12345";
    std::string string_value_2 = "xyz12345";

    srand(time(NULL));
    {
        // value 1, Counters
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        long start_uint_value = rand() % 1000000;

        array_counter.push_back(start_uint_value);
        array_value->add_value()->set_uint_value(start_uint_value++);

        array_counter.push_back(start_uint_value);
        array_value->add_value()->set_uint_value(start_uint_value++);

        array_counter.push_back(start_uint_value);
        array_value->add_value()->set_uint_value(start_uint_value++);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(UInt32)"));
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
    std::string query_on_table = "select Counters, \n" /* 0. Array (UInt32) */
                                 "     Host,\n"        /* 1. type: String */
                                 "     Colo\n"         /* 2. type: String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

    auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
        const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
        int column_index = 0;

        for (auto& p : columns_with_type_and_name) {
            LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                      << " column type: " << p.type->getName() << " column name: " << p.name
                      << " number of rows in header: " << p.column->size();
        }

        DB::MutableColumns columns = query_result.mutateColumns();

        size_t number_of_columns = columns.size();
        ASSERT_EQ(number_of_columns, 3U);

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
        LOG(INFO) << "resolved data type is: " << dataTypePtr->getName();
        // Inspect array column. Note the following code followes what is implemented in
        // ./Storages/System/StorageSystemRowPolicies.cpp
        auto& array_data = assert_cast<DB::ColumnUInt32&>(column_0.getData());
        auto& array_data_offsets = column_0.getOffsets();

        size_t number_of_elements = array_data.size(); // this is the array element
        ASSERT_EQ(number_of_elements, 3U);             // Only having 1 row, but three elements in total.

        // The following is the array element in each row.
        for (size_t i = 0; i < number_of_elements; i++) {
            uint32_t val = array_data.getData()[i];
            LOG(INFO) << "retrieved array element: " << i << " with value: " << val;
            ASSERT_EQ((long)val, array_counter[i]);
        }

        // The boundary of the array element is defined by the offsets.  In one row array, we have
        // one value in the offset array, which is the element in the row, 3.
        size_t values_in_offsets = array_data_offsets.size();
        for (size_t i = 0; i < values_in_offsets; i++) {
            LOG(INFO) << "values in offset is: " << array_data_offsets[i];
        }

        ASSERT_EQ(values_in_offsets, 1);
        ASSERT_EQ(array_data_offsets[0], 3);

        // Inspect string columns
        auto column_1_string = column_1.getDataAt(0); // We only have 1 row.
        std::string column_1_real_string(column_1_string.data, column_1_string.size);
        auto column_2_string = column_2.getDataAt(0); // We only have 1 row.
        std::string column_2_real_string(column_2_string.data, column_2_string.size);

        LOG(INFO) << "column 1 has string value: " << column_1_real_string;
        LOG(INFO) << "column 2 has string value: " << column_2_real_string;

        ASSERT_EQ(column_1_real_string, string_value_1);
        ASSERT_EQ(column_2_real_string, string_value_2);
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * SELECT * FROM simple_event_63

    ┌─Counters────────────────────┬─Host──────┬─Colo─┐
    │ [[123,456,789],[1011,1213]] │ graphdb-1 │ LVS  │
    └─────────────────────────────┴───────────┴──────┘
    ┌─Counters─────────────────────────────────┬─Host──────────┬─Colo──────────┐
    │ [[393890,393891,393892],[393893,393894]] │ abcdefghi1234 │ ABCDEFGHI1234 │
    └──────────────────────────────────────────┴───────────────┴───────────────┘
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest, InsertOneRowWithNestedArrayWithDepth2) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_63";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into simple_event_63 values(?, ?, ?);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_event_63 values(?, ?, ?);";
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

    std::vector<long> array_counter;
    std::string string_value_1 = "abcdefghi1234";
    std::string string_value_2 = "ABCDEFGHI1234";

    srand(time(NULL));
    {
        // value 1, Counters
        long start_uint_value = rand() % 1000000;
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        {
            // each array value is also an array, with three elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
        }
        {
            // each array value is also an array, with two elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
        }

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(Array(UInt32))"));
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
    std::string query_on_table = "select Counters, \n" /* 0. Array (Array(UInt32))*/
                                 "     Host,\n"        /* 1. type: String */
                                 "     Colo\n"         /* 2. type: String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

    auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
        const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
        int column_index = 0;

        for (auto& p : columns_with_type_and_name) {
            LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                      << " column type: " << p.type->getName() << " column name: " << p.name
                      << " number of rows (in header): " << p.column->size();
        }

        DB::MutableColumns columns = query_result.mutateColumns();

        size_t number_of_columns = columns.size();
        ASSERT_EQ(number_of_columns, 3U);

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
        LOG(INFO) << "resolved data type is: " << dataTypePtr->getName();

        // invoke the array flatten function
        size_t input_rows_count = 1;
        DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
        const DB::ColumnArray* flattened_array_column = DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
        DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

        // Inspect array column. Note the following code followes what is implemented in
        // ./Storages/System/StorageSystemRowPolicies.cpp
        auto& array_data = assert_cast<DB::ColumnUInt32&>(flattened_array_column_2->getData());
        auto& array_data_offsets = flattened_array_column_2->getOffsets();

        size_t number_of_elements = array_data.size(); // this is the array element
        ASSERT_EQ(number_of_elements, 5U);             // Only having 1 row, but five elements in total from the array.

        // The following is the array element in each row.
        for (size_t i = 0; i < number_of_elements; i++) {
            uint32_t val = array_data.getData()[i];
            LOG(INFO) << "retrieved array element: " << i << " with value: " << val;
            ASSERT_EQ((long)val, array_counter[i]);
        }

        // The boundary of the array element is defined by the offsets.  In one-row column array, we have
        // one value in the offset array, which is the element in the single row, 5.
        size_t values_in_offsets = array_data_offsets.size();
        for (size_t i = 0; i < values_in_offsets; i++) {
            LOG(INFO) << "values in offset is: " << array_data_offsets[i];
        }

        ASSERT_EQ(values_in_offsets, 1);
        ASSERT_EQ(array_data_offsets[0], 5);

        // Inspect string columns
        auto column_1_string = column_1.getDataAt(0); // We only have 1 row.
        std::string column_1_real_string(column_1_string.data, column_1_string.size);
        auto column_2_string = column_2.getDataAt(0); // We only have 1 row.
        std::string column_2_real_string(column_2_string.data, column_2_string.size);

        LOG(INFO) << "column 1 has string value: " << column_1_real_string;
        LOG(INFO) << "column 2 has string value: " << column_2_real_string;

        ASSERT_EQ(column_1_real_string, string_value_1);
        ASSERT_EQ(column_2_real_string, string_value_2);
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * SELECT * FROM simple_event_63

    ┌─Counters─────────────────────────────────┬─Host──────────┬─Colo──────────┐
    │ [[799449,799450,799451],[799452,799453]] │ abcdefghi1234 │ ABCDEFGHI1234 │
    │ [[486562,486563,486564],[486565,486566]] │ abcdefghi1234 │ ABCDEFGHI1234 │
    │ [[119020,119021,119022],[119023,119024]] │ abcdefghi1234 │ ABCDEFGHI1234 │
    │ [[121826,121827,121828],[121829,121830]] │ abcdefghi1234 │ ABCDEFGHI1234 │
    │ [[484336,484337,484338],[484339,484340]] │ abcdefghi1234 │ ABCDEFGHI1234 │
    └──────────────────────────────────────────┴───────────────┴───────────────┘
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest, InsertMultipleRowsWithNestedArrayWithDepth2) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_63";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into simple_event_63 values(?, ?, ?);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_event_63 values(?, ?, ?);";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    std::vector<long> array_counter;
    std::string string_value_1 = "abcdefghi1234";
    std::string string_value_2 = "ABCDEFGHI1234";

    size_t rows = 5;
    srand(time(NULL));

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

        // value 1, Counters
        long start_uint_value = rand() % 1000000;
        // value 3, list of long values
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        {
            // each array value is also an array, with three elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);

            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);

            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
        }
        {
            // each array value is also an array, with two elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);

            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
        }

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(Array(UInt32))"));
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
    std::string query_on_table = "select Counters, \n" /* 0. Array (Array(UInt32))*/
                                 "     Host,\n"        /* 1. type: String */
                                 "     Colo\n"         /* 2. type: String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

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

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
        LOG(INFO) << "resolved data type is: " << dataTypePtr->getName();

        // invoke the array flatten function
        size_t input_rows_count = rows;
        DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
        const DB::ColumnArray* flattened_array_column = DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
        DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

        // Inspect array column. Note the following code follows what is implemented in
        // ./Storages/System/StorageSystemRowPolicies.cpp
        auto& array_data = assert_cast<DB::ColumnUInt32&>(flattened_array_column_2->getData());
        auto& array_data_offsets = flattened_array_column_2->getOffsets();

        size_t number_of_elements = array_data.size(); // this is the total array element
        ASSERT_EQ(number_of_elements,
                  (size_t)(rows * 5)); // Having 5 row, each row ha five elements in total from the array.

        // The following is the array element in each row.
        for (size_t i = 0; i < number_of_elements; i++) {
            uint32_t val = array_data.getData()[i];
            LOG(INFO) << "retrieved array element: " << i << " with value: " << val;
            ASSERT_EQ((long)val, array_counter[i]);
        }

        // The boundary of the array element is defined by the offsets.  In one-row column array, we have
        // one value in the offset array, which is the element in the single row, 5.
        size_t values_in_offsets = array_data_offsets.size();
        for (size_t i = 0; i < values_in_offsets; i++) {
            LOG(INFO) << "values in offset is: " << array_data_offsets[i];
        }

        ASSERT_EQ(values_in_offsets, rows);
        for (size_t i = 0; i < rows; i++) {
            ASSERT_EQ(array_data_offsets[i],
                      (size_t)(rows * (i + 1))); // Each row we have 5 elements, then increasing for each row.
        }

        for (size_t row_num = 0; row_num < rows; row_num++) {
            auto column_1_string = column_1.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_2.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, string_value_1);
            ASSERT_EQ(column_2_real_string, string_value_2);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * create table simple_event_64 (
     Counters Array(Array(Nullable(String))),
     Host String,
     Colo String)

   ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_64', '{replica}') ORDER BY(Host) SETTINGS
 index_granularity=8192;

   insert into simple_event_64 (`Counters`, `Host`, `Colo`) VALUES ([[Null, Null, 'abc'], ['cde', Null, Null]],
 'host-1', 'colo-1');

   SELECT * FROM simple_event_64

    ┌─Counters────────────────────────────────────────┬─Host─────┬─Colo─────┐
    │ [['384505',NULL,NULL],['384506','384507',NULL]] │ abc12345 │ xyz12345 │
    └─────────────────────────────────────────────────┴──────────┴──────────┘
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest, InsertOneRowWithNestedArrayWithDepth2AndNullable) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_64";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    // for table: simple_nullable_event_2;
    std::string query = "insert into simple_event_64 values(?, ?, ?);";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string table = table_name;
    std::string sql = "insert into simple_event_64 values(?, ?, ?);";
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

    nucolumnar::datatypes::v1::NullValueP nullValueP = nucolumnar::datatypes::v1::NullValueP::NULL_VALUE;

    std::vector<std::optional<std::string>> array_string_values;
    std::string const_string_value_1 = "abcdefghi1234";
    std::string const_string_value_2 = "ABCDEFGHI1234";

    srand(time(NULL));
    {
        // value 1, Counters
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        long start_uint_value = rand() % 1000000;

        {
            // each array value is also an array, with three elements: not null, null, null
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            std::string string_val = std::to_string(start_uint_value++);

            array_string_values.push_back(string_val);
            array_value_nested_array->add_value()->set_string_value(string_val);

            array_string_values.push_back(std::nullopt);
            array_value_nested_array->add_value()->set_null_value(nullValueP);

            array_string_values.push_back(std::nullopt);
            array_value_nested_array->add_value()->set_null_value(nullValueP);
        }
        {
            // each array value is also an array, with three elements: not null, not null, null.
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            std::string string_val_1 = std::to_string(start_uint_value++);
            array_string_values.push_back(string_val_1);
            array_value_nested_array->add_value()->set_string_value(string_val_1);

            std::string string_val_2 = std::to_string(start_uint_value++);
            array_string_values.push_back(string_val_2);
            array_value_nested_array->add_value()->set_string_value(string_val_2);

            array_string_values.push_back(std::nullopt);
            array_value_nested_array->add_value()->set_null_value(nullValueP);
        }

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value(const_string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value(const_string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(Array(Nullable(String)))"));
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
    std::string query_on_table = "select Counters, \n" /* 0. Array (Array(Nullable(String)))*/
                                 "     Host,\n"        /* 1. type: String */
                                 "     Colo\n"         /* 2. type: String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";
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

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
        LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): " << dataTypePtr->getName();

        // invoke the array flatten function
        size_t input_rows_count = 1;
        DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
        const DB::ColumnArray* flattened_array_column = DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
        DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

        // Inspect array column. Note the following code follows what is implemented in
        // ./Storages/System/StorageSystemRowPolicies.cpp
        auto& array_nullable_data = assert_cast<DB::ColumnNullable&>(flattened_array_column_2->getData());
        auto& array_nullable_data_offsets = flattened_array_column_2->getOffsets();

        size_t number_of_elements = array_nullable_data.size(); // this is the total array element
        ASSERT_EQ(number_of_elements, (size_t)6); // Having 1 row, each row has six elements in total from the array.

        auto nested_column = array_nullable_data.getNestedColumnPtr().get();
        const DB::ColumnString& resulted_column_string = assert_cast<const DB::ColumnString&>(*nested_column);
        // The following is the array element in each row.
        for (size_t i = 0; i < number_of_elements; i++) {
            auto column_string = resulted_column_string.getDataAt(i);
            std::string real_string(column_string.data, column_string.size);
            LOG(INFO) << "retrieved array element: " << i << " with value: " << real_string;
            if (array_string_values[i]) {
                ASSERT_EQ(real_string, *array_string_values[i]);
            } else {
                ASSERT_EQ(real_string, ""); // become the empty string.
            }
        }

        // The boundary of the array element is defined by the offsets.  In one-row column array, we have
        // one value in the offset array, which is the element in the single row, 5.
        size_t values_in_offsets = array_nullable_data_offsets.size();
        for (size_t i = 0; i < values_in_offsets; i++) {
            LOG(INFO) << "values in offset is: " << array_nullable_data_offsets[i];
        }

        ASSERT_EQ(values_in_offsets, 1); // only one row
        for (size_t i = 0; i < 1; i++) {
            // Each row we have 6 elements, then increasing for each row.
            ASSERT_EQ(array_nullable_data_offsets[i], (size_t)(6 * (i + 1)));
        }

        for (size_t row_num = 0; row_num < 1; row_num++) {
            auto column_1_string = column_1.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_2.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, const_string_value_1);
            ASSERT_EQ(column_2_real_string, const_string_value_2);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * To test insertion that deliberately not insert the column with nested array and then the server side should fill
 * up the missing value with system default.
 *
 * In the protobuf-message's sql field is: std::string sql = "insert into simple_event_64(Host, Colo) values(?, ?);". So
 that
 * we ask the protobuf reader only constructs the columns as specified in the statement.
 *
 * Then when we load the data to clickhouse, we use the following query:
 *   std::string query = "insert into " + table_name + " (Counters, Host, Colo) VALUES ();";
 * Note that this query covers the full columns. And thus we are forcing the clickhouse client-side runtime to perform
 * missing columns filling, and as a result, the loader query can run successfully with the following result:
 *
 * The following log statement shows up:
 *   LOG(WARNING) << " encounter the column: " << column.name << " that is both missing and not defined as a column with
 default value";
 *
 * SELECT * FROM simple_event_64

    ┌─Counters─┬─Host───┬─Colo───┐
    │ []       │ 119281 │ 119282 │
    └──────────┴────────┴────────┘
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest,
       InsertOneRowWithNestedArrayMissingAndForceSystemToFillDefaultValues) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_64";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    loader.init();

    LOG(INFO) << "to construct and serialized a message";
    std::string sql = "insert into simple_event_64(Host, Colo) values(?, ?);";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    // Used when doing the loading, not the one used for protobuf-message above to determine which de-serializer to load
    // values specified in VALUES does no get used for parsing.
    std::string query = "insert into " + table_name + " (Counters, Host, Colo) VALUES ();";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();

    std::vector<std::optional<std::string>> array_string_values;
    std::string string_value_1;
    std::string string_value_2;

    srand(time(NULL));

    // only Host and Colo columns are filled in, but not the nested array column.
    {
        long start_uint_value = rand() % 1000000;
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        string_value_1 = std::to_string(start_uint_value++);
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        string_value_2 = std::to_string(start_uint_value++);
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(Array(Nullable(String)))"));
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

    // Use the query statement that specifies the full columns, even though the columns provided has "Counters" column
    // missing.
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Counters, \n" /* 0. Array (Array(Nullable(String)))*/
                                 "     Host,\n"        /* 1. type: String */
                                 "     Colo\n"         /* 2. type: String */
                                 "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

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

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
        LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): " << dataTypePtr->getName();

        // invoke the array flatten function
        size_t input_rows_count = 1;
        DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
        const DB::ColumnArray* flattened_array_column = DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
        DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

        // Inspect array column. Note the following code follows what is implemented in
        // ./Storages/System/StorageSystemRowPolicies.cpp
        auto& array_nullable_data = assert_cast<DB::ColumnNullable&>(flattened_array_column_2->getData());
        auto& array_nullable_data_offsets = flattened_array_column_2->getOffsets();

        size_t number_of_elements = array_nullable_data.size(); // this is the total array element
        ASSERT_EQ(number_of_elements, (size_t)0);               // Having no element at all in the row.

        auto nested_column = array_nullable_data.getNestedColumnPtr().get();
        const DB::ColumnString& resulted_column_string = assert_cast<const DB::ColumnString&>(*nested_column);
        // The following is the array element in each row. since number of elements is 0, no execution on the following
        // loop.
        for (size_t i = 0; i < number_of_elements; i++) {
            auto column_string = resulted_column_string.getDataAt(i);
            std::string real_string(column_string.data, column_string.size);
            LOG(INFO) << "retrieved array element: " << i << " with value: " << real_string;
            ASSERT_EQ(real_string, ""); // become the empty string.
        }

        // The boundary of the array element is defined by the offsets.  In one-row column array, we have
        // one value in the offset array, which is the element in the single row, 0.
        size_t values_in_offsets = array_nullable_data_offsets.size();
        for (size_t i = 0; i < values_in_offsets; i++) {
            LOG(INFO) << "values in offset is: " << array_nullable_data_offsets[i];
        }

        ASSERT_EQ(values_in_offsets, 1); // only one row
        for (size_t i = 0; i < 1; i++) {
            // Each row we have 0 elements, then increasing for each row.
            ASSERT_EQ(array_nullable_data_offsets[i], (size_t)0);
        }

        for (size_t row_num = 0; row_num < 1; row_num++) {
            auto column_1_string = column_1.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_2.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, string_value_1);
            ASSERT_EQ(column_2_real_string, string_value_2);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * Create table simple_event_65 (
     Counters Array(Array(Nullable(String))) default [['aaa', NULL]],
     Host String,
     Colo String)

  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_65', '{replica}') ORDER BY(Host) SETTINGS
 index_granularity=8192;

  From the client, we specify only two columns (with default column to be filled),
       std::string sql = "insert into simple_event_65(Host, Colo) values(?, ?);";

  When we do the loader insertion, we specify three columns (that is how the actual Aggregator Loader code behaves):
         std::string query = "insert into " + table_name + " (Counters, Host, Colo) VALUES ();";

  And thus it forces the Aggregator to perform default value add at the aggregator.

  SELECT *  FROM simple_event_65

    ┌─Counters───────┬─Host───┬─Colo───┐
    │ [['aaa',NULL]] │ 694398 │ 694399 │
    └────────────────┴────────┴────────┘
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest,
       InsertOneRowWithNestedArrayMissingWithDefaultValuesFilledByClickHouse) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_65";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);

    // Instead of creating for testing, retrieved from the backend.
    // nuclm::TableColumnsDescription table_definition (table_name);

    // table_definition.addColumnDescription(nuclm::TableColumnDescription ("Counters",
    // "Array(Array(Nullable(String)))")); table_definition.addColumnDescription(nuclm::TableColumnDescription ("Host",
    // "String")); table_definition.addColumnDescription(nuclm::TableColumnDescription ("Colo", "String"));

    LOG(INFO) << "retrieved table definition using Aggregator Loader: " << table_name;
    const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
    LOG(INFO) << " with definition: " << table_definition.str();

    size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
    LOG(INFO) << " size of default columns count is: " << default_columns_count;

    ASSERT_TRUE(default_columns_count == 1);
    size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
    LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

    LOG(INFO) << "to construct and serialized a message";
    std::string sql = "insert into simple_event_65(Host, Colo) values(?, ?);";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    // Used when doing the loading, not the one used for protobuf-message above to determine which de-serializer to load
    // values specified in VALUES does no get used for parsing.
    //
    std::string query = "insert into " + table_name + " (Counters, Host, Colo) VALUES ();";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();

    srand(time(NULL));

    std::string string_value_1;
    std::string string_value_2;

    // only Host and Colo columns are filled in, but not the nested array column.
    {
        long start_uint_value = rand() % 1000000;
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        string_value_1 = std::to_string(start_uint_value++);
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        string_value_2 = std::to_string(start_uint_value++);
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

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

    // Use the query statement that specifies the full columns, even though the columns provided has "Counters" column
    // missing.
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    loader.init();
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table =
        "select Counters, \n" /* 0. Array (Array(Nullable(String))), with default value  [['aaa', NULL]] */
        "     Host,\n"        /* 1. type: String */
        "     Colo\n"         /* 2. type: String */
        "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

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

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
        LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): " << dataTypePtr->getName();

        // invoke the array flatten function
        size_t input_rows_count = 1;
        DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
        const DB::ColumnArray* flattened_array_column = DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
        DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

        // Inspect array column. Note the following code follows what is implemented in
        // ./Storages/System/StorageSystemRowPolicies.cpp
        auto& array_nullable_data = assert_cast<DB::ColumnNullable&>(flattened_array_column_2->getData());
        auto& array_nullable_data_offsets = flattened_array_column_2->getOffsets();

        size_t number_of_elements = array_nullable_data.size(); // this is the total array element
        ASSERT_EQ(number_of_elements, (size_t)2); // Having 1 row, each row has two elements in total from the array.

        auto nested_column = array_nullable_data.getNestedColumnPtr().get();
        const DB::ColumnString& resulted_column_string = assert_cast<const DB::ColumnString&>(*nested_column);
        // The following is the array element in each row.
        size_t non_empty_string_count = 0;
        for (size_t i = 0; i < number_of_elements; i++) {
            auto column_string = resulted_column_string.getDataAt(i);
            std::string real_string(column_string.data, column_string.size);
            LOG(INFO) << "retrieved array element: " << i << " with value: " << real_string;
            if (real_string.length() > 0) {
                ASSERT_EQ(real_string, "aaa");
                non_empty_string_count++;
            }
        }

        ASSERT_EQ(non_empty_string_count, 1); // one row, which has one "aaa" in the default value.

        // The boundary of the array element is defined by the offsets.  In one-row column array, we have
        // one value in the offset array, which is the element in the single row, 5.
        size_t values_in_offsets = array_nullable_data_offsets.size();
        for (size_t i = 0; i < values_in_offsets; i++) {
            LOG(INFO) << "values in offset is: " << array_nullable_data_offsets[i];
        }

        // ASSERT_EQ (values_in_offsets, 1); // only one row, with value = ["aaa", NULL]
        // for (size_t i=0; i < 1; i++ ) {
        //    One row having 2 elements.
        ASSERT_EQ(array_nullable_data_offsets[0], 2);
        //}

        for (size_t row_num = 0; row_num < 1; row_num++) {
            auto column_1_string = column_1.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_2.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, string_value_1);
            ASSERT_EQ(column_2_real_string, string_value_2);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 *
 * create table simple_event_66 (
     Counters1 Array(Array(UInt32)),
     Counters2 Array(Array(Nullable(String))) default [['aaa', NULL]],
     Host String,
     Colo String)

  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_66', '{replica}') ORDER BY(Host) SETTINGS
 index_granularity=8192;

 * To test out how blockAddMissing method defined in "BlockAddMissingDefaults.cpp" handle the situation: Column
 Counters1
 * is missing (not filled) by the application and it does not have default value, Column Counters2 is also missing (not
 filled)
 * by the application and it has default expression. At Aggregator, Counters1 will be filled with system-default values
 and Counters
 * will be filled with user-defined default expressions.
 *
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest,
       InsertOneRowWithNestedArrayColumnsMissingAndAggregatorAndSystemFillColumns1) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table_name = "simple_event_66";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    // Instead of creating for testing, retrieved from the backend.
    // nuclm::TableColumnsDescription table_definition (table_name);

    // table_definition.addColumnDescription(nuclm::TableColumnDescription ("Counters",
    // "Array(Array(Nullable(String)))")); table_definition.addColumnDescription(nuclm::TableColumnDescription ("Host",
    // "String")); table_definition.addColumnDescription(nuclm::TableColumnDescription ("Colo", "String"));

    LOG(INFO) << "retrieved table definition using Aggregator Loader: " << table_name;
    const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
    LOG(INFO) << " with definition: " << table_definition.str();

    size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
    LOG(INFO) << " size of default columns count is: " << default_columns_count;

    ASSERT_TRUE(default_columns_count == 1);
    size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
    LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

    LOG(INFO) << "to construct and serialized a message";
    // the application purposely miss Column Counters1 and Column Counters2
    std::string sql = "insert into simple_event_66(Host, Colo) values(?, ?);";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    // Used when doing the loading, not the one used for protobuf-message above to determine which de-serializer to load
    // values specified in VALUES does no get used for parsing.
    //
    std::string query = "insert into " + table_name + " (Counters1, Counters2, Host, Colo) VALUES ();";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();

    srand(time(NULL));

    // only Host and Colo columns are filled in, but not the nested array column.
    std::string string_value_1;
    std::string string_value_2;
    {
        long start_uint_value = rand() % 1000000;
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        string_value_1 = std::to_string(start_uint_value++);
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        string_value_2 = std::to_string(start_uint_value++);
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

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

    // Use the query statement that specifies the full columns, even though the columns provided has "Counters" column
    // missing.
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    loader.init();
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table =
        "select Counters1 , \n" /* 0. Array(Array(UInt32)), no default value */
        "     Counters2,  \n"   /* 1. Array (Array(Nullable(String))), with default value  [['aaa', NULL]] */
        "     Host,\n"          /* 3 type: String.  */
        "     Colo\n"           /* 2. type: String */
        "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

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
        ASSERT_EQ(number_of_columns, 4U);

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnArray&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);
        auto& column_3 = assert_cast<DB::ColumnString&>(*columns[3]);

        {
            // for column 1: Array(Array(Nullable(String))), default value:  [['aaa', NULL]]
            DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
            LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): "
                      << dataTypePtr->getName();

            // invoke the array flatten function
            size_t input_rows_count = 1;
            DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
            const DB::ColumnArray* flattened_array_column =
                DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
            DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

            // Inspect array column. Note the following code follows what is implemented in
            // ./Storages/System/StorageSystemRowPolicies.cpp
            auto& array_data = assert_cast<DB::ColumnUInt32&>(flattened_array_column_2->getData());
            auto& array_data_offsets = flattened_array_column_2->getOffsets();

            size_t number_of_elements = array_data.size(); // this is the array element
            ASSERT_EQ(number_of_elements, 0U); // Only having 1 row, but zero elements in total from the array.

            // The following is the array element in each row, but we have zero number of elements.
            for (size_t i = 0; i < number_of_elements; i++) {
                uint32_t val = array_data.getData()[i];
                LOG(INFO) << "retrieved array element: " << i << " with value: " << val;
            }

            // The boundary of the array element is defined by the offsets.  In one-row column array, we have
            // one value in the offset array, which is the element in the single row, 5.
            size_t values_in_offsets = array_data_offsets.size();
            for (size_t i = 0; i < values_in_offsets; i++) {
                LOG(INFO) << "value in offset is: " << array_data_offsets[i] << " for " << i << "-th array offset";
            }

            ASSERT_EQ(values_in_offsets, 1);
            ASSERT_EQ(array_data_offsets[0], 0);
        }

        {
            // for column 1: Array(Array(Nullable(String))), default value:  [['aaa', NULL]]
            DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[1].type);
            LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): "
                      << dataTypePtr->getName();

            // invoke the array flatten function
            size_t input_rows_count = 1;
            DB::ColumnPtr flattened_column = arrayFlattenFunction(column_1, input_rows_count);
            const DB::ColumnArray* flattened_array_column =
                DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
            DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

            // Inspect array column. Note the following code follows what is implemented in
            // ./Storages/System/StorageSystemRowPolicies.cpp
            auto& array_nullable_data = assert_cast<DB::ColumnNullable&>(flattened_array_column_2->getData());
            auto& array_nullable_data_offsets = flattened_array_column_2->getOffsets();

            size_t number_of_elements = array_nullable_data.size(); // this is the total array element
            ASSERT_EQ(number_of_elements,
                      (size_t)2); // Having 1 row, each row has two elements in total from the array.

            auto nested_column = array_nullable_data.getNestedColumnPtr().get();
            const DB::ColumnString& resulted_column_string = assert_cast<const DB::ColumnString&>(*nested_column);
            // The following is the array element in each row.
            size_t non_empty_string_count = 0;
            for (size_t i = 0; i < number_of_elements; i++) {
                auto column_string = resulted_column_string.getDataAt(i);
                std::string real_string(column_string.data, column_string.size);
                LOG(INFO) << "retrieved array element: " << i << " with value: " << real_string;
                if (real_string.length() > 0) {
                    ASSERT_EQ(real_string, "aaa");
                    non_empty_string_count++;
                }
            }

            ASSERT_EQ(non_empty_string_count, 1); // one row, which has one "aaa" in the default value.

            // The boundary of the array element is defined by the offsets.  In one-row column array, we have
            // one value in the offset array, which is the element in the single row, 5.
            size_t values_in_offsets = array_nullable_data_offsets.size();
            for (size_t i = 0; i < values_in_offsets; i++) {
                LOG(INFO) << "value in offset is: " << array_nullable_data_offsets[i] << " for " << i
                          << "-th array offset";
            }

            // ASSERT_EQ (values_in_offsets, 1); // only one row, with value = ["aaa", NULL]
            // for (size_t i=0; i < 1; i++ ) {
            //    One row having 2 elements.
            ASSERT_EQ(array_nullable_data_offsets[0], 2);
            //}
        }

        for (size_t row_num = 0; row_num < 1; row_num++) {
            auto column_1_string = column_2.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_3.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, string_value_1);
            ASSERT_EQ(column_2_real_string, string_value_2);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * Similar to the test case: InsertOneRowWithNestedArrayColumnsMissingAndAggregatorAndSystemFillColumns1, but Column
 Counters1
 * is filled with user-defined values, and only Column Counters2 is missing from the application. The purpose is to see
 * how the log statement in the method blockAndMissingDefaults show on the array-related columns being constructed.
 *
 * We can see the following logs:
 *
 * I0815 19:20:53.572716  8100 BlockAddMissingDefaults.cpp:42]  retrieved offsets_name from table-name-extraction is:
 Counters1 I0815 19:20:53.572728  8100 BlockAddMissingDefaults.cpp:46] before possible assignment, offsets_column is
 null I0815 19:20:53.572738  8100 BlockAddMissingDefaults.cpp:63] condition matches, offsets_column is assigned with:
 0x7f2a866b8010 I0815 19:20:53.572748  8100 BlockAddMissingDefaults.cpp:70] after possible assignment, offsets_column
 has family name: UInt64 and name: UInt64 and size: 1
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest,
       InsertOneRowWithNestedArrayColumnsMissingAndAggregatorAndSystemFillColumns2) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table_name = "simple_event_66";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    // Instead of creating for testing, retrieved from the backend.
    // nuclm::TableColumnsDescription table_definition (table_name);

    // table_definition.addColumnDescription(nuclm::TableColumnDescription ("Counters",
    // "Array(Array(Nullable(String)))")); table_definition.addColumnDescription(nuclm::TableColumnDescription ("Host",
    // "String")); table_definition.addColumnDescription(nuclm::TableColumnDescription ("Colo", "String"));

    LOG(INFO) << "retrieved table definition using Aggregator Loader: " << table_name;
    const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
    LOG(INFO) << " with definition: " << table_definition.str();

    size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
    LOG(INFO) << " size of default columns count is: " << default_columns_count;

    ASSERT_TRUE(default_columns_count == 1);
    size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
    LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

    LOG(INFO) << "to construct and serialized a message";
    // the application purposely miss Column Counters1 and Column Counters2
    std::string sql = "insert into simple_event_66(Counters1, Host, Colo) values(?, ?, ?);";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    // Used when doing the loading, not the one used for protobuf-message above to determine which de-serializer to load
    // values specified in VALUES does no get used for parsing.
    //
    std::string query = "insert into " + table_name + " (Counters1, Counters2, Host, Colo) VALUES ();";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();

    srand(time(NULL));
    std::vector<long> array_counter;
    {
        // fill out Counters1: Array(Array(UInt32))
        long start_uint_value = rand() % 1000000;
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        {
            // each array value is also an array, with three elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
        }
        {
            // each array value is also an array, with two elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            array_counter.push_back(start_uint_value);
            array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
        }

        // add val1
        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);
    }
    // only Host and Colo columns are filled in, but not the nested array column.
    std::string string_value_1;
    std::string string_value_2;
    {
        long start_uint_value = rand() % 1000000;
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        string_value_1 = std::to_string(start_uint_value++);
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        string_value_2 = std::to_string(start_uint_value++);
        val3->set_string_value(string_value_2);

        // add val2
        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        // add val3
        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

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

    // Use the query statement that specifies the full columns, even though the columns provided has "Counters" column
    // missing.
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    loader.init();
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    // to retrieve array counter with 5 elements, and to retrieve default array(array(nullable(string))).
    std::string table_being_queried = "default." + table_name;
    std::string query_on_table =
        "select Counters1 , \n" /* 0. Array(Array(UInt32)), no default value */
        "     Counters2,  \n"   /* 1. Array (Array(Nullable(String))), with default value  [['aaa', NULL]] */
        "     Host,\n"          /* 3 type: String.  */
        "     Colo\n"           /* 2. type: String */
        "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

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
        ASSERT_EQ(number_of_columns, 4U);

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnArray&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);
        auto& column_3 = assert_cast<DB::ColumnString&>(*columns[3]);

        {
            // for column 0: Array(Array(UInt32)), with one row.
            DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
            LOG(INFO) << "resolved data type is: " << dataTypePtr->getName();

            // invoke the array flatten function
            size_t input_rows_count = 1;
            DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
            const DB::ColumnArray* flattened_array_column =
                DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
            DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

            // Inspect array column. Note the following code follows what is implemented in
            // ./Storages/System/StorageSystemRowPolicies.cpp
            auto& array_data = assert_cast<DB::ColumnUInt32&>(flattened_array_column_2->getData());
            auto& array_data_offsets = flattened_array_column_2->getOffsets();

            size_t number_of_elements = array_data.size(); // this is the total array element
            ASSERT_EQ(
                number_of_elements,
                (size_t)(input_rows_count * 5)); // Having 5 row, each row ha five elements in total from the array.

            // The following is the array element in each row.
            for (size_t i = 0; i < number_of_elements; i++) {
                uint32_t val = array_data.getData()[i];
                LOG(INFO) << "retrieved array element: " << i << " with value: " << val;
                ASSERT_EQ((long)val, array_counter[i]);
            }

            // The boundary of the array element is defined by the offsets.  In one-row column array, we have
            // one value in the offset array, which is the element in the single row, 5.
            size_t values_in_offsets = array_data_offsets.size();
            for (size_t i = 0; i < values_in_offsets; i++) {
                LOG(INFO) << "values in offset is: " << array_data_offsets[i];
            }

            ASSERT_EQ(values_in_offsets, input_rows_count);
            for (size_t i = 0; i < input_rows_count; i++) {
                ASSERT_EQ(array_data_offsets[i],
                          (size_t)(5 * (i + 1))); // Each row we have 5 elements, then increasing for each row.
            }
        }

        {
            // for column 1: Array(Array(Nullable(String))), default value:  [['aaa', NULL]]
            DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[1].type);
            LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): "
                      << dataTypePtr->getName();

            // invoke the array flatten function
            size_t input_rows_count = 1;
            DB::ColumnPtr flattened_column = arrayFlattenFunction(column_1, input_rows_count);
            const DB::ColumnArray* flattened_array_column =
                DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
            DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

            // Inspect array column. Note the following code follows what is implemented in
            // ./Storages/System/StorageSystemRowPolicies.cpp
            auto& array_nullable_data = assert_cast<DB::ColumnNullable&>(flattened_array_column_2->getData());
            auto& array_nullable_data_offsets = flattened_array_column_2->getOffsets();

            size_t number_of_elements = array_nullable_data.size(); // this is the total array element
            ASSERT_EQ(number_of_elements,
                      (size_t)2); // Having 1 row, each row has two elements in total from the array.

            auto nested_column = array_nullable_data.getNestedColumnPtr().get();
            const DB::ColumnString& resulted_column_string = assert_cast<const DB::ColumnString&>(*nested_column);
            // The following is the array element in each row.
            size_t non_empty_string_count = 0;
            for (size_t i = 0; i < number_of_elements; i++) {
                auto column_string = resulted_column_string.getDataAt(i);
                std::string real_string(column_string.data, column_string.size);
                LOG(INFO) << "retrieved array element: " << i << " with value: " << real_string;
                if (real_string.length() > 0) {
                    ASSERT_EQ(real_string, "aaa");
                    non_empty_string_count++;
                }
            }

            ASSERT_EQ(non_empty_string_count, 1); // one row, which has one "aaa" in the default value.

            // The boundary of the array element is defined by the offsets.  In one-row column array, we have
            // one value in the offset array, which is the element in the single row, 5.
            size_t values_in_offsets = array_nullable_data_offsets.size();
            for (size_t i = 0; i < values_in_offsets; i++) {
                LOG(INFO) << "value in offset is: " << array_nullable_data_offsets[i] << " for " << i
                          << "-th array offset";
            }

            // ASSERT_EQ (values_in_offsets, 1); // only one row, with value = ["aaa", NULL]
            // for (size_t i=0; i < 1; i++ ) {
            //    One row having 2 elements.
            ASSERT_EQ(array_nullable_data_offsets[0], 2);
            //}
        }

        for (size_t row_num = 0; row_num < 1; row_num++) {
            auto column_1_string = column_2.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_3.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, string_value_1);
            ASSERT_EQ(column_2_real_string, string_value_2);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * This test is to make sure that in order to perform default column filling, we need to have manually constructed
 * table definition to include default expression for columns. Since column default expression is not part of schema
 hash,
 * the schema will not be updated if the manually constructed table definition and the true table definition in the
 backend
 * only have difference in terms of default column expression.
 *
 * With default column expression specified in the manually constructed table definition, we have the correct result:
 *   SELECT * FROM simple_event_65
        ┌─Counters───────┬─Host───┬─Colo───┐
        │ [['aaa',NULL]] │ 756476 │ 756477 │
        └────────────────┴────────┴────────┘
 *
 * In conclusion, manually constructed table definition needs to have default expression to be constructed also.
 */
TEST_F(AggregatorLoaderArraySerializerRelatedTest,
       InsertOneRowWithNestedArrayMissingWithDefaultValuesFilledByClickHouse2) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderArraySerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderArraySerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table_name = "simple_event_65";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    LOG(INFO) << "to construct and serialized a message";
    std::string sql = "insert into simple_event_65(Host, Colo) values(?, ?);";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    // Used when doing the loading, not the one used for protobuf-message above to determine which de-serializer to load
    // values specified in VALUES does no get used for parsing.
    //
    std::string query = "insert into " + table_name + " (Counters, Host, Colo) VALUES ();";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table_name);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();

    std::string string_value_1;
    std::string string_value_2;

    srand(time(NULL));

    // only Host and Colo columns are filled in, but not the nested array column.
    {
        long start_uint_value = rand() % 1000000;
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        string_value_1 = std::to_string(start_uint_value++);
        val2->set_string_value(string_value_1);
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        string_value_2 = std::to_string(start_uint_value++);
        val3->set_string_value(string_value_2);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table_name);

    table_definition.addColumnDescription(nuclm::TableColumnDescription(
        "Counters", "Array(Array(Nullable(String)))", nuclm::ColumnDefaultDescription("default", "[['aaa',NULL]]")));
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

    // Use the query statement that specifies the full columns, even though the columns provided has "Counters" column
    // missing.
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    loader.init();
    bool result = loader.load_buffer(table_name, query, block_holder);

    ASSERT_TRUE(result);

    // to retrieve array counter with 5 elements, and to retrieve default array(array(nullable(string))).
    std::string table_being_queried = "default." + table_name;
    std::string query_on_table =
        "select Counters , \n" /* 1. Array (Array(Nullable(String))), with default value  [['aaa', NULL]] */
        "     Host,\n"         /* 3 type: String.  */
        "     Colo\n"          /* 2. type: String */
        "from " +
        table_being_queried +
        "\n"
        "ORDER BY(Host)";

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

        auto& column_0 = assert_cast<DB::ColumnArray&>(*columns[0]);
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);

        {
            // for column 1: Array(Array(Nullable(String))), default value:  [['aaa', NULL]]
            DB::DataTypePtr dataTypePtr = resolveArrayType(columns_with_type_and_name[0].type);
            LOG(INFO) << "resolved most-primitive array data type is (should be with nullable): "
                      << dataTypePtr->getName();

            // invoke the array flatten function
            size_t input_rows_count = 1;
            DB::ColumnPtr flattened_column = arrayFlattenFunction(column_0, input_rows_count);
            const DB::ColumnArray* flattened_array_column =
                DB::checkAndGetColumn<DB::ColumnArray>(flattened_column.get());
            DB::ColumnArray* flattened_array_column_2 = const_cast<DB::ColumnArray*>(flattened_array_column);

            // Inspect array column. Note the following code follows what is implemented in
            // ./Storages/System/StorageSystemRowPolicies.cpp
            auto& array_nullable_data = assert_cast<DB::ColumnNullable&>(flattened_array_column_2->getData());
            auto& array_nullable_data_offsets = flattened_array_column_2->getOffsets();

            size_t number_of_elements = array_nullable_data.size(); // this is the total array element
            ASSERT_EQ(number_of_elements,
                      (size_t)2); // Having 1 row, each row has two elements in total from the array.

            auto nested_column = array_nullable_data.getNestedColumnPtr().get();
            const DB::ColumnString& resulted_column_string = assert_cast<const DB::ColumnString&>(*nested_column);
            // The following is the array element in each row.
            size_t non_empty_string_count = 0;
            for (size_t i = 0; i < number_of_elements; i++) {
                auto column_string = resulted_column_string.getDataAt(i);
                std::string real_string(column_string.data, column_string.size);
                LOG(INFO) << "retrieved array element: " << i << " with value: " << real_string;
                if (real_string.length() > 0) {
                    ASSERT_EQ(real_string, "aaa");
                    non_empty_string_count++;
                }
            }

            ASSERT_EQ(non_empty_string_count, 1); // one row, which has one "aaa" in the default value.

            size_t values_in_offsets = array_nullable_data_offsets.size();
            for (size_t i = 0; i < values_in_offsets; i++) {
                LOG(INFO) << "value in offset is: " << array_nullable_data_offsets[i] << " for " << i
                          << "-th array offset";
            }

            // ASSERT_EQ (values_in_offsets, 1); // only one row, with value = ["aaa", NULL]
            // for (size_t i=0; i < 1; i++ ) {
            //    One row having 2 elements.
            ASSERT_EQ(array_nullable_data_offsets[0], 2);
            //}
        }

        for (size_t row_num = 0; row_num < 1; row_num++) {
            auto column_1_string = column_1.getDataAt(row_num);
            std::string column_1_real_string(column_1_string.data, column_1_string.size);
            auto column_2_string = column_2.getDataAt(row_num);
            std::string column_2_real_string(column_2_string.data, column_2_string.size);

            LOG(INFO) << "column 1 has string value: " << column_1_real_string;
            LOG(INFO) << "column 2 has string value: " << column_2_real_string;

            ASSERT_EQ(column_1_real_string, string_value_1);
            ASSERT_EQ(column_2_real_string, string_value_2);
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
