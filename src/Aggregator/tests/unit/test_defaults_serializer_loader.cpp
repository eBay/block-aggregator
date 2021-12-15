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
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Parsers/ExpressionListParsers.h>
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

class AggregatorLoaderDefaultValuesSerializerRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context = nullptr;

static DB::ASTPtr build_ast_default_expression(DB::ParserExpression& parser, const std::string& expression) {
    DB::ASTPtr ast =
        DB::parseQuery(parser, expression.data(), expression.data() + expression.size(), "default expression", 0, 0);
    return ast;
}

static std::string formattedAST(const DB::ASTPtr& ast) {
    if (!ast) {
        return {};
    }
    DB::WriteBufferFromOwnString buf;
    DB::formatAST(*ast, buf, false, true);
    return buf.str();
}

TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest, testASTTreeParsingForDefaultExpression) {
    // string constant
    {
        std::string expression = "`normal`";
        DB::ParserExpression expr_parser;
        DB::ASTPtr expression_ptr = build_ast_default_expression(expr_parser, expression);

        {
            std::string result = formattedAST(expression_ptr);
            LOG(INFO) << "parse default expression result is: " << result;
        }
    }

    // constant
    {
        std::string expression = "-1";
        DB::ParserExpression expr_parser;
        DB::ASTPtr expression_ptr = build_ast_default_expression(expr_parser, expression);

        {
            std::string result = formattedAST(expression_ptr);
            LOG(INFO) << "parse default expression result is: " << result;
        }
    }

    // expression
    {
        std::string expression = "today()";
        DB::ParserExpression expr_parser;
        DB::ASTPtr expression_ptr = build_ast_default_expression(expr_parser, expression);

        {
            std::string result = formattedAST(expression_ptr);
            LOG(INFO) << "parse default expression result is: " << result;
        }
    }

    // expression: toDate(EventTime)
    {
        std::string expression = "toDate(EventTime)";
        DB::ParserExpression expr_parser;
        DB::ASTPtr expression_ptr = build_ast_default_expression(expr_parser, expression);

        {
            std::string result = formattedAST(expression_ptr);
            LOG(INFO) << "parse default expression result is: " << result;
        }
    }
}

TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest, testGetDefaultsFromTableDefinition) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    bool failed = false;
    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == "xdr_tst2") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;
                DB::ColumnDefaults column_defaults = table_definition.getColumnDefaults();
                LOG(INFO) << " ****retrieved column-defaults has size: " << column_defaults.size();

                size_t default_column_count = 0;
                for (std::pair<std::string, DB::ColumnDefault> column_default : column_defaults) {
                    LOG(INFO) << "column name is: " << column_default.first;
                    if (column_default.second.kind == DB::ColumnDefaultKind::Default) {
                        LOG(INFO) << " column default kind is: "
                                  << " default column";
                    } else if (column_default.second.kind == DB::ColumnDefaultKind::Materialized) {
                        LOG(INFO) << " column default kind is: "
                                  << " materialized column";
                    } else if (column_default.second.kind == DB::ColumnDefaultKind::Alias) {
                        LOG(INFO) << " column default kind is: "
                                  << " alias column";
                    }

                    {
                        DB::ASTPtr expression_ptr = column_default.second.expression;
                        std::string expression_str = formattedAST(expression_ptr);
                        LOG(INFO) << "default column's default expression is: " << expression_str;
                    }

                    default_column_count++;
                }

                ASSERT_EQ(default_columns_count, (size_t)2);
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
 * It runs and throw the expected exception.
 *
 *  In this test:
 *    (1) the insert SQL statement in protobuf message only specifies the non-default columns explicitly. and via
 protobuf reader we populate the default columns
 *
    I0701 15:02:39.026209  4585 test_defaults_serializer_loader.cpp:358] total number of rows in block holder: 5
    I0701 15:02:39.026216  4585 test_defaults_serializer_loader.cpp:360] column names dumped in block holder : date,
 answered, end_date, start_date I0701 15:02:39.026227  4585 test_defaults_serializer_loader.cpp:365] structure dumped in
 block holder: date Date UInt16(size = 5), answered Int8 Int8(size = 5), end_date UInt64 UInt64(size = 5), start_date
 UInt64 UInt64(size = 5)

     (2) The insert statement that we issue to the Aggregator Loader, also ONLY specified the explicit columns same as
 the insert SQL statement in protobuf message. We get the following exception message from the server:

     Received exception Code: 10. DB::Exception: Received from 10.169.98.238:9000. DB::Exception: Not found column date
 in block. There are only columns: end_date, start_date.

    The reason that it encounters the exception is because after protobuf-reader constructs the block, the reader
 constructs the block with the full-table-definition retrieved from the server side, which contains "date" column and
 "answered" column, totally 4 columns. However, the insert query statement only contains 2 columns. Therefore, the
 exception is because the final insert query specified for loader has the columns less than what has been already
 constructed in the block.

    The next test case: InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNames, after this one is to
 fix the insert query and avoid the exception.

 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNamesAndEncounterException) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    bool failed = false;
    try {

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == "xdr_tst2") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                size_t rows = 5;
                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    // This is from the input by the client application that is only aware of the table definitions with
                    // two columns, even though totally the server-side schema is with 4 columns.
                    std::string sql = "insert into " + table_name + " (end_date, start_date) values (?, ?)";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        val1->set_long_value(9123456 + r);
                        // value 2
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        val2->set_long_value(9223456 + r);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);
                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // for table: simple_event_5;
                std::string query = "insert into " + table_name + " (end_date, start_date) VALUES (9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                // The total number of the columns defined in xdr_tst2: date, answered, end_date, start_date.
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                // As we can see from the dump structure output, the dump structure already contains 4 columns, and thus
                // the insert query of "insert into xdr_tst2 (end_date, start_date) VALUES (9098, 9097);" is wrong. In
                // fact, when block insertion gets performed in Aggregator, the columns of the insert query query is
                // constructed from the retrieved table definition from the server. More specifically, via:
                //  BlockSupportedBufferFlushTask::formulateInsertQuery()
                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder that is constructed from protobuf-reader that takes "
                             "into account the server-side schema already: "
                          << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                // WE ARE AWARE OF THIS TEST CASE's CURRENT EXECUTION BEHAVIOR....SO WE ASSERT result = false
                ASSERT_FALSE(result);
            }
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        LOG(INFO) << "WE ARE AWARE OF THIS TEST CASE's CURRENT EXECUTION BEHAVIOR....SO WE ASSIGN failed = false.";
        failed = false;
    }

    ASSERT_FALSE(failed);
}

/**
 * Note this test case is to slightly change the insert query statement, compared to the previous test case:
 *   InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNamesAndEncounterException
 *
 * and check that the exception does not occur any more on:
 *     Received exception Code: 10. DB::Exception: Received from 10.194.224.19:9000. DB::Exception: Not found column
 date in block. There are only columns: end_date, start_date.

 * Even though the the client only provide 2 columns, the protobuf-reader populate the other two columns with default
 expression, and as a result, we have the
 * following query results:
 *
        ┌───────date─┬─answered─┬─end_date─┬─start_date─┐
        │ 2020-11-23 │       -1 │  9123456 │    9223456 │
        │ 2020-11-23 │       -1 │  9123457 │    9223457 │
        │ 2020-11-23 │       -1 │  9123458 │    9223458 │
        │ 2020-11-23 │       -1 │  9123459 │    9223459 │
        │ 2020-11-23 │       -1 │  9123460 │    9223460 │
        └────────────┴──────────┴──────────┴────────────┘
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNames) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

    try {

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    // This is from the input by the client application that is only aware of the table definitions with
                    // two columns, even though totally the server-side schema is with 4 columns.
                    std::string sql = "insert into " + table_name + +" (end_date, start_date) values (?, ?)";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val1->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value 2
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        val = initial_val + r;
                        val2->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);
                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // To have the insert query specification matches the one retrieved from the table definition retrieved
                // from the server side. as the current block is constructed based on this table definition. This table
                // definition does not need to be perfectly match with the server-side schema. But at the client loader
                // side, the local query specification and the constructed block's column definition will need to match,
                // to avoid server-side exception of:
                //    Received exception Code: 10. DB::Exception: Received from 10.194.224.19:9000. DB::Exception: Not
                //    found column date in block. There are only columns: end_date, start_date.

                // further, purposely not follow the ordering of the tables, as the block constructed has the column
                // names encoded.
                std::string query = "insert into " + table_name +
                    " (end_date, start_date, answered, date) VALUES (9098, 9097, -1, '1996-06-15 23:00:06');";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                // As we can see from the dump structure output, the dump structure already contains 4 columns, and thus
                // the insert query of "insert into xdr_tst2 (end_date, start_date) VALUES (9098, 9097);" is wrong. In
                // fact, when block insertion gets performed in Aggregator, the columns of the insert query query is
                // constructed from the retrieved table definition from the server. More specifically, via:
                //  BlockSupportedBufferFlushTask::formulateInsertQuery()
                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder that is constructed from protobuf-reader that takes "
                             "into account the server-side schema already: "
                          << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                // with the fix of insert query statement, the following one should be true.
                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with current computed date value is: " << days_since_epoch;
                        int date_difference = (int)days_since_epoch - (int)column_0_date_val;
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value to be default value : -1";
                        ASSERT_EQ(column_1_answered_value, -1);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * NOTE: This test case actually follows the previous test case that is working:
 *      InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNames
 *
 * In summary:  (1) protobuf reader fetches the most recent table definition from the server, and use this table
 definition to construct the
 *                  block and also to formulate the insert query statement. The block constructed and insert query
 statement are consistent
 *                  with respect to the table definition held by the aggregator.
 *              (2) Note that the aggregator does not need to have the most recent table definitions from the server,
 and if so, the server-side will populate
 *                  the columns with constant default values, as shown by the test case of:
 *
 *                  TEST_F(AggregatorLoaderRelatedTest,
 InsertARowToCHServerWithInputBlocksHavingMissingColumnsComparedToServerSide)
 *
 *                  in test_aggregator_loader.cpp
 *
 * The insertion scheme works. The test cases works with successful insertion. That we use the full explicit columns for
 the final block insertion.
 * And the block prepared for query insertion to clickhouse is fully populated at the end:
 *
    I0701 15:48:07.453977  5024 AggregatorLoader.cpp:91] Connected to ClickHouse belonging to server:
 graphdb-1-2454883.lvs02.dev.ebayc3.com server version 19.17.3 revision 54428. I0701 15:48:07.453999  5024
 test_defaults_serializer_loader.cpp:476] total number of rows in block holder: 1 I0701 15:48:07.454007  5024
 test_defaults_serializer_loader.cpp:478] column names dumped in block holder : date, answered, end_date, start_date
    I0701 15:48:07.454018  5024 test_defaults_serializer_loader.cpp:483] structure dumped in block holder: date Date
 UInt16(size = 1), answered Int8 Int8(size = 1), end_date UInt64 UInt64(size = 1), start_date UInt64 UInt64(size = 1)

 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 unique key combined is ever increasing.
 * and also use the query with "order by" clause to guarantee the same sequence of  the array population.
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertARowWithDefaultRowMissingAndInsertQueryWithAllColumnNamesFullSpecified) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    // This is provided from the client application that has the older version of the schema compared to
                    // the one published from the ClickHouse server
                    std::string sql = "insert into " + table_name + " (end_date, start_date) values (?, ?)";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val1->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value 2
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        val = initial_val + r;
                        val2->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);
                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                //  The insert query based on the aggregator loader's most recent knowledge of the table schema
                //  definitions retrieved from the server sometime ago. Note that the insert query statement does not
                //  need to match the ordering of the column definitions, as the block structure only contains the
                //  column name information. This is already demonstrated from the previous test case:
                //         InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNames
                std::string query = "insert into " + table_name +
                    " (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with current computed date value is: " << days_since_epoch;
                        int date_difference = (int)days_since_epoch - (int)column_0_date_val;
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value to be default value : -1";
                        ASSERT_EQ(column_1_answered_value, -1);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * The insertion scheme works! The reason is that the table definition used to construct the block is the same as the
 one from the server.
 *
 * to avoid specifies the explicit columns as they may not follow the exact order as the schema definition's order. We
 can have implicit insertion query
 * without specifying the column names as:
 *
 * insert query query expression is: INSERT INTO xdr_tst2 VALUES
 *
 * And the block that we constructed has been fully populated always with full columns:
 *
    I0701 16:02:03.849958  5299 test_defaults_serializer_loader.cpp:598] total number of rows in block holder: 1
    I0701 16:02:03.849967  5299 test_defaults_serializer_loader.cpp:600] column names dumped in block holder : date,
 answered, end_date, start_date I0701 16:02:03.849977  5299 test_defaults_serializer_loader.cpp:605] structure dumped in
 block holder: date Date UInt16(size = 1), answered Int8 Int8(size = 1), end_date UInt64 UInt64(size = 1), start_date
 UInt64 UInt64(size = 1)

 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 unique key combined is ever increasing.
 * and also use the query with "order by" clause to guarantee the same sequence of  the array population.
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertARowWithDefaultRowMissingAndInsertQueryWithAllColumnNamesImplicitlySpecified) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    // This is from the client application based on the schema that is already out-of-date, compared to
                    // the one that exists in the Clickhouse Server. But the aggregator does not use this out-of-date
                    // schema to construct the blocks. It adds the missing columns that are with default expressions, or
                    // without default expressions. When no default expression is specified, the system-provided default
                    // constant values will be populated to the columns that are missing in the client's insert data.
                    std::string sql = "insert into " + table_name + " (end_date, start_date) values (?, ?)";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val1->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value 2
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        val = initial_val + r;
                        val2->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);
                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // This is how the table insertion's columns to be implicitly specified. The reason is that the table
                // definition used to construct the block is the same as the one from the server.
                std::string query = "insert into " + table_name + " VALUES ('2020-02-27', 6, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with current computed date value is: " << days_since_epoch;
                        int date_difference = (int)days_since_epoch - (int)column_0_date_val;
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value to be default value : -1";
                        ASSERT_EQ(column_1_answered_value, -1);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * The default expression is just a constant: -1.  passed!
 * Here we need to specify all of the four columns, as the missing column "answered" will be fixed by the protobuf
 reader.
 *
    I0701 16:10:19.069043 12408 test_defaults_serializer_loader.cpp:743] final total number of rows in block holder: 1
    I0701 16:10:19.069053 12408 test_defaults_serializer_loader.cpp:745] final column names dumped in block holder :
 date, answered, end_date, start_date I0701 16:10:19.069069 12408 test_defaults_serializer_loader.cpp:750] final
 structure dumped in block holder: date Date UInt16(size = 1), answered Int8 Int8(size = 1), end_date UInt64 UInt64(size
 = 1), start_date UInt64 UInt64(size = 1)

   Here the Loader's insert query statement uses fully specified column names.

   insert into xdr_tst (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097); for table:
 xdr_tst

 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 unique key combined is ever increasing.
 * and also use the query with "order by" clause to guarantee the same sequence of  the array population.
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertARowWithDefaultRowMissingAndWithDefaultConstantWithFullExplicitColumnNames) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 1);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    std::string sql = "insert into " + table_name + " (date, end_date, start_date) VALUES (?, ?, ?);";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value 0: date for table xdr_tst, use now() to insert
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        auto ts = new nucolumnar::datatypes::v1::TimestampP();
                        ts->set_milliseconds(ms.count());
                        val1->set_allocated_timestamp(ts);

                        // value 1 is answered, with default value

                        // value 2 end_date for table xdr_tst
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val2->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value 3 start_date for table xdr_tst
                        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                        val = initial_val + r;
                        val3->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);

                        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                        pval3->CopyFrom(*val3);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);

                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // for table: xdr_tst2, here we need to specify all of the four columns, as the missing column
                // "answered" will be fixed by the protobuf reader.
                std::string query = "insert into " + table_name +
                    " (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "final total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "final column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "final structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";
                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with current computed date value is: " << days_since_epoch;
                        int date_difference = (int)days_since_epoch - (int)column_0_date_val;
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value to be default value : -1";
                        ASSERT_EQ(column_1_answered_value, -1);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 *  Same as InsertARowWithDefaultRowMissingAndWithDefaultConstantWithFullExplicitColumnNames, but the Loader's insertion
 query uses
 *  the implicit column names.
 *
 *  It works, as the protobuf reader will populate the column that is missing from the client insert data, based on the
 table definition
 *  that it retrieves from the ClickHouse server.

 *  Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 unique key combined is ever increasing.
 *  and also use the query with "order by" clause to guarantee the same sequence of  the array population.
 *
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertARowWithDefaultRowMissingAndWithDefaultConstantWithImplicitColumnNames) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 1);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    std::string sql = "insert into " + table_name + " (date, end_date, start_date) VALUES (?, ?, ?);";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value 1: date
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        auto ts = new nucolumnar::datatypes::v1::TimestampP();
                        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch());
                        ts->set_milliseconds(ms.count());
                        val1->set_allocated_timestamp(ts);

                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val2->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value 3
                        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                        val = initial_val + r;
                        val3->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);

                        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                        pval3->CopyFrom(*val3);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);

                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // use implicit column names.
                std::string query = "insert into " + table_name + " VALUES ('2020-02-27', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "final total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "final column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "final structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with current computed date value is: " << days_since_epoch;
                        int date_difference = (int)days_since_epoch - (int)column_0_date_val;
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value to be default value : -1";
                        ASSERT_EQ(column_1_answered_value, -1);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * This is to retrieve two non-default-expression columns, and the protobuf reader will populate totally 4 columns based
 on
 * the table definition retrieved from the ClickHouse server. Finally, the issued insert query is:
 *
 *  "insert into xdr_tst2 (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097);";
 *
 * Thus both populated columns and the insert query statement agree upon each other, and thus the insertion works.

 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 unique key combined is ever increasing.
 * and also use the query with "order by" clause to guarantee the same sequence of  the array population.
 *
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest, InsertARowWithDefaultRowMissingAndDefaultEvalFunction) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch());
    auto days_since_epoch = ms.count() / 1000 / 3600 / 24;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    std::string sql = "insert into " + table_name + " (end_date, start_date) VALUES (?, ?);";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();

                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();
                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val1->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value 2
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        val = initial_val + r;
                        val2->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);

                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // for table: xdr_tst2, here we need to specify all of the four columns, as the missing columns should
                // have been populated.
                std::string query = "insert into " + table_name +
                    " (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "final total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "final column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "final structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with current computed date value is: " << days_since_epoch;
                        int date_difference = (int)days_since_epoch - (int)column_0_date_val;
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value to be default value : -1";
                        ASSERT_EQ(column_1_answered_value, -1);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * This test works if we have all of the default columns to be fully populated, via the command of insert (?, ?, ?, ?)
 * or full column names specification  insert (col_1, col_2, ...)
 * status: passed.
 *
 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 * unique key combined is ever increasing. and also use the query with "order by" clause to guarantee the same sequence
 * of  the array population.
 *
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest, InsertARowWithDefaultRowNoMissing) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 5;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> date_value_array;
    std::vector<int8_t> answered_value_array;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());

                std::string serializedSqlBatchRequestInString;

                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    std::string sql = "insert into " + table_name + " values(?, ?, ?, ?)";
                    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                    nucolumnar::aggregator::v1::DataBindingList bindingList;
                    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                    sqlBatchRequest.set_shard(shard);
                    sqlBatchRequest.set_table(table);

                    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                        sqlBatchRequest.mutable_nucolumnarencoding();
                    sqlWithBatchBindings->set_sql(sql);
                    sqlWithBatchBindings->mutable_batch_bindings();
                    for (size_t r = 0; r < rows; r++) {
                        nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                            sqlWithBatchBindings->add_batch_bindings();

                        // val1, date:
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        // GMT: Wednesday, February 19, 2020 6:03:48 PM. Epoch time in milliseconds
                        long datetime_val = 1582135428000 + r;
                        val1->mutable_timestamp()->set_milliseconds(datetime_val);
                        date_value_array.push_back(datetime_val);

                        // val2, answered or not.
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        int8_t answered_val = 1 + r;
                        val2->set_int_value(answered_val);
                        answered_value_array.push_back(answered_val);

                        // value3: end_date;
                        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                        long val = initial_val + 2 * r;
                        val3->set_long_value(val);
                        end_date_value_array.push_back(val);

                        // value4: start_date:
                        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                        val = initial_val + r;
                        val4->set_long_value(val);
                        start_date_value_array.push_back(val);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);

                        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                        pval3->CopyFrom(*val3);

                        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                        pval4->CopyFrom(*val4);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);

                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // The insert query statement needs to respect all of the columns that have been populated by the
                // protobuf reader.
                std::string query = "insert into " + table_name +
                    " (`date`, `answered`, `end_date`, `start_date` ) VALUES ('2020-02-27', 9, 9098, 8097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with expected date value: " << date_value_array[i] / 1000 / 3600 / 24;
                        int date_difference = (int)column_0_date_val - (int)(date_value_array[i] / 1000 / 3600 / 24);
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value: " << (int)answered_value_array[i];
                        ASSERT_EQ(column_1_answered_value, answered_value_array[i]);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * Multiple batches, each one with no default columns missing with insert query statement at the time of loading to the
 * clickhouse matches the table definition.
 *
 * status: passed.
 *
 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 * unique key combined is ever increasing. and also use the query with "order by" clause to guarantee the same sequence
 * of  the array population.
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest, InsertARowWithDefaultRowNoMissingMultiBatches) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> date_value_array;
    std::vector<int8_t> answered_value_array;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());

                size_t batches = 3;
                size_t rows = 5;
                size_t counter_value = 0;

                for (size_t batch = 0; batch < batches; batch++) {
                    std::string serializedSqlBatchRequestInString;
                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql = "insert into " + table_name + " values(?, ?, ?, ?)";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();

                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();

                            // val1, date:
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            // GMT: Wednesday, February 19, 2020 6:03:48 PM. Epoch time in milliseconds
                            long datetime_val = 1582135428000 + counter_value;
                            val1->mutable_timestamp()->set_milliseconds(datetime_val);
                            date_value_array.push_back(datetime_val);

                            // val2, answered or not.
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            int8_t answered_val = counter_value;
                            val2->set_int_value(answered_val);
                            answered_value_array.push_back(answered_val);

                            // value3: end_date;
                            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                            long val = initial_val + 2 * counter_value;
                            val3->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value4: start_date:
                            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val4->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                            pval3->CopyFrom(*val3);

                            nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                            pval4->CopyFrom(*val4);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                } // end of multiple batches.

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // for table: simple_event_5;
                std::string query = "insert into " + table_name +
                    " (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, batches * rows);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                // we should have 15 rows here!!
                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";
                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    for (size_t i = 0; i < rows * batches; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with expected date value: " << date_value_array[i] / 1000 / 3600 / 24;
                        int date_difference = (int)column_0_date_val - (int)(date_value_array[i] / 1000 / 3600 / 24);
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value: " << (int)answered_value_array[i];
                        ASSERT_EQ(column_1_answered_value, answered_value_array[i]);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * Insertion of two rows: one have default values missing, and the other has default values populated. two rows get
 * consolidated with one block and at the end, the insert query statement to issue the block insertion specifies all of
 the
 * columns that are in the table definition.
 *
 * It works:
    I0701 16:30:59.538390 14447 test_defaults_serializer_loader.cpp:1532] total number of rows in block holder: 3
    I0701 16:30:59.538400 14447 test_defaults_serializer_loader.cpp:1534] column names dumped in block holder : date,
 answered, end_date, start_date I0701 16:30:59.538415 14447 test_defaults_serializer_loader.cpp:1539] structure dumped
 in block holder: date Date UInt16(size = 3), answered Int8 Int8(size = 3), end_date UInt64 UInt64(size = 3), start_date
 UInt64 UInt64(size = 3)
 *
 * Since the table uses (start date, end date) for primary key and thus ordering key, we need to make sure that the
 unique key combined is ever increasing.
 * and also use the query with "order by" clause to guarantee the same sequence of  the array population.
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertThreeRowsMixedWithDefaultRowNoMissingAndOtherMissingWithFullExplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 1;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> date_value_array;
    std::vector<int8_t> answered_value_array;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;

    size_t counter_value = 0;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());

                // sub-block-1: no row missing;
                {
                    std::string serializedSqlBatchRequestInString;

                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql = "insert into " + table_name + " values(?, ?, ?, ?)";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();
                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();

                            // val1, date:
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            // GMT: Wednesday, February 19, 2020 6:03:48 PM. Epoch time in milliseconds
                            long datetime_val = 1582135428000 + counter_value;
                            val1->mutable_timestamp()->set_milliseconds(datetime_val);
                            date_value_array.push_back(datetime_val);

                            // val2, answered or not.
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            int8_t answered_val = 1 + counter_value;
                            val2->set_int_value(answered_val);
                            answered_value_array.push_back(answered_val);

                            // value3: end_date;
                            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                            long val = initial_val + 2 * (counter_value);
                            val3->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value4: start_date:
                            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val4->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                            pval3->CopyFrom(*val3);

                            nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                            pval4->CopyFrom(*val4);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                }

                // sub-block 2: 1 default column missing.
                {
                    std::string serializedSqlBatchRequestInString;
                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql =
                            "insert into " + table_name + " (date, end_date, start_date) VALUES (?, ?, ?);";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();

                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();
                            // value 1: date
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            auto ts = new nucolumnar::datatypes::v1::TimestampP();
                            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch());
                            auto time_val = ms.count();
                            ts->set_milliseconds(time_val);
                            val1->set_allocated_timestamp(ts);
                            date_value_array.push_back(time_val);

                            // answered has the default value: -1
                            answered_value_array.push_back(-1);

                            // value end_date,
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            long val = initial_val + 2 * counter_value;
                            val2->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value 3
                            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val3->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                            pval3->CopyFrom(*val3);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                }

                // sub-block 3: with 2 default columns missing
                {
                    std::string serializedSqlBatchRequestInString;
                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql = "insert into " + table_name + " (end_date, start_date) VALUES (?, ?);";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();

                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();

                            // no insertion no today();
                            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch());
                            auto time_val = ms.count();
                            date_value_array.push_back(time_val);

                            // answered has the default value: -1
                            answered_value_array.push_back(-1);

                            // value end_date,
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            long val = initial_val + 2 * counter_value;
                            val1->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value 2
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val2->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                }

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // for full table insertion that includes all of the columns.
                std::string query = "insert into " + table_name +
                    " (`date`, `answered`, `end_date`, `start_date` ) VALUES ('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, (size_t)3);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";
                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    // 3 batches, each batch having one row.
                    for (size_t i = 0; i < rows * 3; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with expected date value: " << date_value_array[i] / 1000 / 3600 / 24;
                        int date_difference = column_0_date_val - (int)(date_value_array[i] / 1000 / 3600 / 24);
                        LOG(INFO) << " measured absolute time difference is: " << date_difference
                                  << " expected to be < 2 days";
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value: " << (int)answered_value_array[i];
                        ASSERT_EQ(column_1_answered_value, answered_value_array[i]);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
 * When the Aggregator Loader issue insert query to CH, use the implicit columns in the insert statement, which is:
 *
 *  insert query query expression is: INSERT INTO xdr_tst2 VALUES
 *
    I0701 16:36:00.998111 14573 test_defaults_serializer_loader.cpp:1788] total number of rows in block holder: 3
    I0701 16:36:00.998121 14573 test_defaults_serializer_loader.cpp:1790] column names dumped in block holder : date,
 answered, end_date, start_date I0701 16:36:00.998137 14573 test_defaults_serializer_loader.cpp:1795] structure dumped
 in block holder: date Date UInt16(size = 3), answered Int8 Int8(size = 3), end_date UInt64 UInt64(size = 3), start_date
 UInt64 UInt64(size = 3) I0701 16:36:00.998148 14573 AggregatorLoader.cpp:405]  loader received insert query: insert
 into xdr_tst2  VALUES ('02-15-2020', 8, 9098, 9097); for table: xdr_tst2

 * The insert query statement will return from the server side as part of the insertion protocol all of the columns held
 in the table definition, but
 * these columns match the table schema that is used to construct the blocks.
 *
 */
TEST_F(AggregatorLoaderDefaultValuesSerializerRelatedTest,
       InsertThreeRowsMixedWithDefaultRowNoMissingAndOtherMissingWithImplicitColumns) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderDefaultValuesSerializerRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string chosen_table_name = "xdr_tst2";
    bool removed = removeTableContent(context, ioc, chosen_table_name);
    LOG(INFO) << "remove table content for table: " << chosen_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    srand(time(NULL));
    bool failed = false;
    size_t rows = 1;

    int initial_val = rand() % 10000000;
    std::vector<uint64_t> date_value_array;
    std::vector<int8_t> answered_value_array;
    std::vector<uint64_t> end_date_value_array;
    std::vector<uint64_t> start_date_value_array;

    size_t counter_value = 0;

    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == chosen_table_name) {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());

                // sub-block-1: no row missing;
                {
                    std::string serializedSqlBatchRequestInString;

                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql = "insert into " + table_name + " values(?, ?, ?, ?)";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();
                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();

                            // val1, date:
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            // GMT: Wednesday, February 19, 2020 6:03:48 PM. Epoch time in milliseconds
                            long datetime_val = 1582135428000 + counter_value;
                            val1->mutable_timestamp()->set_milliseconds(datetime_val);
                            date_value_array.push_back(datetime_val);

                            // val2, answered or not.
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            int8_t answered_val = 1 + counter_value;
                            val2->set_int_value(answered_val);
                            answered_value_array.push_back(answered_val);

                            // value3: end_date;
                            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                            long val = initial_val + 2 * (counter_value);
                            val3->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value4: start_date:
                            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val4->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                            pval3->CopyFrom(*val3);

                            nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                            pval4->CopyFrom(*val4);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                }

                // sub-block 2: 1 default column missing.
                {
                    std::string serializedSqlBatchRequestInString;
                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql =
                            "insert into " + table_name + " (date, end_date, start_date) VALUES (?, ?, ?);";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();

                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();
                            // value 1: date
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            auto ts = new nucolumnar::datatypes::v1::TimestampP();
                            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch());
                            auto time_val = ms.count();
                            ts->set_milliseconds(time_val);
                            val1->set_allocated_timestamp(ts);
                            date_value_array.push_back(time_val);

                            // answered has the default value: -1
                            answered_value_array.push_back(-1);

                            // value end_date,
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            long val = initial_val + 2 * counter_value;
                            val2->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value 3
                            nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val3->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                            pval3->CopyFrom(*val3);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                }

                // sub-block 3: with 2 default columns missing
                {
                    std::string serializedSqlBatchRequestInString;

                    {
                        LOG(INFO) << "to construct and serialized a message";
                        std::string table = table_name;
                        std::string sql = "insert into " + table_name + " (end_date, start_date) VALUES (?, ?);";
                        std::string shard = "nudata.monstor.cdc.dev.marketing.1";

                        nucolumnar::aggregator::v1::DataBindingList bindingList;
                        nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
                        sqlBatchRequest.set_shard(shard);
                        sqlBatchRequest.set_table(table);

                        nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                            sqlBatchRequest.mutable_nucolumnarencoding();
                        sqlWithBatchBindings->set_sql(sql);
                        sqlWithBatchBindings->mutable_batch_bindings();

                        for (size_t r = 0; r < rows; r++) {
                            nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                                sqlWithBatchBindings->add_batch_bindings();

                            // no insertion no today();
                            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch());
                            auto time_val = ms.count();
                            date_value_array.push_back(time_val);

                            // answered has the default value: -1
                            answered_value_array.push_back(-1);

                            // value end_date,
                            nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                            long val = initial_val + 2 * counter_value;
                            val1->set_long_value(val);
                            end_date_value_array.push_back(val);

                            // value 2
                            nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                            val = initial_val + counter_value;
                            val2->set_long_value(val);
                            start_date_value_array.push_back(val);

                            nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                            pval1->CopyFrom(*val1);

                            nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                            pval2->CopyFrom(*val2);

                            counter_value++;
                        }

                        serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                    }

                    nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                        std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                    nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                           block_holder, context);

                    bool serialization_status = batchReader.read();
                    ASSERT_TRUE(serialization_status);
                }

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // use implicit column specification:
                std::string query = "insert into " + table_name + " VALUES ('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
                size_t total_number_of_columns = block_holder.columns();
                LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, (size_t)3);
                ASSERT_EQ(total_number_of_columns, (size_t)4);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);

                std::string table_being_queried = "default." + table_name;
                std::string query_on_table = "select date , \n"  /*  0. Date with default expression: today() */
                                             "     answered,\n"  /* Int8, with default expression: -1 */
                                             "     end_date,\n"  /* 2. UInt64 */
                                             "     start_date\n" /*  3. UInt64 */
                                             "from " +
                    table_being_queried +
                    "\n"
                    "order by (start_date, end_date)";

                auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
                    const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                        sample_block.getColumnsWithTypeAndName();
                    int column_index = 0;

                    for (auto& p : columns_with_type_and_name) {
                        LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                                  << " column type: " << p.type->getName() << " column name: " << p.name
                                  << " number of rows: " << p.column->size();
                    }

                    DB::MutableColumns columns = query_result.mutateColumns();

                    size_t number_of_columns = columns.size();
                    ASSERT_EQ(number_of_columns, 4U);

                    auto& column_0 = assert_cast<DB::ColumnUInt16&>(*columns[0]); // date
                    auto& column_1 = assert_cast<DB::ColumnInt8&>(*columns[1]);   // answered
                    auto& column_2 = assert_cast<DB::ColumnUInt64&>(*columns[2]); // end_date
                    auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]); // start_date

                    // 3 batches, each batch having one row.
                    for (size_t i = 0; i < rows * 3; i++) {
                        // date value retrieval.
                        auto column_0_date_val = column_0.getData()[i]; //
                        LOG(INFO) << "retrieved date value is: " << column_0_date_val
                                  << " with expected date value: " << date_value_array[i] / 1000 / 3600 / 24;
                        int date_difference = column_0_date_val - (int)(date_value_array[i] / 1000 / 3600 / 24);
                        LOG(INFO) << " measured absolute time difference is: " << date_difference
                                  << " expected to be < 2 days";
                        ASSERT_TRUE(std::abs(date_difference) < 2);

                        // answered value retrieval
                        auto column_1_answered_value = column_1.getData()[i];
                        // Need to cast to int value in order to show result on console.
                        LOG(INFO) << "retrieved answered value: " << (int)column_1_answered_value
                                  << " with expected value: " << (int)answered_value_array[i];
                        ASSERT_EQ(column_1_answered_value, answered_value_array[i]);

                        // end_date retrieval
                        auto column_2_end_date_val = column_2.getData()[i];
                        LOG(INFO) << "retrieved end date value: " << column_2_end_date_val
                                  << " with expected value: " << end_date_value_array[i];
                        ASSERT_EQ(column_2_end_date_val, end_date_value_array[i]);

                        // start_date retrieval
                        auto column_3_start_date_val = column_3.getData()[i];
                        LOG(INFO) << "retrieved start date value: " << column_3_start_date_val
                                  << " with expected value: " << start_date_value_array[i];
                        ASSERT_EQ(column_3_start_date_val, start_date_value_array[i]);
                    }
                };

                bool query_status =
                    inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
                ASSERT_TRUE(query_status);
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
