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

#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/ProtobufBatchReader.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include <limits.h>

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

class AggregatorProtobufReaderRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorProtobufReaderRelatedTest::shared_context = nullptr;

TEST_F(AggregatorProtobufReaderRelatedTest, testExtractColumnNames1) {
    std::string sql_statement = "insert into simple_event_5 values (?, ?, ?)";
    auto extracted_result = nuclm::ProtobufBatchReader::extractColumnNames(sql_statement, "simple_event_5");

    auto column_names = extracted_result.first;
    auto number_of_placeholders = extracted_result.second;
    ASSERT_EQ(column_names.size(), (size_t)0);
    ASSERT_EQ(number_of_placeholders, 3);
}

TEST_F(AggregatorProtobufReaderRelatedTest, testExtractColumnNames2) {
    std::string sql_statement = "insert into simple_event_5 (col_1, col_2, col_3) values (?, ?, ?)";
    auto extracted_result = nuclm::ProtobufBatchReader::extractColumnNames(sql_statement, "simple_event_5");

    auto column_names = extracted_result.first;
    ASSERT_EQ(column_names.size(), (size_t)3);
    ASSERT_EQ(column_names[0], "col_1");
    ASSERT_EQ(column_names[1], "col_2");
    ASSERT_EQ(column_names[2], "col_3");

    auto number_of_placeholders = extracted_result.second;
    ASSERT_EQ(number_of_placeholders, (size_t)3);
}

// Note single quote works!
TEST_F(AggregatorProtobufReaderRelatedTest, testExtractColumnNamesWithSingleQuotes) {
    std::string sql_statement = "insert into ontime (`flightYear`, `quarter`, `flightMonth`, `dayOfMonth`, "
                                "`dayOfWeek`, `flightDate`) VALUES (?, ?, ?, ?, ?, ?)";
    auto extracted_result = nuclm::ProtobufBatchReader::extractColumnNames(sql_statement, "ontime");
    auto column_names = extracted_result.first;

    ASSERT_EQ(column_names.size(), (size_t)6);
    ASSERT_EQ(column_names[0], "flightYear");
    ASSERT_EQ(column_names[1], "quarter");
    ASSERT_EQ(column_names[2], "flightMonth");
    ASSERT_EQ(column_names[3], "dayOfMonth");
    ASSERT_EQ(column_names[4], "dayOfWeek");
    ASSERT_EQ(column_names[5], "flightDate");

    auto number_of_placeholders = extracted_result.second;
    ASSERT_EQ(number_of_placeholders, (size_t)6);
}

// Note no single quote also works.
TEST_F(AggregatorProtobufReaderRelatedTest, testExtractColumnNamesdWithoutSingleQuotes) {
    std::string sql_statement = "insert into ontime (flightYear, quarter, flightMonth, dayOfMonth, "
                                "dayOfWeek, flightDate) VALUES (?, ?, ?, ?, ?, ?)";
    auto extracted_result = nuclm::ProtobufBatchReader::extractColumnNames(sql_statement, "ontime");
    auto column_names = extracted_result.first;

    ASSERT_EQ(column_names.size(), (size_t)6);
    ASSERT_EQ(column_names[0], "flightYear");
    ASSERT_EQ(column_names[1], "quarter");
    ASSERT_EQ(column_names[2], "flightMonth");
    ASSERT_EQ(column_names[3], "dayOfMonth");
    ASSERT_EQ(column_names[4], "dayOfWeek");
    ASSERT_EQ(column_names[5], "flightDate");

    auto number_of_placeholders = extracted_result.second;
    ASSERT_EQ(number_of_placeholders, (size_t)6);
}

TEST_F(AggregatorProtobufReaderRelatedTest, testRetrieveTableDefinitionWithoutDefaults1) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorProtobufReaderRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorProtobufReaderRelatedTest::shared_context->getIOContext();
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

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_EQ(default_columns_count, (size_t)0);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << default_columns_count;

                ASSERT_EQ(ordinary_columns_count, (size_t)3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder;
                std::string message = "";

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(message, schema_tracker_ptr, block_holder, context);

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type->getName(), "UInt64");
                ASSERT_EQ(full_columns_definition[0].name, "Count");

                ASSERT_EQ(full_columns_definition[1].type->getName(), "String");
                ASSERT_EQ(full_columns_definition[1].name, "Host");

                ASSERT_EQ(full_columns_definition[2].type->getName(), "String");
                ASSERT_EQ(full_columns_definition[2].name, "Colo");

                bool table_definition_followed = false;
                bool default_columns_missing = true;
                bool columns_not_covered_no_deflexpres = false;
                std::vector<size_t> order_mapping;
                nuclm::ColumnTypesAndNamesTableDefinition chosen_in_query = batchReader.determineColumnsDefinition(
                    sql_statement, table_definition_followed, default_columns_missing,
                    columns_not_covered_no_deflexpres, order_mapping, table_definition);

                ASSERT_EQ(chosen_in_query.size(), (size_t)3);
                // as the column sequence is with ?, ? ,? , ?
                ASSERT_TRUE(table_definition_followed);
                ASSERT_FALSE(default_columns_missing);
                ASSERT_FALSE(columns_not_covered_no_deflexpres);
                ASSERT_EQ(order_mapping.size(), (size_t)3);

                size_t column_index = 0;
                for (auto& column : chosen_in_query) {
                    LOG(INFO) << "chosen column: " << column_index << " column name: " << column.name
                              << " column type: " << column.type_name
                              << " with order mapping: " << order_mapping[column_index];
                    column_index++;
                }

                ASSERT_EQ(chosen_in_query[0].name, "Count");
                ASSERT_EQ(chosen_in_query[1].name, "Host");
                ASSERT_EQ(chosen_in_query[2].name, "Colo");
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

TEST_F(AggregatorProtobufReaderRelatedTest, testRetrieveTableDefinitionWithoutDefaults2) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorProtobufReaderRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorProtobufReaderRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string sql_statement = "insert into simple_event_5 (Colo, Host) values (?, ?)";

        for (const auto& table_name : table_names) {
            if (table_name == "simple_event_5") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_EQ(default_columns_count, (size_t)0);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << default_columns_count;

                ASSERT_EQ(ordinary_columns_count, (size_t)3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder;
                std::string message = "";

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(message, schema_tracker_ptr, block_holder, context);

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type_name, "UInt64");
                ASSERT_EQ(full_columns_definition[0].name, "Count");

                ASSERT_EQ(full_columns_definition[1].type_name, "String");
                ASSERT_EQ(full_columns_definition[1].name, "Host");

                ASSERT_EQ(full_columns_definition[2].type_name, "String");
                ASSERT_EQ(full_columns_definition[2].name, "Colo");

                bool table_definition_followed = false;
                bool default_columns_missing = true;
                bool columns_not_covered_no_deflexpres = false;
                std::vector<size_t> order_mapping;
                nuclm::ColumnTypesAndNamesTableDefinition chosen_in_query = batchReader.determineColumnsDefinition(
                    sql_statement, table_definition_followed, default_columns_missing,
                    columns_not_covered_no_deflexpres, order_mapping, table_definition);

                ASSERT_EQ(chosen_in_query.size(), (size_t)2);
                // as long as the column ordering is explicitly specified, it is false.
                ASSERT_FALSE(table_definition_followed);
                ASSERT_FALSE(default_columns_missing);
                ASSERT_TRUE(columns_not_covered_no_deflexpres);
                ASSERT_EQ(order_mapping.size(), (size_t)2);

                size_t column_index = 0;
                for (auto& column : chosen_in_query) {
                    LOG(INFO) << "chosen column: " << column_index << " column name: " << column.name
                              << " column type: " << column.type->getName()
                              << " with order mapping: " << order_mapping[column_index];
                    column_index++;
                }

                ASSERT_EQ(chosen_in_query[0].name, "Colo");
                ASSERT_EQ(chosen_in_query[1].name, "Host");
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

TEST_F(AggregatorProtobufReaderRelatedTest, testRetrieveTableDefinitionWithDefaults1) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorProtobufReaderRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorProtobufReaderRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string sql_statement = "insert into xdr_tst (date, answered, end_date, start_date) values (?, ?, ?, ?)";

        for (const auto& table_name : table_names) {
            if (table_name == "xdr_tst") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_EQ(default_columns_count, (size_t)1);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << default_columns_count;

                ASSERT_EQ(ordinary_columns_count, (size_t)3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder;
                std::string message = "";

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(message, schema_tracker_ptr, block_holder, context);

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type_name, "Date");
                ASSERT_EQ(full_columns_definition[0].type->getName(), "Date");
                ASSERT_EQ(full_columns_definition[0].name, "date");

                ASSERT_EQ(full_columns_definition[1].type_name, "Int8");
                ASSERT_EQ(full_columns_definition[1].type->getName(), "Int8");
                ASSERT_EQ(full_columns_definition[1].name, "answered");

                ASSERT_EQ(full_columns_definition[2].type_name, "UInt64");
                ASSERT_EQ(full_columns_definition[2].type->getName(), "UInt64");
                ASSERT_EQ(full_columns_definition[2].name, "end_date");

                ASSERT_EQ(full_columns_definition[3].type_name, "UInt64");
                ASSERT_EQ(full_columns_definition[3].type->getName(), "UInt64");
                ASSERT_EQ(full_columns_definition[3].name, "start_date");

                bool table_definition_followed = false;
                bool default_columns_missing = true;
                bool columns_not_covered_no_deflexpres = false;
                std::vector<size_t> order_mapping;
                nuclm::ColumnTypesAndNamesTableDefinition chosen_in_query = batchReader.determineColumnsDefinition(
                    sql_statement, table_definition_followed, default_columns_missing,
                    columns_not_covered_no_deflexpres, order_mapping, table_definition);

                ASSERT_EQ(chosen_in_query.size(), (size_t)4);

                // as long as the insertion has its own specific ordering, then table_definition_followed is false.
                ASSERT_FALSE(table_definition_followed);
                ASSERT_FALSE(default_columns_missing);
                ASSERT_FALSE(columns_not_covered_no_deflexpres);
                ASSERT_EQ(order_mapping.size(), (size_t)4);

                size_t column_index = 0;
                for (auto& column : chosen_in_query) {
                    LOG(INFO) << "chosen column: " << column_index << " column name: " << column.name
                              << " column type: " << column.type->getName()
                              << " with order mapping: " << order_mapping[column_index];
                    column_index++;
                }

                ASSERT_EQ(chosen_in_query[0].name, "date");
                ASSERT_EQ(chosen_in_query[1].name, "answered");
                ASSERT_EQ(chosen_in_query[2].name, "end_date");
                ASSERT_EQ(chosen_in_query[3].name, "start_date");
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

TEST_F(AggregatorProtobufReaderRelatedTest, testRetrieveTableDefinitionWithDefaults2) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorProtobufReaderRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorProtobufReaderRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string sql_statement = "insert into xdr_tst (answered, start_date, end_date) values (?, ?, ?)";

        for (const auto& table_name : table_names) {
            if (table_name == "xdr_tst") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_EQ(default_columns_count, (size_t)1);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << default_columns_count;

                ASSERT_EQ(ordinary_columns_count, (size_t)3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder;
                std::string message = "";

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(message, schema_tracker_ptr, block_holder, context);

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type_name, "Date");
                ASSERT_EQ(full_columns_definition[0].name, "date");

                ASSERT_EQ(full_columns_definition[1].type_name, "Int8");
                ASSERT_EQ(full_columns_definition[1].name, "answered");

                ASSERT_EQ(full_columns_definition[2].type_name, "UInt64");
                ASSERT_EQ(full_columns_definition[2].name, "end_date");

                ASSERT_EQ(full_columns_definition[3].type_name, "UInt64");
                ASSERT_EQ(full_columns_definition[3].name, "start_date");

                bool table_definition_followed = false;
                bool default_columns_missing = true;
                bool columns_not_covered_no_deflexpres = false;
                std::vector<size_t> order_mapping;
                nuclm::ColumnTypesAndNamesTableDefinition chosen_in_query = batchReader.determineColumnsDefinition(
                    sql_statement, table_definition_followed, default_columns_missing,
                    columns_not_covered_no_deflexpres, order_mapping, table_definition);

                ASSERT_EQ(chosen_in_query.size(), (size_t)3);

                // as long as the insertion has explicit column orders defined.
                ASSERT_FALSE(table_definition_followed);
                ASSERT_FALSE(default_columns_missing);
                ASSERT_TRUE(columns_not_covered_no_deflexpres);
                ASSERT_EQ(order_mapping.size(), (size_t)3);

                size_t column_index = 0;
                for (auto& column : chosen_in_query) {
                    LOG(INFO) << "chosen column: " << column_index << " column name: " << column.name
                              << " column type: " << column.type_name
                              << " with ordering mapping: " << order_mapping[column_index];
                    column_index++;
                }

                ASSERT_EQ(chosen_in_query[0].name, "answered");
                ASSERT_EQ(chosen_in_query[1].name, "start_date");
                ASSERT_EQ(chosen_in_query[2].name, "end_date");
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

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
