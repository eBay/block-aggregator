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

#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <memory>
#include <thread>
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

class AggregatorLoaderManagerRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderManagerRelatedTest::shared_context = nullptr;

TEST_F(AggregatorLoaderManagerRelatedTest, testInitializeLoaderConnection) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderManagerRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderManagerRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        bool status = manager.checkLoaderConnection();

        ASSERT_TRUE(status);

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorLoaderManagerRelatedTest, testInitDefinedTables) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderManagerRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderManagerRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& name : table_names) {
            LOG(INFO) << "retrieved table name: " << name;
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorLoaderManagerRelatedTest, testRetrieveAllTableDefinitions) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderManagerRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderManagerRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            LOG(INFO) << "retrieved table name: " << table_name;
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);

            LOG(INFO) << " with definition: " << table_definition.str();
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorLoaderManagerRelatedTest, testRetrieveTableDefinitionWithoutDefaults) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderManagerRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderManagerRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();

        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == "simple_event_5") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 0);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << default_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 3);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type->getName(), "UInt64");
                ASSERT_EQ(full_columns_definition[0].name, "Count");

                ASSERT_EQ(full_columns_definition[1].type->getName(), "String");
                ASSERT_EQ(full_columns_definition[1].name, "Host");

                ASSERT_EQ(full_columns_definition[2].type->getName(), "String");
                ASSERT_EQ(full_columns_definition[2].name, "Colo");
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

TEST_F(AggregatorLoaderManagerRelatedTest, testRetrieveTableDefinitionWithDefaults) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderManagerRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderManagerRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == "my_table") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;
                ASSERT_TRUE(default_columns_count == 1);

                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                ASSERT_TRUE(ordinary_columns_count == 1);
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type->getName(), "Date");
                ASSERT_EQ(full_columns_definition[0].name, "date");

                ASSERT_EQ(full_columns_definition[1].type->getName(), "String");
                ASSERT_EQ(full_columns_definition[1].name, "s");

                const nuclm::TableColumnDescription& column_1 = table_definition.getColumnDescription("date");
                ASSERT_EQ(column_1.column_name, "date");
                ASSERT_EQ(column_1.column_type, "Date");

                const nuclm::TableColumnDescription& column_2 = table_definition.getColumnDescription("s");
                ASSERT_EQ(column_2.column_name, "s");
                ASSERT_EQ(column_2.column_type, "String");

                std::vector<std::string> selected_columns_1{"s"};
                bool defaults_columns_included_1 = table_definition.queryIncludeAllDefaultColumns(selected_columns_1);
                ASSERT_EQ(defaults_columns_included_1, false);

                std::vector<std::string> selected_columns_2{"s", "date"};
                bool defaults_columns_included_2 = table_definition.queryIncludeAllDefaultColumns(selected_columns_2);
                ASSERT_EQ(defaults_columns_included_2, true);
                {
                    nuclm::ColumnTypesAndNamesTableDefinition chosen_columns_definition = {{"Date", "date"},
                                                                                           {"String", "s"}};

                    std::vector<size_t> sequence_1 =
                        table_definition.getColumnToSequenceMapping(chosen_columns_definition);
                    ASSERT_EQ(sequence_1.size(), (size_t)2);

                    ASSERT_EQ(sequence_1[0], (size_t)0);
                    ASSERT_EQ(sequence_1[1], (size_t)1);
                }

                {
                    nuclm::ColumnTypesAndNamesTableDefinition chosen_columns_definition = {{"String", "s"},
                                                                                           {"Date", "date"}};

                    std::vector<size_t> sequence_1 =
                        table_definition.getColumnToSequenceMapping(chosen_columns_definition);
                    ASSERT_EQ(sequence_1.size(), (size_t)2);

                    ASSERT_EQ(sequence_1[0], (size_t)1);
                    ASSERT_EQ(sequence_1[1], (size_t)0);
                }

            } else if (table_name == "xdr_tst") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;
                ASSERT_TRUE(default_columns_count == 1);

                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                ASSERT_TRUE(ordinary_columns_count == 3);
                LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                //{"Count", UInt64}, {"Host", "String"}, {"Colo", "String"}
                ASSERT_EQ(full_columns_definition[0].type->getName(), "Date");
                ASSERT_EQ(full_columns_definition[0].name, "date");

                ASSERT_EQ(full_columns_definition[1].type->getName(), "Int8");
                ASSERT_EQ(full_columns_definition[1].name, "answered");

                ASSERT_EQ(full_columns_definition[2].type->getName(), "UInt64");
                ASSERT_EQ(full_columns_definition[2].name, "end_date");

                ASSERT_EQ(full_columns_definition[3].type->getName(), "UInt64");
                ASSERT_EQ(full_columns_definition[3].name, "start_date");

                const nuclm::TableColumnDescription& column_1 = table_definition.getColumnDescription("date");
                ASSERT_EQ(column_1.column_name, "date");
                ASSERT_EQ(column_1.column_type, "Date");

                const nuclm::TableColumnDescription& column_2 = table_definition.getColumnDescription("start_date");
                ASSERT_EQ(column_2.column_name, "start_date");
                ASSERT_EQ(column_2.column_type, "UInt64");

                std::vector<std::string> selected_columns_1{"date"};
                bool defaults_columns_included_1 = table_definition.queryIncludeAllDefaultColumns(selected_columns_1);
                ASSERT_EQ(defaults_columns_included_1, false);

                std::vector<std::string> selected_columns_2{"date", "answered"};
                bool defaults_columns_included_2 = table_definition.queryIncludeAllDefaultColumns(selected_columns_2);
                ASSERT_EQ(defaults_columns_included_2, true);
                {
                    nuclm::ColumnTypesAndNamesTableDefinition chosen_columns_definition = {{"Date", "date"},
                                                                                           {"Int8", "answered"}};

                    std::vector<size_t> sequence_1 =
                        table_definition.getColumnToSequenceMapping(chosen_columns_definition);
                    ASSERT_EQ(sequence_1.size(), (size_t)2);

                    ASSERT_EQ(sequence_1[0], (size_t)0);
                    ASSERT_EQ(sequence_1[1], (size_t)1);
                }

                {
                    nuclm::ColumnTypesAndNamesTableDefinition chosen_columns_definition = {
                        {"UInt64", "end_date"}, {"UInt64", "start_date"}, {"Date", "date"}, {"Int8", "answered"}};

                    std::vector<size_t> sequence_1 =
                        table_definition.getColumnToSequenceMapping(chosen_columns_definition);
                    ASSERT_EQ(sequence_1.size(), (size_t)4);

                    ASSERT_EQ(sequence_1[0], (size_t)2);
                    ASSERT_EQ(sequence_1[1], (size_t)3);
                    ASSERT_EQ(sequence_1[2], (size_t)0);
                    ASSERT_EQ(sequence_1[3], (size_t)1);
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

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
