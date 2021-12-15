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

class AggregatorLoaderColumnsDescriptionRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderColumnsDescriptionRelatedTest::shared_context = nullptr;

TEST_F(AggregatorLoaderColumnsDescriptionRelatedTest,
       checkNativeColumnsDescriptionCreatedTableDefinitionWithoutDefault) {
    std::string table_name = "simple_event_5";
    nuclm::TableColumnsDescription table_definition(table_name);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Count", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));

    const DB::ColumnsDescription& nativeColumnsDescription = table_definition.getNativeDBColumnsDescriptionCache();

    // Invoke the supported API: NamesAndTypesList getOrdinary() const
    DB::NamesAndTypesList namesAndTypesList = nativeColumnsDescription.getAll();
    {
        DB::Names columnNames = namesAndTypesList.getNames();
        DB::DataTypes columnTypes = namesAndTypesList.getTypes();
        ASSERT_EQ(columnNames.size(), (size_t)3);
        for (size_t i = 0; i < columnNames.size(); i++) {
            LOG(INFO) << "Reconstructed columns description for: " << table_name << " has column: " << i
                      << " with name: " << columnNames[i] << " and type: " << columnTypes[i]->getName();
        }

        for (size_t i = 0; i < columnNames.size(); i++) {
            const DB::ColumnDescription& nativeColumnDescription = nativeColumnsDescription.get(columnNames[i]);
            LOG(INFO) << "Reconstructed column description for column: " << i
                      << " with name: " << nativeColumnDescription.name
                      << " and type: " << nativeColumnDescription.type->getName();
        }

        // Check no defaults
        bool withDefaults = nativeColumnsDescription.hasDefaults();
        ASSERT_FALSE(withDefaults);

        // Check total size
        size_t columns_size = nativeColumnsDescription.size();
        ASSERT_EQ(columns_size, (size_t)3);
    }
}

TEST_F(AggregatorLoaderColumnsDescriptionRelatedTest,
       checkNativeColumnsDescriptionCreatedTableDefinitionWithDefaultValues) {
    std::string table_name = "simple_event_5";
    nuclm::TableColumnsDescription table_definition(table_name);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Count", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));
    table_definition.addColumnDescription(
        nuclm::TableColumnDescription("Memory", "UInt64", nuclm::ColumnDefaultDescription("default", "100")));
    table_definition.addColumnDescription(
        nuclm::TableColumnDescription("CPU", "UInt64", nuclm::ColumnDefaultDescription("default", "100")));

    const DB::ColumnsDescription& nativeColumnsDescription = table_definition.getNativeDBColumnsDescriptionCache();

    // Invoke the supported API: NamesAndTypesList getOrdinary() const
    DB::NamesAndTypesList namesAndTypesList = nativeColumnsDescription.getAll();
    {
        DB::Names columnNames = namesAndTypesList.getNames();
        DB::DataTypes columnTypes = namesAndTypesList.getTypes();
        ASSERT_EQ(columnNames.size(), (size_t)5);
        for (size_t i = 0; i < columnNames.size(); i++) {
            LOG(INFO) << "Reconstructed columns description for: " << table_name << " has column: " << i
                      << " with name: " << columnNames[i] << " and type: " << columnTypes[i]->getName();
        }

        for (size_t i = 0; i < columnNames.size(); i++) {
            const DB::ColumnDescription& nativeColumnDescription = nativeColumnsDescription.get(columnNames[i]);
            LOG(INFO) << "Reconstructed column description for column: " << i
                      << " with name: " << nativeColumnDescription.name
                      << " and type: " << nativeColumnDescription.type->getName();
        }

        // Check no defaults
        bool withDefaults = nativeColumnsDescription.hasDefaults();
        ASSERT_TRUE(withDefaults);

        DB::ColumnDefaults columnsWithDefaultValues = nativeColumnsDescription.getDefaults();
        for (auto it = columnsWithDefaultValues.begin(); it != columnsWithDefaultValues.end(); ++it) {
            LOG(INFO) << "default column: " << it->first
                      << " with default expression: " << it->second.expression->dumpTree(0);
        }

        // Check total size
        size_t columns_size = nativeColumnsDescription.size();
        ASSERT_EQ(columns_size, (size_t)5);
    }
}

TEST_F(AggregatorLoaderColumnsDescriptionRelatedTest,
       checkNativeColumnsDescriptionRetrievedTableDefinitionWithDefaultValues) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    std::string table_name = "ontime_with_nullable_extended";
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderColumnsDescriptionRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderColumnsDescriptionRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        {
            const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
            LOG(INFO) << " with definition: " << table_definition.str();

            size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
            LOG(INFO) << " size of default columns count is: " << default_columns_count;

            ASSERT_TRUE(default_columns_count == 3);
            size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
            LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

            ASSERT_TRUE(ordinary_columns_count == 8);

            const DB::ColumnsDescription& nativeColumnsDescription =
                table_definition.getNativeDBColumnsDescriptionCache();

            // Invoke the supported API: NamesAndTypesList getOrdinary() const
            DB::NamesAndTypesList namesAndTypesList = nativeColumnsDescription.getAll();
            {
                DB::Names columnNames = namesAndTypesList.getNames();
                DB::DataTypes columnTypes = namesAndTypesList.getTypes();
                ASSERT_EQ(columnNames.size(), (size_t)11);
                for (size_t i = 0; i < columnNames.size(); i++) {
                    LOG(INFO) << "Reconstructed columns description for: " << table_name << " has column: " << i
                              << " with name: " << columnNames[i] << " and type: " << columnTypes[i]->getName();
                }

                for (size_t i = 0; i < columnNames.size(); i++) {
                    const DB::ColumnDescription& nativeColumnDescription = nativeColumnsDescription.get(columnNames[i]);
                    LOG(INFO) << "Reconstructed column description for column: " << i
                              << " with name: " << nativeColumnDescription.name
                              << " and type: " << nativeColumnDescription.type->getName();
                }

                // Check no defaults
                bool withDefaults = nativeColumnsDescription.hasDefaults();
                ASSERT_TRUE(withDefaults);

                DB::ColumnDefaults columnsWithDefaultValues = nativeColumnsDescription.getDefaults();
                for (auto it = columnsWithDefaultValues.begin(); it != columnsWithDefaultValues.end(); ++it) {
                    LOG(INFO) << "default column: " << it->first
                              << " with default expression: " << it->second.expression->dumpTree(0);
                }

                ASSERT_EQ(columnsWithDefaultValues.size(), (size_t)3);

                // Check total size
                size_t columns_size = nativeColumnsDescription.size();
                ASSERT_EQ(columns_size, (size_t)11);
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
