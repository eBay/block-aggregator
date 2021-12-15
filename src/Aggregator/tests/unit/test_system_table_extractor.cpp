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
#include <Aggregator/SystemStatusTableExtractor.h>
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
#include <DataTypes/DataTypeDateTime.h>
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

class SystemTableExtractorRelatedTest : public ::testing::Test {
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

ContextWrapper* SystemTableExtractorRelatedTest::shared_context = nullptr;

class CustomizedSystemStatusTableExtractor : public nuclm::SystemStatusTableExtractor {
  public:
    CustomizedSystemStatusTableExtractor(boost::asio::io_context& ioc,
                                         const nuclm::AggregatorLoaderManager& loader_manager_,
                                         DB::ContextMutablePtr context_) :
            nuclm::SystemStatusTableExtractor(ioc, loader_manager_, context_) {}

    bool doCustomizedExtractSystemReplicasWork(std::vector<nuclm::SystemReplicasExtractedResult>& row_results) {
        return doExtractSystemReplicasWork(row_results);
    }

    bool doCustomizedDoExtractSystemTablesWork(std::vector<nuclm::SystemTablesExtractedResult>& row_results) {
        return doExtractSystemTablesWork(row_results);
    }

    ~CustomizedSystemStatusTableExtractor() = default;
};

/**
 * The following test case is to find out internal data representation of ClickHouse DateTime column
 * in terms of how many sizes, so that we can cast the column data type correctly when retrieving
 * DateTime related fields in system.replicas table.
 *
 * The result is: 8
 */

TEST_F(SystemTableExtractorRelatedTest, testByteSizeOfTimeTsType) {
    time_t x;
    size_t size = sizeof(x);
    LOG(INFO) << "time_t size is: " << size;
    ASSERT_EQ(size, 8);
}

TEST_F(SystemTableExtractorRelatedTest, testExtractionOfSystemReplicasTable) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = SystemTableExtractorRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = SystemTableExtractorRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);
    CustomizedSystemStatusTableExtractor extractor(ioc, manager, context);

    std::vector<nuclm::SystemReplicasExtractedResult> replicas_retrieved_results;
    bool result = extractor.doCustomizedExtractSystemReplicasWork(replicas_retrieved_results);

    ASSERT_TRUE(result);

    size_t counter = 0;
    for (auto& p : replicas_retrieved_results) {
        LOG(INFO) << " retrieved system.replicas index: " << counter++ << "values: " << p.str();
    }
}

TEST_F(SystemTableExtractorRelatedTest, testExtractionOfSystemTables) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = SystemTableExtractorRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = SystemTableExtractorRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);
    CustomizedSystemStatusTableExtractor extractor(ioc, manager, context);

    std::vector<nuclm::SystemTablesExtractedResult> tables_retrieved_results;
    bool result = extractor.doCustomizedDoExtractSystemTablesWork(tables_retrieved_results);

    ASSERT_TRUE(result);

    size_t counter = 0;
    for (auto& p : tables_retrieved_results) {
        LOG(INFO) << " retrieved system.tables index: " << counter++ << "values: " << p.str();
    }
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
