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

class AggregatorLoaderDynamicTableRetrievalRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderDynamicTableRetrievalRelatedTest::shared_context = nullptr;

TEST_F(AggregatorLoaderDynamicTableRetrievalRelatedTest, testRetrieveAllTables) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderDynamicTableRetrievalRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderDynamicTableRetrievalRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        nuclm::AggregatorLoaderManager manager(context, ioc);
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();

        {
            DB::Block query_result;

            bool status = loader.getDefinedTables(query_result);
            ASSERT_TRUE(status);
            // the block header has the definition of:
            // name String String(size = 3), type String String(size = 3), default_type String String(size = 3),
            //       default_expression String String(size = 3), comment String String(size = 3),
            //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
            if (status) {
                std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

                std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                    std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
                // header definition for the table definition block:
                DB::Block sample_block;
                sm->loadSampleHeader(sample_block);
                const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
                int column_index = 0;

                for (auto& p : columns_with_type_and_name) {
                    LOG(INFO) << "column index: " << column_index++ << " column type: " << p.type->getName()
                              << " column name: " << p.name << " number of rows: " << p.column->size();
                }

                DB::MutableColumns columns = query_result.mutateColumns();
                // only one column, with type of string;
                auto& column_string_0 = assert_cast<DB::ColumnString&>(*columns[0]);

                size_t total_row_count = column_string_0.size();

                for (size_t i = 0; i < total_row_count; i++) {
                    std::string column_0 = column_string_0.getDataAt(i).toString();
                    LOG(INFO) << " row: " << i << ": " << column_0;
                }

                LOG(INFO) << " total number of rows retrieved:  " << total_row_count;

                ASSERT_TRUE(total_row_count > 0);
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
