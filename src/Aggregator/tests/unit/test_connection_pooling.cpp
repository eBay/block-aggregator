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
#include "Aggregator/IoServiceBasedThreadPool.h"

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

class AggregatorLoaderManagerConnectionPoolingRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderManagerConnectionPoolingRelatedTest::shared_context = nullptr;

/**
 * number of the tasks should be much larger than the thread number, to test the re-use of the threads.
 */
class SimulatedConnectionConsumers {
  public:
    SimulatedConnectionConsumers(size_t number_of_threads_, size_t number_of_tasks_,
                                 nuclm::AggregatorLoaderManager& loader_manager_) :
            number_of_threads(number_of_threads_),
            number_of_tasks(number_of_tasks_),
            thread_pool(&io_service, number_of_threads),
            loader_manager(loader_manager_) {
        LOG(INFO) << " SimualtedConnectionConsumers starts with thread pool size: " << number_of_threads;
    }

    ~SimulatedConnectionConsumers() {
        thread_pool.shutdown();
        LOG(INFO) << "SimulatedConnectionConsumers destroyed";
    }

    void start() {
        thread_pool.init();
        LOG(INFO) << " number of the tasks put to the queue is: " << number_of_tasks;
        for (size_t task_number = 0; task_number < number_of_tasks; task_number++) {
            thread_pool.enqueue([this] { compute(); });
        }
    }

    void compute() {
        counter++;
        try {
            std::shared_ptr<nuclm::LoaderConnectionPool> connection_pool = loader_manager.getConnectionPool();
            const nuclm::DatabaseConnectionParameters& database_parameters = loader_manager.getConnectionParameters();

            // NOTE: the following line cause the exception in terms of the setting of the parameters.
            // Need to figure out the correct way.
            // context.setSetting("connection_pool_max_wait_ms", 100000);
            // context.getSettingsRef();

            // scope for the entry
            {
                std::string server_name;
                std::string server_display_name;

                UInt64 server_revision = 0;
                UInt64 server_version_major = 0;
                UInt64 server_version_minor = 0;
                UInt64 server_version_patch = 0;

                // force connect for the retrieved pooled entry.
                LOG(INFO) << " waiting to get the connection pool entry with task counter: " << counter
                          << " at thread: " << std::this_thread::get_id();
                DB::ConnectionPool::Entry connection_pool_entry =
                    connection_pool->get(database_parameters.timeouts, nullptr, true);
                LOG(INFO) << " finish waiting to get the connection pool entry with task counter: " << counter
                          << " at thread: " << std::this_thread::get_id();

                connection_pool_entry->getServerVersion(database_parameters.timeouts, server_name, server_version_major,
                                                        server_version_minor, server_version_patch, server_revision);

                std::string server_version = DB::toString(server_version_major) + "." +
                    DB::toString(server_version_minor) + "." + DB::toString(server_version_patch);

                if (server_display_name = connection_pool_entry->getServerDisplayName(database_parameters.timeouts);
                    server_display_name.length() == 0) {
                    server_display_name = database_parameters.host;
                }

                // server display name is the server name, for example,  "graphdb-1-2454883.lvs02.dev.ebayc3.com".
                LOG(INFO) << "Connected to " << server_name << " belonging to server: " << server_display_name
                          << " server version " << server_version << " revision " << server_revision << "."
                          << " in thread: " << std::this_thread::get_id();
            }
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            auto code = DB::getCurrentExceptionCode();

            LOG(ERROR) << "with exception return code: " << code;
        }
    }

  private:
    boost::asio::io_service io_service;
    size_t number_of_threads;
    size_t number_of_tasks;
    std::atomic<int> counter{0};
    nuclm::IOServiceBasedThreadPool thread_pool;
    nuclm::AggregatorLoaderManager& loader_manager;
};

static void simulate_run_thread_with_delay(int sleeptime_on_ms) {
    // need to use scope guard to exit the application.
    LOG(INFO) << "simulate thread running with thread id: " << std::this_thread::get_id();
    std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
}

/**
 * This test is to test out that connection pool entry's lifetime is scope-based. If the retrieved
 * pool entries gets held by the vector. Then at the end, all of the pooled connections get exhausted.
 * And the program will be in the forever waiting state.
 */
TEST_F(AggregatorLoaderManagerConnectionPoolingRelatedTest, testConsumeAllConnectionPoolEntriesInSequentialMode) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context =
            AggregatorLoaderManagerConnectionPoolingRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc =
            AggregatorLoaderManagerConnectionPoolingRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        auto max_number_of_pool_entries = with_settings([this](SETTINGS s) {
            auto& var = s.config.databaseServer;
            return var.number_of_pooled_connections;
        });

        nuclm::AggregatorLoaderManager manager(context, ioc);
        std::shared_ptr<nuclm::LoaderConnectionPool> connection_pool = manager.getConnectionPool();
        const nuclm::DatabaseConnectionParameters& database_parameters = manager.getConnectionParameters();

        // NOTE: the following line cause the exception in terms of the setting of the parameters.
        // Need to figure out the correct way.
        // context.setSetting("connection_pool_max_wait_ms", 100000);
        // context.getSettingsRef();

        // scope for the entry
        std::vector<DB::ConnectionPool::Entry> collected_entries;
        LOG(INFO) << " configuration setting for max number of the pooled connections is: "
                  << max_number_of_pool_entries;
        for (size_t i = 0; i < max_number_of_pool_entries; i++) {
            std::string server_name;
            std::string server_display_name;

            UInt64 server_revision = 0;
            UInt64 server_version_major = 0;
            UInt64 server_version_minor = 0;
            UInt64 server_version_patch = 0;

            // force connect for the retrieved pooled entry.
            LOG(INFO) << " waiting to get the connection pool entry with index: " << i;
            DB::ConnectionPool::Entry connection_pool_entry =
                connection_pool->get(database_parameters.timeouts, nullptr, true);
            LOG(INFO) << " finish waiting to get the connection pool entry with index: " << i;

            // collected to the query.
            collected_entries.push_back(connection_pool_entry);

            connection_pool_entry->getServerVersion(database_parameters.timeouts, server_name, server_version_major,
                                                    server_version_minor, server_version_patch, server_revision);

            std::string server_version = DB::toString(server_version_major) + "." + DB::toString(server_version_minor) +
                "." + DB::toString(server_version_patch);

            if (server_display_name = connection_pool_entry->getServerDisplayName(database_parameters.timeouts);
                server_display_name.length() == 0) {
                server_display_name = database_parameters.host;
            }

            // server display name is the server name, for example,  "graphdb-1-2454883.lvs02.dev.ebayc3.com".
            LOG(INFO) << "Connected to " << server_name << " belonging to server: " << server_display_name
                      << " server version " << server_version << " revision " << server_revision << "."
                      << " in thread: " << std::this_thread::get_id();
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
 * To have 2x number of the thread launched, and each one uses the connection to connect to the server.
 */
TEST_F(AggregatorLoaderManagerConnectionPoolingRelatedTest, testConsumePoolEntriesInConcurrentMode) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context =
            AggregatorLoaderManagerConnectionPoolingRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc =
            AggregatorLoaderManagerConnectionPoolingRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        auto max_number_of_pool_entries = with_settings([this](SETTINGS s) {
            auto& var = s.config.databaseServer;
            return var.number_of_pooled_connections;
        });

        nuclm::AggregatorLoaderManager manager(context, ioc);
        size_t number_of_worker_threads = max_number_of_pool_entries + 1;
        size_t total_number_of_tasks = max_number_of_pool_entries * 10;
        SimulatedConnectionConsumers simulated_connection_consumer(number_of_worker_threads, total_number_of_tasks,
                                                                   manager);
        simulated_connection_consumer.start();
        // main thread wait until all of the tasks completed.
        simulate_run_thread_with_delay(10000);
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
