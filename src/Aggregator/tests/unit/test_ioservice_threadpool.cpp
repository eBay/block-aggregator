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

#include "application.hpp"

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include "Aggregator/IoServiceBasedThreadPool.h"

#include <Common/Exception.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
#include <iostream>
#include <string>
#include <vector>

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

static void simulate_run_thread_with_delay(int sleeptime_on_ms) {
    // need to use scope guard to exit the application.
    LOG(INFO) << "simulate thread running with thread id: " << std::this_thread::get_id();
    std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
}

/**
 * number of the tasks should be much larger than the thread number, to test the re-use of the threads.
 */
class SimulatedKafkaConnector {
  public:
    SimulatedKafkaConnector(size_t number_of_threads_, size_t number_of_tasks_) :
            number_of_threads(number_of_threads_),
            number_of_tasks(number_of_tasks_),
            thread_pool(&io_service, number_of_threads) {
        LOG(INFO) << " SimualtedKafkaConnector starts with thread pool size: " << number_of_threads;
    }

    ~SimulatedKafkaConnector() {
        thread_pool.shutdown();
        LOG(INFO) << "SimulatedKafkaConnector destroyed";
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
        LOG(INFO) << "task counter is: " << counter.load(std::memory_order_relaxed)
                  << " at thread: " << std::this_thread::get_id();
        simulate_run_thread_with_delay(20);
    }

  private:
    boost::asio::io_service io_service;
    size_t number_of_threads;
    size_t number_of_tasks;
    std::atomic<int> counter{0};
    nuclm::IOServiceBasedThreadPool thread_pool;
};

TEST(IOServiceThread, testLaunchingOneThreadAndManyTasks) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;
    SimulatedKafkaConnector simulatedKafkaConnector(1, 10);
    simulatedKafkaConnector.start();
    // main thread wait until all of the tasks completed.
    simulate_run_thread_with_delay(10000);
}

TEST(IOServiceThread, testLaunchingTwoThreadsAndManyTasks) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;
    SimulatedKafkaConnector simulatedKafkaConnector(2, 20);
    simulatedKafkaConnector.start();
    // main thread wait until all of the tasks completed.
    simulate_run_thread_with_delay(10000);
}

// Call RUN_ALL_TESTS() in main()
// invoke:  ./test_main_launcher --config_file  <config_file>
int main(int argc, char** argv) {
    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
