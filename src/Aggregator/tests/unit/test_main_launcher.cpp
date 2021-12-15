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

#include "monitor/metrics_collector.hpp"

#include <Common/Exception.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
#include <iostream>
#include <string>
#include <vector>

#include <csetjmp>
#include <csignal>
#include <cstdlib>

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

static void simulate_stop_aggregator_with_delay(int sleeptime_on_ms) {
    // need to use scope guard to exit the application.
    LOG(INFO) << "simulate application running ";
    std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
    nuclm::Application::instance().stop();
}

std::function<void()> s_graceful_app_shutdown;

TEST(AggregatorApplicationTesting, testMainLauncher) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // start the application simulation thread at the beginning.
    // make wait time long enough as many tables in the database that needs distributed locks to be initialized
    std::thread thread_obj(simulate_stop_aggregator_with_delay, 100000);

    try {
        SETTINGS_FACTORY.load(path);

        std::mutex m;
        std::condition_variable cv;

        bool stopping = false;
        LOG(INFO) << "Starting Aggregator Application";

        // Start application.
        if (!nuclm::Application::instance().start([&]() {
                {
                    std::unique_lock<std::mutex> lk(m);
                    stopping = true;
                }
                cv.notify_one();
            })) {
            LOG(ERROR) << "Failed to start Aggregator application!";
        } else {
            LOG(INFO) << "Aggregator Application is running";

            // Wait until application stops
            std::unique_lock<std::mutex> lk(m);
            cv.wait(lk, [&] { return stopping; });
        }

    } catch (...) {
        LOG(ERROR) << boost::current_exception_diagnostic_information();
    }

    // need to wait for the thread to exit, otherwise, we get abort signal.
    s_graceful_app_shutdown = [&thread_obj]() { thread_obj.join(); };

    LOG(INFO) << "Application is exiting";
    s_graceful_app_shutdown();
    LOG(INFO) << "Aggregator Application now exited....";
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
