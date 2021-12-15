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

#include <Common/Exception.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
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

TEST(ConfigurationLoadingTesting, testDefaultConstantInConfigurationFile) {
    std::string path = getConfigFilePath("example_aggregator_config_no_aggregatorloader.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    try {
        SETTINGS_FACTORY.load(path);
        with_settings([](SETTINGS s) {
            auto& aggregator_loader = s.config.aggregatorLoader;
            ASSERT_EQ(aggregator_loader.max_allowed_block_size_in_bytes, (size_t)10485760);
            ASSERT_EQ(aggregator_loader.max_allowed_block_size_in_rows, (size_t)100000);
            ASSERT_EQ(aggregator_loader.sleep_time_for_retry_initial_connection_ms, (size_t)1000);
            ASSERT_EQ(aggregator_loader.number_of_initial_connection_retries, (size_t)600);
            ASSERT_EQ(aggregator_loader.sleep_time_for_retry_table_definition_retrieval_ms, (size_t)50);
            ASSERT_EQ(aggregator_loader.number_of_table_definition_retrieval_retries, (size_t)120);
        });

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
    }
}

/**
 * This particular configuration has the aggregator constants to be changed.
 */
TEST(ConfigurationLoadingTesting, loadingOfConfigurationFile) {
    std::string path = getConfigFilePath("example_aggregator_config_altered_aggregatorloader.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    try {
        SETTINGS_FACTORY.load(path);
        LOG(INFO) << "Loaded config " << path;

        with_settings([](SETTINGS s) {
            auto& databases = s.config.schema.databases;
            int databaseIndex = 0;
            for (const auto& database : databases) {
                ASSERT_EQ(database.databaseName, "default");

                if (databaseIndex == 0) {
                    auto& tables = database.tables;
                    databaseIndex++;

                    int tableIndex = 0;
                    for (const auto& table : tables) {
                        if (tableIndex == 0) {

                            ASSERT_EQ(table.tableName, "ontime");

                            ASSERT_EQ(table.columns[0].type, "UInt16");
                            ASSERT_EQ(table.columns[0].name, "year");

                            ASSERT_EQ(table.columns[1].type, "UInt8");
                            ASSERT_EQ(table.columns[1].name, "quarter");

                            ASSERT_EQ(table.columns[2].type, "UInt8");
                            ASSERT_EQ(table.columns[2].name, "month");

                            ASSERT_EQ(table.columns[3].type, "UInt8");
                            ASSERT_EQ(table.columns[3].name, "dayOfMonth");

                            ASSERT_EQ(table.columns[4].type, "UInt8");
                            ASSERT_EQ(table.columns[4].name, "dayOfWeek");

                            ASSERT_EQ(table.columns[5].type, "Date");
                            ASSERT_EQ(table.columns[5].name, "flightDate");
                        }

                        tableIndex++;
                    }
                }
            }
        });
        LOG(INFO) << "Checked database schema";

        with_settings([](SETTINGS s) {
            auto& aggregator_loader = s.config.aggregatorLoader;
            ASSERT_EQ(aggregator_loader.max_allowed_block_size_in_bytes, (size_t)512000);
            ASSERT_EQ(aggregator_loader.max_allowed_block_size_in_rows, (size_t)300000);
            ASSERT_EQ(aggregator_loader.sleep_time_for_retry_initial_connection_ms, (size_t)5000);
            ASSERT_EQ(aggregator_loader.number_of_initial_connection_retries, (size_t)500);
            ASSERT_EQ(aggregator_loader.sleep_time_for_retry_table_definition_retrieval_ms, (size_t)50);
            ASSERT_EQ(aggregator_loader.number_of_table_definition_retrieval_retries, (size_t)30);
        });

        LOG(INFO) << "Checked config value";

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
        FAIL();
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
