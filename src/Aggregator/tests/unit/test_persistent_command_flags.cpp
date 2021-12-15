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
#include "common/file_command_flags.hpp"

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
const std::string COMMAND_FLAGS_DIR_ENV_VAR = "COMMAND_FLAGS_DIR";

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

static std::string checkCommandFlagsDir() {
    const char* env_p = std::getenv(COMMAND_FLAGS_DIR_ENV_VAR.c_str());
    if (env_p == nullptr) {
        LOG(ERROR) << "cannot find  COMMAND_FLAGS_DIR_ENV_VAR environment variable....exit test execution...";
        exit(-1);
    }

    std::string result(env_p);
    return result;
}

TEST(PersistentCommandFlagsTest, testCheckReadWriteCommandFlagsToFile) {
    std::string command_flags_dir = checkCommandFlagsDir();
    LOG(INFO) << "retrieved command flags from the test host environment: " << command_flags_dir;
    ASSERT_TRUE(command_flags_dir.length() > 0);

    std::string retrieved_command_path = PersistentCommandFlags::get_command_flags_dir();
    LOG(INFO) << "retrieved command flags from the Persistent Command utility: " << retrieved_command_path;
    ASSERT_TRUE(retrieved_command_path == command_flags_dir);

    bool exists =
        PersistentCommandFlags::check_command_flag_exists(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
    LOG(INFO) << " Command flags: " << PersistentCommandFlags::FreezeTrafficCommandFlagFileName
              << " exists or not: " << exists;
    ASSERT_FALSE(exists);

    if (!exists) {
        bool created =
            PersistentCommandFlags::create_command_flag(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
        ASSERT_TRUE(created);

        // then delete it immediately.
        bool deleted =
            PersistentCommandFlags::remove_command_flag(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
        ASSERT_TRUE(deleted);
        LOG(INFO) << " Command flags: " << PersistentCommandFlags::FreezeTrafficCommandFlagFileName
                  << " being deleted or not: " << deleted;

        bool does_not_exists =
            PersistentCommandFlags::check_command_flag_exists(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
        ASSERT_FALSE(does_not_exists);
        LOG(INFO) << " Command flags: " << PersistentCommandFlags::FreezeTrafficCommandFlagFileName
                  << " exists or not: " << does_not_exists;
    }
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
