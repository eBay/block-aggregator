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

#include "common/file_command_flags.hpp"
#include "common/utils.hpp"
#include "common/logging.hpp"
#include <boost/filesystem.hpp>

#include <stdio.h>
#include <iostream>

namespace filesystem = boost::filesystem;

// The command file name located under command-flags-dir (from the environment variable)
const std::string PersistentCommandFlags::FreezeTrafficCommandFlagFileName = "freeze_kafka_connector_command";

std::string PersistentCommandFlags::get_command_flags_dir() noexcept {
    const char* value = std::getenv("COMMAND_FLAGS_DIR");
    return std::string(value ? value : get_cwd());
}

std::string PersistentCommandFlags::create_command_flags_dir() noexcept {
    std::string command_dir = get_command_flags_dir();
    filesystem::path command_dir_path{command_dir};
    if (!filesystem::exists(command_dir_path) && !filesystem::create_directories(command_dir_path)) {
        std::cerr << "Unable to create directory: " << command_dir_path << std::endl;
        std::exit(-1);
    }

    return command_dir;
}

// check the command file name under the command flags directory that has been set
bool PersistentCommandFlags::check_command_flag_exists(const std::string& cmd_file_name) {
    std::string command_dir = get_command_flags_dir();
    filesystem::path command_dir_path{command_dir};
    filesystem::path command_file_path = command_dir_path / cmd_file_name;
    bool exists = filesystem::exists(command_file_path);
    return exists;
}

// create the command file name under the command flags directory that has been set
bool PersistentCommandFlags::create_command_flag(const std::string& cmd_file_name) {
    std::string command_dir = get_command_flags_dir();
    filesystem::path command_dir_path{command_dir};
    filesystem::path command_file_path = command_dir_path / cmd_file_name;

    std::ofstream f(command_file_path.BOOST_FILESYSTEM_C_STR);
    if (!f) {
        return false;
    } else {
        std::string contents; // empty content
        f << contents;
        f.flush();
        f.close();
        return true;
    }
}

// remove the command file name under the command flags directory that has been set
bool PersistentCommandFlags::remove_command_flag(const std::string& cmd_file_name) {
    std::string command_dir = get_command_flags_dir();
    filesystem::path command_dir_path{command_dir};
    filesystem::path command_file_path = command_dir_path / cmd_file_name;

    bool result = filesystem::remove(command_file_path);
    return result;
}
