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

#pragma once

#include <string>

class PersistentCommandFlags {
  public:
    static const std::string FreezeTrafficCommandFlagFileName;

  public:
    static std::string get_command_flags_dir() noexcept;
    static std::string create_command_flags_dir() noexcept;

    static bool check_command_flag_exists(const std::string& cmd_file_name);
    static bool create_command_flag(const std::string& cmd_file_name);
    static bool remove_command_flag(const std::string& cmd_file_name);
};
