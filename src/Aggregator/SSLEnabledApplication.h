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

#include <Poco/Util/Application.h>
#include <IO/UseSSL.h>
#include <string>
#include <memory>

namespace nuclm {

class SSLEnabledApplication : public Poco::Util::Application {
  public:
    SSLEnabledApplication(const std::string& loading_config_path_) :
            use_ssl{}, loading_config_path(loading_config_path_), app_initialized{false} {}

    ~SSLEnabledApplication() = default;

    int init();

    bool isInitialized() const { return app_initialized; }

  private:
    void initialize(Poco::Util::Application& self);
    std::unique_ptr<DB::UseSSL> use_ssl;
    std::string loading_config_path;
    bool app_initialized;
};
} // namespace nuclm
