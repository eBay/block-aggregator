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

#include <Aggregator/SSLEnabledApplication.h>
#include <Common/Config/configReadClient.h>
#include <Common/Exception.h>

#include <boost/filesystem.hpp>

#include <glog/logging.h>

namespace nuclm {

int SSLEnabledApplication::init() {
    initialize(*this);
    return 0;
}

void SSLEnabledApplication::initialize(Poco::Util::Application& self) {
    Poco::Util::Application::initialize(self);
    // this is home_path + "/.clickhouse-client/config.xml", which includes the ssl related setting.
    boost::filesystem::path p(loading_config_path);
    if (boost::filesystem::is_directory(p)) {
        LOG(INFO) << "loading config path for SSL application setting is: " << loading_config_path;
        bool result = DB::configReadClient(config(), loading_config_path);
        if (result) {
            // then initialize the SSL configuration setting;
            LOG(INFO) << "ssl configuration setting was loaded successfully";
            try {
                use_ssl = std::make_unique<DB::UseSSL>();
                app_initialized = true;
            } catch (...) {
                LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                auto code = DB::getCurrentExceptionCode();
                LOG(ERROR) << "ssl setting initialization finally failed with with exception return code: " << code;
            }
        } else {
            LOG(ERROR) << "ssl configuration setting does not get loaded successfully";
        }
    } else {
        LOG(ERROR) << "loading config path: " << loading_config_path
                   << " for SSL application setting is not set as directory";
    }
}

} // namespace nuclm
