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

#include "log_cleaner.hpp"
#include "settings_factory.hpp"
#include "utils.hpp"

void LogCleaner::start() {
    if (!running_.load()) {
        LOG(INFO) << "Log cleaner is stopped";
        return;
    }
    uint32_t wait = SETTINGS_PARAM(config->logCleaner->scanIntervalSecs);
    LOG_ADMIN(4) << "Scheduling clean log timer in " << wait << " secs"
                 << " on log directory: " << FLAGS_log_dir;
    timer_.expires_after(boost::asio::chrono::seconds(wait));
    timer_.async_wait(std::bind(&LogCleaner::clean_logs, this));
}

void LogCleaner::stop() { running_ = false; }

void LogCleaner::clean_logs() {
    if (!running_.load()) {
        LOG(INFO) << "Log cleaner is stopped";
        return;
    }
    const std::string target_path(FLAGS_log_dir);

    /* Retain only most recent N files for each of these filter types */
    std::vector<const char*> filters = {"NuColumnarAggregator\\.log\\.INFO.*",
                                        "NuColumnarAggregator\\.log\\.WARNING.*",
                                        "NuColumnarAggregator\\.log\\.ERROR.*",
                                        "NuColumnarAggregator\\.log\\.FATAL.*",
                                        "dump_.*log" /* crash-dumps/dump_YYYY-MM-DDTHH:MM:SS.log*/,
                                        "valgrind_.*"};

    uint32_t max_count = SETTINGS_PARAM(config->logCleaner->maxFileCountPerType);
    LOG_ADMIN(4) << "Log file purging config maxFileCountPerType: " << max_count;

    for (auto& filter : filters) {
        /* Recursively goes through all the sub-directories under target_path */
        keep_recent_n_files(target_path, filter, max_count);
    }

    start();
}
