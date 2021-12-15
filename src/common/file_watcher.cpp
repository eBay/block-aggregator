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

#include "file_watcher.hpp"
#include "common/logging.hpp"

#include <boost/algorithm/string.hpp>

FileWatcher::FileWatcher(const std::vector<std::string>& paths, FileWatcher::listener_t&& updater) :
        paths{paths},
        listener{std::move(updater)},
        monitor{fsw::monitor_factory::create_monitor(fsw_monitor_type::system_default_monitor_type, paths,
                                                     process_events, this)} {
    if (listener == nullptr) {
        LOG(FATAL) << "FileWatcher listener should not be null";
    }
}

FileWatcher::~FileWatcher() {
    if (is_running()) {
        stop();
    }
}

void FileWatcher::start() {
    LOG(INFO) << "File watcher start on [" << boost::algorithm::join(paths, ", ") << "]";
    if (daemon != nullptr) {
        stop();
    }
    daemon = std::make_unique<boost::thread>([this]() {
#ifdef __APPLE__
        pthread_setname_np("FileWatcher");
#else
        pthread_setname_np(pthread_self(), "FileWatcher");
#endif /* __APPLE__ */
        LOG(INFO) << "File watcher thread started";
        monitor->start();
        LOG(INFO) << "File watcher thread exited";
    });
}

void FileWatcher::stop() {
    try {
        LOG(INFO) << "File watcher stopping";
        monitor->stop();
        if (daemon != nullptr && daemon->joinable()) {
            daemon->join();
        }
        LOG(INFO) << "File watcher stopped";
    } catch (...) {
        LOG(FATAL) << "Failed to stop file watcher due to: " << boost::current_exception_diagnostic_information();
    }
}

bool FileWatcher::is_running() { return monitor->is_running(); }

void FileWatcher::process_events(const std::vector<fsw::event>& events, void* context) {
    auto* self = reinterpret_cast<FileWatcher*>(context);
    if (self->listener) {
        self->listener(events);
    }
}
