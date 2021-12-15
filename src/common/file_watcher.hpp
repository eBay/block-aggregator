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

#include <boost/thread.hpp>
#include <libfswatch/c++/monitor_factory.hpp>
#include <functional>
#include <memory>

class FileWatcher {
  public:
    using listener_t = std::function<void(const std::vector<fsw::event>&)>;

    FileWatcher(const std::vector<std::string>& paths, listener_t&& updater);

    virtual ~FileWatcher();

    void start();

    void stop();

    bool is_running();

  private:
    static void process_events(const std::vector<fsw::event>& events, void* context);

    std::vector<std::string> paths;
    listener_t listener;
    std::unique_ptr<fsw::monitor> monitor;
    std::unique_ptr<boost::thread> daemon;
};
