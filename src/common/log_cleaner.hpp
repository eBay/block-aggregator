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

#ifndef NUCOLUMNARSERVICE_LOG_CLEANER_H
#define NUCOLUMNARSERVICE_LOG_CLEANER_H

/**
 * periodically check log files and purge them if necessary
 */
#include <boost/asio.hpp>
namespace asio = boost::asio;
using asio::steady_timer;

class LogCleaner {
  public:
    explicit LogCleaner(asio::io_context& ioc) : timer_(ioc), running_(true){};
    ~LogCleaner() = default;

    void start();

    void stop();

  private:
    void clean_logs();
    steady_timer timer_;
    std::atomic<bool> running_;
};

#endif // NUCOLUMNARSERVICE_LOG_CLEANER_H
