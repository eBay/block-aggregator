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

#include "common/coord_http_client.hpp"
#include <Interpreters/Context.h>
#include <boost/asio.hpp>
#include <vector>
#include <memory>

namespace nuclm {

using namespace nucolumnar;
class AggregatorLoaderManager;

class ServerStatusInspector {
  public:
    using ServerStatus = nucolumnar::ServerStatus;
    ServerStatusInspector(boost::asio::io_context& ioc, const AggregatorLoaderManager& loader_manager_,
                          DB::ContextMutablePtr context_);

    ~ServerStatusInspector() = default;

    void start();
    void stop();

    ServerStatus reportStatus() const {
        ServerStatus status = server_status.load(std::memory_order_relaxed);
        return status;
    }

    bool syncStatus(std::string& err_msg, bool force = false);

    void reportMetrics(ServerStatus status);

  private:
    void setupAsyncWaitTimer();
    void inspectServer();
    bool doInspectionWork();

    const AggregatorLoaderManager& loader_manager;
    CoordHttpClient coord_client;
    DB::ContextMutablePtr context;
    std::atomic<ServerStatus> server_status;
    boost::asio::steady_timer timer;
    std::atomic<bool> running;
};

}; // namespace nuclm
