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

#include "glog/logging.h"
#include "http_server/http_server.hpp"
#include "common/log_cleaner.hpp"
#include "common/logging.hpp"
#include <condition_variable>

namespace nuclm {
using HttpServer = nuclm::http_server::HttpServer;
using RequestHandler = nuclm::http_server::RequestHandler;

// forward declaration instead of including the header file - intentionally to minise inclusion of headers in this file
// which is included in lots of places
class NuColumnarRpcServer;

class Application {
  public:
    static Application& instance();
    /**
     * @brief Starts all application subsystems
     * @note This method does not block and returns quickly. The calling thread is expected to wait on
     * cv_notify_when_stopped when stop() completesl stop() should be called from an signal handler to  trigger a
     * graceful exit of the app
     * @param notify_when_stopped
     * @return true if start was successful
     * @return false if there was an error
     */
    virtual bool start(std::function<void()>&& notify_when_stopped) = 0;

    /**
     * @brief Stops the application
     * @note: does not block; notifies cv_notify_when_stopped
     */
    virtual void stop() = 0;

    boost::asio::io_context& io_context() { return m_ioc; }
    HttpServer& http_server() { return *DCHECK_NOTNULL(m_http_server.get()); }

  protected:
    Application();
    boost::asio::io_context m_ioc;
    HttpServer::ptr_t m_http_server;
    LogCleaner m_log_cleaner;
};

// Aliases for convenience
#define AppInstance Application::instance()
#define g_http_server AppInstance.http_server()

} // namespace nuclm
