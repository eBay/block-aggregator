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

#ifndef NUCOLUMNARAGGR_COORD_HTTP_CLIENT_H
#define NUCOLUMNARAGGR_COORD_HTTP_CLIENT_H
#include <boost/system/error_code.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

/**
 * HTTP sync wrapper
 *
 * FIXME: Use shared http_client.
 */
namespace nucolumnar {

namespace beast = boost::beast; // from <boost/beast.hpp>
namespace http = beast::http;   // from <boost/beast/http.hpp>

enum class ServerStatus {
    DOWN = 0,   // database server is at the down state
    UP = 1,     // database server is at up state
    UNKNOWN = 2 // unknown state
};

static std::string& toServerStatusName(ServerStatus status) {
    static std::string down("Down"), up("Up"), unknown("Unknown");
    switch (status) {
    case ServerStatus::DOWN:
        return down;
    case ServerStatus::UP:
        return up;
    case ServerStatus::UNKNOWN:
        return unknown;
    default:
        return unknown;
    }
}

class CoordHttpClient {
  public:
    /**
     * @param ioc
     * @param coord_host
     * @param coord_port
     * @param timeout_ms
     */
    CoordHttpClient(boost::asio::io_context& ioc, long timeout_ms = 10000);

    /**
     *  POST /api/v1/replica/:replica/database
     *  Payload: {"apiVersion":"v1","data":{"state":"UP","timestamp":1590485470560},"kind":"Replica"}
     * @param replica_id
     * @param state
     */
    bool report_db_status(const std::string& replica_id, const ServerStatus state, std::string& err_msg);

    bool report_db_status(const ServerStatus state, std::string& err_msg);

    void execute_request(http::request<http::string_body>& req, http::response<http::dynamic_body>& res,
                         beast::error_code& error);

    /**
     * Expose for test purpose only
     * @param host
     * @param port
     * @param secret_signature
     * @param req
     * @param res
     * @param error
     */
    void execute_request(const std::string& host, int port, http::request<http::string_body>& req,
                         http::response<http::dynamic_body>& res, beast::error_code& error);

  private:
    boost::asio::io_context& ioc_;
    long timeout_ms_;
    boost::asio::ssl::context ssl_ctx_;
};
} // namespace nucolumnar

#endif // NUCOLUMNARAGGR_COORD_HTTP_CLIENT_H
