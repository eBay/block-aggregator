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
#include <boost/asio/io_context.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>
#include <nlohmann/json.hpp>
#include <Poco/URI.h>
namespace nuclm::http_server {

namespace http = boost::beast::http;

using request_t = http::request<http::string_body>;
using response_t = http::response<http::string_body>;
using completion_t = std::function<void(response_t&&)>;

// struct URL {
//     URL(const std::string& url_s); // omitted copy, ==, accessors, ...
//   private:
//     void parse(const std::string& url_s);
//     std::string protocol_, host_, path_, query_;
// };

/**
 * @brief signature for http handlers
 * @param grpc_uri - contains parsed uri string at the time of invocation
 * @request_t - movable request to keep inside of handler for the duration of processing
 * @completion_t - also movable callable object which needs to be kept for the duration of request processing; once the
 * handler is ready to respond, it must create an instance of response_t, populate it, and invoke
 * completion_t(response_t)
 * @see \ref HttpServer::send_json(...) and \ref HttpServer::send_string(..) utility function that constructs response
 * object itself and invokes your completion handlerwith it
 */
using request_cb_t = std::function<void(const Poco::URI&, request_t&&, completion_t&&)>;

struct RequestHandler {
    RequestHandler() {}
    RequestHandler(request_cb_t cb_, int32_t log_level_) : cb(cb_), log_level(log_level_) {}
    RequestHandler(RequestHandler& handler) : cb(handler.cb), log_level(handler.log_level) {}

    request_cb_t cb;
    int32_t log_level;
};

class HttpServer {
  public:
    using status = boost::beast::http::status;
    using ptr_t = std::unique_ptr<HttpServer>;
    static ptr_t create(boost::asio::io_context& ioc);

    virtual ~HttpServer() = default;
    virtual HttpServer& register_handler(std::string path, RequestHandler handler) = 0;

    virtual void stop() = 0;

    // helper function to simplify sending typical successful json responses
    static void send_json(const nlohmann::json& body, completion_t& cont, const request_t& req,
                          http::status status = http::status::ok) {
        send_string(body.dump(4), json_content_type, cont, req, status);
    }

    static void send_json(std::string&& str_body, completion_t& cont, const request_t& req) {
        send_string(std::move(str_body), json_content_type, cont, req);
    }

    static void send_xml(std::string&& str_body, completion_t& cont, const request_t& req) {
        send_string(std::move(str_body), xml_content_type, cont, req);
    }

    // helper function to simplify sending typical successful string responses
    static void send_string(std::string&& body, const std::string& content_type, completion_t& cont,
                            const request_t& req, http::status status_v = http::status::ok);

    static inline std::string json_content_type = "application/json";
    static inline std::string xml_content_type = "application/xml";
    static inline std::string text_content_type = "text/plain";
};

} // namespace nuclm::http_server
