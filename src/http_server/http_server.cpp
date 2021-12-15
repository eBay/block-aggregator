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

#include <string_view>
#include <initializer_list>

#include "common/urcu_helper.hpp"
#include "common/settings_factory.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core/bind_handler.hpp>
#include <boost/beast/version.hpp>
namespace beast = boost::beast; // from <boost/beast.hpp>

// #include <boost/asio/bind_executor.hpp>
// #include <boost/asio/dispatch.hpp>
#include <boost/asio/defer.hpp>
#include <boost/config.hpp>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <nlohmann/json.hpp>
#include <cctype>

#include "http_server.hpp"
#include "common/logging.hpp"
#include <Poco/Exception.h>
#include <mutex>

namespace beast = boost::beast;   // from <boost/beast.hpp>
namespace http = beast::http;     // from <boost/beast/http.hpp>
namespace net = boost::asio;      // from <boost/asio.hpp>
namespace ssl = boost::asio::ssl; // from <boost/asio/ssl.hpp>
using tcp = boost::asio::ip::tcp; // from <boost/asio/ip/tcp.hpp>

namespace nuclm::http_server {

void HttpServer::send_string(std::string&& str_body, const std::string& content_type, completion_t& cont,
                             const request_t& req, http::status status_v) {
    http::response<http::string_body> res{status_v, 11 /*http version 1.1*/};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, content_type);

    res.content_length(str_body.size());
    res.body() = std::move(str_body);
    res.keep_alive(req.keep_alive());
    return cont(std::move(res));
}

static std::string path_cat(beast::string_view base, beast::string_view path) {
    if (base.empty())
        return path.to_string();
    std::string result = base.to_string();
    char constexpr path_separator = '/';
    if (result.back() == path_separator)
        result.resize(result.size() - 1);
    result.append(path.data(), path.size());
    return result;
}

using handler_map_t = std::unordered_map<std::string, RequestHandler>;
//------------------------------------------------------------------------------

// Report a failure
void fail(beast::error_code ec, char const* what) {
    // ssl::error::stream_truncated, also known as an SSL "short read",
    // indicates the peer closed the connection without performing the
    // required closing handshake (for example, Google does this to
    // improve performance). Generally this can be a security issue,
    // but if your communication protocol is self-terminated (as
    // it is with both HTTP and WebSocket) then you may simply
    // ignore the lack of close_notify.
    //
    // https://github.com/boostorg/beast/issues/38
    //
    // https://security.stackexchange.com/questions/91435/how-to-handle-a-malicious-ssl-tls-shutdown
    //
    // When a short read would cut off the end of an HTTP message,
    // Beast returns the error beast::http::error::partial_message.
    // Therefore, if we see a short read here, it has occurred
    // after the message has been completed, so it is safe to ignore it.

    if (ec == net::ssl::error::stream_truncated)
        return;

    LOG(ERROR) << what << ": " << ec.message() << "\n";
}

// Handles an HTTP server connection
class session : public std::enable_shared_from_this<session> {
    beast::ssl_stream<beast::tcp_stream> stream_;
    beast::flat_buffer buffer_;
    std::shared_ptr<handler_map_t> handlers_;
    http::request<http::string_body> req_;
    std::shared_ptr<void> res_;

  public:
    // Take ownership of the socket
    explicit session(tcp::socket&& socket, ssl::context& ctx, std::shared_ptr<handler_map_t> handlers) :
            stream_(std::move(socket), ctx), handlers_(handlers) {}

    // Start the asynchronous operation
    void run() {
        // Set the timeout.
        beast::get_lowest_layer(stream_).expires_after(
            std::chrono::seconds(SETTINGS_PARAM(config->httpServer->timeoutSecs)));

        // Perform the SSL handshake
        stream_.async_handshake(ssl::stream_base::server,
                                beast::bind_front_handler(&session::on_handshake, shared_from_this()));
    }

    void on_req_fail(beast::error_code ec, char const* what) {
        if (ec == net::ssl::error::stream_truncated)
            return;
        LOG(ERROR) << "[" << this << " " << req_.method_string() << " " << req_.target() << "] failed at " << what
                   << ": " << ec.message() << "\n";
    }

    void on_conn_fail(beast::error_code ec, char const* what) {
        if (ec == net::ssl::error::stream_truncated)
            return;
        LOG(ERROR) << "[" << this << "] failed at " << what << ": " << ec.message() << "\n";
    }

    void on_handshake(beast::error_code ec) {
        if (ec)
            return on_conn_fail(ec, "handshake");

        do_read();
    }

    void do_read() {
        // Make the request empty before reading,
        // otherwise the operation behavior is undefined.
        req_ = {};

        // Set the timeout.
        beast::get_lowest_layer(stream_).expires_after(
            std::chrono::seconds(SETTINGS_PARAM(config->httpServer->timeoutSecs)));

        // Read a request
        http::async_read(stream_, buffer_, req_, beast::bind_front_handler(&session::on_read, shared_from_this()));
    }

    void on_read(beast::error_code ec, std::size_t bytes_transferred) {
        boost::ignore_unused(bytes_transferred);

        // This means they closed the connection
        if (ec == http::error::end_of_stream)
            return do_close();

        // Close after no more new requests
        if (ec == beast::error::timeout) {
            CVLOG(VMODULE_HTTP_SERVER, 3) << "[" << this << "] close long hanging session";
            return do_close();
        }

        if (ec)
            return on_req_fail(ec, "read");

        // Send the response
        on_request([session_ = std::weak_ptr<session>(shared_from_this())](response_t&& msg) {
            if (auto self = session_.lock()) {

                session& s = *self;
                // The lifetime of the message has to extend
                // for the duration of the async operation so
                // we use a shared_ptr to manage it.
                auto sp = std::make_shared<response_t>(std::move(msg));

                // Store a type-erased version of the shared
                // pointer in the class to keep it alive.
                s.res_ = sp;

                // Write the response
                http::async_write(s.stream_, *sp,
                                  beast::bind_front_handler(&session::on_write, s.shared_from_this(), sp->need_eof()));
            };
        });
    }

    void on_request(completion_t&& send) {
        // Returns a bad request response
        auto const bad_request = [this](beast::string_view why) {
            http::response<http::string_body> res{http::status::bad_request, req_.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "text/html");
            res.keep_alive(req_.keep_alive());
            res.body() = why.to_string();
            res.prepare_payload();
            LOG(WARNING) << "[" << this << " " << req_.method_string() << " " << req_.target()
                         << "] bad request: " << why;
            return res;
        };

        // Returns a not found response
        auto const not_found = [this](beast::string_view target) {
            http::response<http::string_body> res{http::status::not_found, req_.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "text/html");
            res.keep_alive(req_.keep_alive());
            res.body() = "The resource '" + target.to_string() + "' was not found.";
            res.prepare_payload();
            LOG(WARNING) << "[" << this << " " << req_.method_string() << " " << req_.target() << "] not found";
            return res;
        };

        // Returns a server error response
        [[maybe_unused]] auto const server_error = [this](beast::string_view what) {
            http::response<http::string_body> res{http::status::internal_server_error, req_.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "text/html");
            res.keep_alive(req_.keep_alive());
            res.body() = "An error occurred: '" + what.to_string() + "'";
            res.prepare_payload();
            LOG(ERROR) << "[" << this << " " << req_.method_string() << " " << req_.target()
                       << "] internal server error: " << what.to_string();
            return res;
        };

        // Make sure we can handle the method
        if (req_.method() != http::verb::get && req_.method() != http::verb::post)
            return send(bad_request("Only get|post method is allowed"));

        // Request path must be absolute and not contain "..".
        if (req_.target().empty() || req_.target()[0] != '/' || req_.target().find("..") != beast::string_view::npos)
            return send(bad_request("Illegal request-target"));

        std::string u = std::string("http://localhost") + std::string(req_.target());
        try {
            Poco::URI uri(u);
            // std::string path(uri->path);
            if (auto it = handlers_->find(uri.getPath()); it != handlers_->end()) {
                // Respond to GET request
                std::stringstream info;
                info << "[" << this << " " << req_.method_string() << " " << req_.target() << "]";
                CVLOG(VMODULE_HTTP_SERVER, it->second.log_level) << info.str() << " process start";
                it->second.cb(uri, std::move(req_), std::move(send));
                CVLOG(VMODULE_HTTP_SERVER, it->second.log_level) << info.str() << " process finish";
            } else {
                return send(not_found(req_.target()));
            }
        } catch (Poco::SyntaxException& e) {
            return send(bad_request("invalid uri: " + e.message()));
        }
    }

    void on_write(bool close, beast::error_code ec, std::size_t bytes_transferred) {
        boost::ignore_unused(bytes_transferred);

        if (ec)
            return on_req_fail(ec, "write");

        if (close) {
            // This means we should close the connection, usually because
            // the response indicated the "Connection: close" semantic.
            return do_close();
        }

        // We're done with the response so delete it
        res_ = nullptr;

        // Read another request
        do_read();
    }

    void do_close() {
        // Set the timeout.
        beast::get_lowest_layer(stream_).expires_after(std::chrono::seconds(30));

        // Perform the SSL shutdown
        stream_.async_shutdown(beast::bind_front_handler(&session::on_shutdown, shared_from_this()));
    }

    void on_shutdown(beast::error_code ec) {
        if (ec == std::errc::bad_file_descriptor) {
            CVLOG(VMODULE_HTTP_SERVER, 3) << "[" << this << "] conn already closed";
        } else if (ec) {
            on_conn_fail(ec, "shutdown");
        }
        // At this point the connection is closed gracefully
    }
};

//------------------------------------------------------------------------------

// Accepts incoming connections and launches the sessions
class listener : public std::enable_shared_from_this<listener> {
    net::io_context& ioc_;
    ssl::context ctx_;
    tcp::acceptor acceptor_{net::make_strand(ioc_)};
    std::shared_ptr<handler_map_t> handlers_;

  public:
    listener(net::io_context& ioc, ssl::context&& ctx, tcp::endpoint endpoint,
             std::shared_ptr<handler_map_t> handlers) :
            ioc_{ioc}, ctx_(std::move(ctx)), acceptor_(ioc), handlers_{handlers} {

        beast::error_code ec;

        // Open the acceptor
        acceptor_.open(endpoint.protocol(), ec);
        if (ec) {
            fail(ec, "open");
            return;
        }

        // Allow address reuse
        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        if (ec) {
            fail(ec, "set_option");
            return;
        }

        // Bind to the server address
        acceptor_.bind(endpoint, ec);
        if (ec) {
            fail(ec, "bind");
            return;
        }

        // Start listening for connections
        acceptor_.listen(net::socket_base::max_listen_connections, ec);
        if (ec) {
            fail(ec, "listen");
            return;
        }
    }

    // Start accepting incoming connections
    void run() {
        if (!acceptor_.is_open()) {
            CVLOG(VMODULE_HTTP_SERVER, 1)
                << "http server acceptor socket is not open - exiting run() right away; endpoint="
                << acceptor_.local_endpoint();
            return;
        }

        std::cout << "HTTP Server is listening on " << acceptor_.local_endpoint() << std::endl;

        do_accept();
    }

    void stop(boost::system::error_code ec, char const* what) {
        if (acceptor_.is_open()) {
            CVLOG(VMODULE_HTTP_SERVER, 1) << "closing acceptor on stop(" << what << ")";
            acceptor_.close();
        }
    }
    void do_accept() {
        // The new connection gets its own strand
        acceptor_.async_accept(net::make_strand(ioc_),
                               beast::bind_front_handler(&listener::on_accept, shared_from_this()));
    }

    void on_accept(beast::error_code ec, tcp::socket socket) {
        if (ec) {
            fail(ec, "accept");
        } else {
            // Create the session and run it
            std::make_shared<session>(std::move(socket), ctx_, handlers_)->run();
        }

        // Accept another connection
        do_accept();
    }
};

struct HttpRequestHandler {
    HttpRequestHandler(std::string path, request_cb_t handler) : path_{std::move(path)}, handler_{std::move(handler)} {}

    //! the prefix path that should match for this handler to be activated
    std::string path_;
    request_cb_t handler_;
};

class BeastHttpServer final : public HttpServer {
  public:
    BeastHttpServer(net::io_context& ioc) {
        auto listener_ptr = with_settings([this, &ioc](SETTINGS s) {
            auto const address = net::ip::make_address(s.config.httpServer.bindAddress);
            auto const port = static_cast<unsigned short>(s.config.httpServer.port);

            // if (s.config.httpServer.tlsEnabled) {
            // The SSL context is required, and holds certificates
            ssl::context ctx{ssl::context::sslv23_server};
            ctx.use_certificate_file(s.config.security.tlsCertPath, ssl::context_base::file_format::pem);
            ctx.use_private_key_file(s.config.security.tlsKeyPath, ssl::context::file_format::pem);
            //}

            auto listener_ptr =
                std::make_shared<listener>(ioc, std::move(ctx), tcp::endpoint{address, port}, handlers_);
            {
                std::lock_guard g{mtx_};
                shutdown_ = [&ioc, ptr = std::weak_ptr<listener>(listener_ptr)]() {
                    if (auto listener_ = ptr.lock(); listener_) {
                        CVLOG(VMODULE_HTTP_SERVER, 1) << "shutdown closure is being called";
                        net::defer(ioc, std::bind(&listener::stop, listener_, boost::system::error_code(), "shutdown"));
                    }
                };
            }
            return listener_ptr;
        });

        // ctx.use_tmp_dh(net::buffer(dh.data(), dh.size()));

        // Create and launch a listening port
        listener_ptr->run();
    }

    HttpServer& register_handler(std::string path, RequestHandler handler) override {
        std::lock_guard g{mtx_};
        (*handlers_)[path] = handler;
        return *this;
    }

    void stop() override {
        std::lock_guard g{mtx_};
        shutdown_();
    }

    ~BeastHttpServer() override = default;

  private:
    std::function<void()> shutdown_;

    std::shared_ptr<handler_map_t> handlers_ = std::make_shared<handler_map_t>();
    mutable std::mutex mtx_;
};

HttpServer::ptr_t HttpServer::create(boost::asio::io_context& ioc) {
    return std::unique_ptr<HttpServer>{new BeastHttpServer{ioc}};
}

} // namespace nuclm::http_server
