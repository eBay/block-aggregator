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

#include "coord_http_client.hpp"

#include "common/logging.hpp"
#include "settings_factory.hpp"

#include "jwt/jwt.hpp"

#include <boost/system/error_code.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <boost/beast/ssl.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <chrono>

namespace net = boost::asio; // from <boost/asio.hpp>
namespace ssl = net::ssl;    // from <boost/asio/ssl.hpp>
using tcp = net::ip::tcp;    // from <boost/asio/ip/tcp.hpp>

namespace nucolumnar {
CoordHttpClient::CoordHttpClient(boost::asio::io_context& ioc, long timeout_ms) :
        ioc_(ioc), timeout_ms_(timeout_ms), ssl_ctx_{ssl::context::sslv23_client} {
    // This holds the root certificate used for verification
    // load_root_certificates(ctx);

    // Verify the remote server's certificate
    ssl_ctx_.set_verify_mode(ssl::verify_none /*ssl::verify_peer*/);
}

static int64_t get_time_since_epoch_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool CoordHttpClient::report_db_status(const ServerStatus state, std::string& err_msg) {
    return with_settings([this, state, &err_msg](SETTINGS s) {
        if (s.config.identity.replicaId.empty()) {
            LOG(ERROR) << "Invalid config [config.identify.replicaId]: replicaId is empty";
            err_msg = "Invalid config [config.identify.replicaId]: replicaId is empty";
            return false;
        }
        return report_db_status(s.config.identity.replicaId, state, err_msg);
    });
}

bool CoordHttpClient::report_db_status(const std::string& replica_id, const ServerStatus state, std::string& err_msg) {
    size_t now_ms = get_time_since_epoch_ms();
    LOG_ADMIN(2) << "Updating replica " << replica_id << " database status as " << toServerStatusName(state) << " at "
                 << now_ms;

    std::string target_path = "/api/v1/replica/" + replica_id + "/database";
    std::string payload_json = "{\"apiVersion\":\"v1\",\"data\":{\"state\":\"" + toServerStatusName(state) +
        "\",\"timestamp\": " + std::to_string(now_ms) + "},\"kind\":\"Replica\"}";

    http::request<http::string_body> req{http::verb::post, target_path, 11};
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(beast::http::field::content_type, "application/json");
    req.body() = payload_json;
    req.prepare_payload();
    LOG_ADMIN(3) << "Sending POST " << target_path << " payload: " << payload_json;

    std::stringstream ss;
    http::response<http::dynamic_body> res;
    beast::error_code ec;
    execute_request(req, res, ec);

    if (ec) {
        ss << "Failed to update replica " << replica_id << " database status: " << ec.category().name() << " "
           << ec.message() << ", errcode: " << ec.value();
        err_msg = ss.str();
        LOG(ERROR) << err_msg;
        return false;
    }

    std::string body_str = boost::beast::buffers_to_string(res.body().data());
    if (res.result() != http::status::ok) {
        ss << "Invalid response status from coordinator " << http::obsolete_reason(res.result()) << ": " << body_str;
        err_msg = ss.str();
        LOG(ERROR) << "Failed to update replica " << replica_id << " database status as " << toServerStatusName(state)
                   << " due to " << err_msg;
        return false;
    } else {
        LOG_ADMIN(3) << "Received response from coordinator " << res.result() << " "
                     << http::obsolete_reason(res.result()) << ": " << body_str;
    }
    LOG_ADMIN(1) << "Successfully updated replica " << replica_id << " database status as "
                 << toServerStatusName(state);
    return true;
}

void CoordHttpClient::execute_request(http::request<http::string_body>& req, http::response<http::dynamic_body>& res,
                                      beast::error_code& error) {
    with_settings([this, &req, &res, &error](SETTINGS s) {
        if (s.config.coordClient.coordHost.empty()) {
            LOG(ERROR) << "Invalid config [config.coordClient.coordHost]: coordinator endpoint host is empty";
            error = beast::error_code(beast::errc::invalid_argument, beast::system_category());
            return;
        }
        req.set(http::field::host, s.config.coordClient.coordHost);
        if (s.config.coordClient.coordAuth.coordAuthorizationEnabled) {
            auto jwt_params = SETTINGS_PARAM(processed.authCoordClient);
            jwt::jwt_object obj;
            obj.header().algo(jwt::algorithm::HS256);
            obj.add_claim("sub", jwt_params.subject)
                .add_claim("iat", (long)get_time_since_epoch_ms() / 1000)
                .add_claim("$int_roles", json_t::array_t{jwt_params.subject})
                .add_claim("id", jwt_params.id)
                .add_claim("username", jwt_params.username)
                .add_claim("ip", jwt_params.ip)
                .secret(jwt_params.secret[0]); // todo: support secret rotation

            std::string jwt_token = obj.signature();

            std::stringstream ss;
            ss << "Bearer " << jwt_token;

            LOG_ADMIN(5) << "Using generated token: " << ss.str();
            req.set(http::field::authorization, ss.str().c_str());
        }

        execute_request(s.config.coordClient.coordHost, s.config.coordClient.coordPort, req, res, error);
    });
}

void CoordHttpClient::execute_request(const std::string& host, int port, http::request<http::string_body>& req,
                                      http::response<http::dynamic_body>& res, beast::error_code& error) {

    LOG_ADMIN(4) << "Executing " << req.method_string() << " " << req.target();
    error = beast::error_code(beast::errc::success, beast::generic_category()); // no error

    try {
        // These objects perform our I/O
        tcp::resolver resolver(ioc_);
        beast::ssl_stream<beast::tcp_stream> stream(ioc_, ssl_ctx_);

        // Set SNI Hostname (many hosts need this to handshake successfully)
        if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
            error = beast::error_code{static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()};
            return;
        }

        // Look up the domain name
        auto const results = resolver.resolve(host, std::to_string(port));

        // Make the connection on the IP address we get from a lookup
        std::chrono::milliseconds timeout{timeout_ms_};

        beast::get_lowest_layer(stream).expires_after(timeout);
        beast::get_lowest_layer(stream).connect(results);

        // Perform the SSL handshake
        beast::get_lowest_layer(stream).expires_after(timeout);
        stream.handshake(ssl::stream_base::client);

        // Send the HTTP request to the remote host
        beast::get_lowest_layer(stream).expires_after(timeout);
        http::write(stream, req);

        // This buffer is used for reading and must be persisted
        beast::flat_buffer buffer;

        // Receive the HTTP response
        beast::get_lowest_layer(stream).expires_after(timeout);
        http::read(stream, buffer, res);

        // check response and set error string if necessary
        LOG_ADMIN(4) << "Request " << req.method_string() << " " << req.target() << " received status code "
                     << res.result();

        // Gracefully close the stream
        beast::get_lowest_layer(stream).expires_after(timeout);
        beast::error_code ec;
        stream.shutdown(ec);

        if (ec && ec != net::error::eof) {
            // Rationale:
            // http://stackoverflow.com/questions/25587403/boost-asio-ssl-async-shutdown-always-finishes-with-an-error
            error = ec;
        }
    } catch (std::exception const& e) {
        LOG(ERROR) << "Fail executing http request " << req.method_string() << " " << req.target() << " due to "
                   << e.what();
        error = beast::error_code{beast::errc::interrupted, net::error::get_system_category()};
    }
}
} // namespace nucolumnar
