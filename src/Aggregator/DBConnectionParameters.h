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

#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>

namespace nuclm {

struct DatabaseConnectionParameters {
    std::string host;
    UInt16 port{};
    std::string default_database;
    DB::Protocol::Secure security = DB::Protocol::Secure::Disable;
    DB::Protocol::Compression compression = DB::Protocol::Compression::Enable;
    DB::ConnectionTimeouts timeouts;
};

inline std::ostream& operator<<(std::ostream& os, const DB::Protocol::Secure& p) {
    os << (uint64_t)p;
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const DB::Protocol::Compression& p) {
    os << (uint64_t)p;
    return os;
}

#define pp1(x) #x "=" << p.x
#define pp2(x) ", " pp1(x)

inline std::ostream& operator<<(std::ostream& os, const Poco::Timespan& p) {
    os << p.milliseconds() << "ms";
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const DB::ConnectionTimeouts& p) {
    os << "{" << pp1(connection_timeout.totalMilliseconds()) << pp2(send_timeout.totalMilliseconds())
       << pp2(receive_timeout.totalMilliseconds()) << pp2(tcp_keep_alive_timeout.totalMilliseconds())
       << pp2(http_keep_alive_timeout.totalMilliseconds()) << "}";
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const DatabaseConnectionParameters& p) {
    os << pp1(host) << pp2(port) << pp2(default_database) << pp2(security) << pp2(compression) << pp2(timeouts);
    return os;
}

} // namespace nuclm
