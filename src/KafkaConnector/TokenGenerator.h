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
#include "KafkaConnector.h"
#include <jwt/jwt.hpp>
#include <chrono>
namespace kafka {
class TokenGenerator {
  public:
    static std::string generate(const std::string& subject, const std::string& id, const std::string& ip,
                                const std::string& username, const std::string& secret) {
        jwt::jwt_object obj;
        obj.header().algo(jwt::algorithm::HS256);
        obj.add_claim("sub", subject)
            .add_claim("iat", kafka::now() / 1000)
            .add_claim("$int_roles", json_t::array_t{subject})
            .add_claim("id", id)
            .add_claim("username", username)
            .add_claim("ip", ip)
            .secret(secret);
        return obj.signature();
    }
};
} // namespace kafka
