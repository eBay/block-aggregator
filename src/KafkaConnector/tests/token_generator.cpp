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

#include "../TokenGenerator.h"

#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <iostream>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

using namespace std;

int main(int argc, char** argv) {
    // subject, id, ip, username, secret
    if (argc < 6) {
        cout << "Usage: " << argv[0] << " <subject> <id> <ip> <username> <secret>" << endl;
        return 0;
    }
    string subject = argv[1];
    string id = argv[2];
    string ip = argv[3];
    string username = argv[4];
    string secret = argv[5];

    // std::string token = kafka::TokenGenerator::generate("my_subject", "2", "10.148.181.161", "my_username",
    //                                                     "636f4088-c54d-491e-a65b-66c8e5a8fdbf");
    std::string token = kafka::TokenGenerator::generate(subject, id, ip, username, secret);
    std::cout << token << std::endl;
    return 0;
}
