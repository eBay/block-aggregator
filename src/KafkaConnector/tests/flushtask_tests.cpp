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

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include "test_common.h"
#include "../SimpleFlushTask.h"

#include "../SimpleFlushTask.h"

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

int basic_test() {
    const char* myMessage = "test message";
    std::vector<std::shared_ptr<kafka::BufferMessage>> vec;
    vec.push_back(std::make_shared<kafka::BufferMessage>(myMessage, strlen(myMessage), 0));
    kafka::SimpleFlushTask flushTask(0, "testTable", vec, 1, 10);
    flushTask.start();
    flushTask.blockWait();

    LOG_KAFKA(3) << "finish the basic testing";

    return 0;
}

int main(int argc, char** argv) {
    // to globally initialize glog
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    TestSuite ts(argc, argv);
    ts.doTest("basic_test", basic_test);
    return 0;
}
