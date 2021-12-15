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
#include "KafkaConnector/Metadata.h"
#include <stdio.h>
// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

int basic_test() {
    kafka::Metadata metadata("replica1", 0);
    metadata.update("myTable1", 1, 10, 10);
    metadata.update("myTable1", 2, 10, 8);
    metadata.update("myTable2", 5, 14, 4);
    metadata.update("myTable2", 10, 25, 10);
    metadata.update("myTable3", 11, 18, 3);

    metadata.update("myTable4", 11, 30, 3);
    metadata.remove("myTable4");
    CHK_EQ(1, metadata.getVersion());
    CHK_EQ(2, metadata.min());
    CHK_EQ(25, metadata.max());
    CHK_EQ(std::string("replica1"), metadata.getReplicaId());
    CHK_EQ(0, metadata.getReference());

    auto serialized = metadata.serialize();
    kafka::Metadata metadata2;
    metadata2.deserialize(serialized);
    CHK_EQ(2, metadata.min());
    CHK_EQ(25, metadata.max());
    CHK_EQ(1, metadata.getVersion());
    CHK_EQ(std::string("replica1"), metadata.getReplicaId());
    CHK_EQ(0, metadata.getReference());

    CHK_EQ(2, metadata2.min());
    CHK_EQ(25, metadata2.max());
    CHK_EQ(1, metadata2.getVersion());
    CHK_EQ(std::string("replica1"), metadata2.getReplicaId());
    CHK_EQ(0, metadata2.getReference());

    kafka::Metadata metadata3;
    metadata3.addFrom(&metadata2);
    CHK_EQ(2, metadata3.min());
    CHK_EQ(25, metadata3.max());
    CHK_EQ(std::string("no_replica_id"), metadata3.getReplicaId());
    CHK_EQ(0, metadata3.getReference());

    return 0;
}

int version_test() {
    kafka::Metadata metadata("replica1", 0);
    metadata.update("myTable1", 1, 10, 10);
    metadata.update("myTable2", 5, 14, 4);

    CHK_EQ(std::string("myTable1,1,10,myTable2,5,14"), metadata.serialize(0));
    CHK_EQ(std::string("1,,replica1,myTable1,1,10,10,myTable2,5,14,4,,0"), metadata.serialize(1));

    kafka::Metadata::setVersion(1);
    CHK_EQ(std::string("1,,replica1,myTable1,1,10,10,myTable2,5,14,4,,0"), metadata.serialize());

    kafka::Metadata::setVersion(0);
    CHK_EQ(std::string("myTable1,1,10,myTable2,5,14"), metadata.serialize());

    return 0;
}

int deserialize_test_from_v0() {
    kafka::Metadata metadata;
    metadata.deserialize("myTable1,1,10,myTable2,5,14");

    CHK_EQ(-1, metadata.getReference());

    CHK_EQ(1, metadata.getOffset("myTable1").begin);
    CHK_EQ(10, metadata.getOffset("myTable1").end);
    CHK_EQ(0, metadata.getOffset("myTable1").count);

    CHK_EQ(5, metadata.getOffset("myTable2").begin);
    CHK_EQ(14, metadata.getOffset("myTable2").end);
    CHK_EQ(0, metadata.getOffset("myTable2").count);

    CHK_EQ(std::string("1,,no_replica_id,myTable1,1,10,0,myTable2,5,14,0,,-1"), metadata.serialize(1));
    CHK_EQ(std::string("myTable1,1,10,myTable2,5,14"), metadata.serialize(0));
    return 0;
}

int deserialize_test_from_v1() {
    kafka::Metadata::setVersion(1);
    kafka::Metadata metadata;
    metadata.deserialize("1,,replica1,myTable1,1,10,10,myTable2,5,14,4,,0");

    CHK_EQ(0, metadata.getReference());
    CHK_EQ(1, metadata.getVersion()); // note that althought the input string is version 0, we want use default version
                                      // (1) for all objects.

    CHK_EQ(1, metadata.getOffset("myTable1").begin);
    CHK_EQ(10, metadata.getOffset("myTable1").end);
    CHK_EQ(10, metadata.getOffset("myTable1").count);

    CHK_EQ(5, metadata.getOffset("myTable2").begin);
    CHK_EQ(14, metadata.getOffset("myTable2").end);
    CHK_EQ(4, metadata.getOffset("myTable2").count);

    CHK_EQ(std::string("1,,replica1,myTable1,1,10,10,myTable2,5,14,4,,0"), metadata.serialize(1));
    CHK_EQ(std::string("myTable1,1,10,myTable2,5,14"), metadata.serialize(0));
    return 0;
}

int add_from_version_0() {
    kafka::Metadata metadata;
    kafka::Metadata::setVersion(0);
    CHK_EQ(0, metadata.getVersion());
    metadata.deserialize("myTable1,1,10,myTable2,5,14");

    kafka::Metadata metadata2;
    metadata2.addFrom(&metadata);
    CHK_EQ(-1, metadata2.getReference());

    CHK_EQ(1, metadata2.getOffset("myTable1").begin);
    CHK_EQ(10, metadata2.getOffset("myTable1").end);
    CHK_EQ(0, metadata2.getOffset("myTable1").count);

    CHK_EQ(5, metadata2.getOffset("myTable2").begin);
    CHK_EQ(14, metadata2.getOffset("myTable2").end);
    CHK_EQ(0, metadata2.getOffset("myTable2").count);

    return 0;
}

int main(int argc, char** argv) {
    // to globally initialize glog
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    TestSuite ts(argc, argv);
    ts.doTest("basic_test test", basic_test);
    ts.doTest("version_test test", version_test);
    ts.doTest("deserialize_test test", deserialize_test_from_v0);
    ts.doTest("deserialize_test test", deserialize_test_from_v1);
    ts.doTest("add_from_version_0 test", add_from_version_0);
    return 0;
}
