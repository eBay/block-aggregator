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

#ifdef __APPLE__
#include <ctime>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif
// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Serializable/ISerializableDataType.h>
#include <Serializable/SerializableDataTypeFactory.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

/**
 * For primitive types: family name and name are identical
 *
 *  family name identified for FixedString(8) is: FixedString
 *  name identified for FixedString(8) is: FixedString(8)
 *
 *  family name identified for Nullable (String) is: Nullable
 *   name identified for Nullable (String)) is: Nullable(String)
 */
TEST(SerializableObjects, testSerializerCreation) {
    auto& data_type_factory = nuclm::SerializableDataTypeFactory::instance();
    auto typeFromString = [&data_type_factory](const std::string& str) { return data_type_factory.get(str); };

    {
        auto serializer = typeFromString("UInt8");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for UInt8 is: " << familyName;
        LOG(INFO) << "name identified for UInt8 is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("UInt32");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for UInt32 is: " << familyName;
        LOG(INFO) << "name identified for UInt32 is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("UInt64");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for UInt64 is: " << familyName;
        LOG(INFO) << "name identified for UInt64 is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("Float32");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for Float32 is: " << familyName;
        LOG(INFO) << "name identified for Float32 is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("Float64");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for Float64 is: " << familyName;
        LOG(INFO) << "name identified for Float64 is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("Date");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for Date is: " << familyName;
        LOG(INFO) << "name identified for Date is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer1 = typeFromString("DateTime");
        std::string familyName1 = serializer1.get()->getFamilyName();
        std::string name1 = serializer1.get()->getName();

        LOG(INFO) << "family name identified for DateTime is: " << familyName1;
        LOG(INFO) << "name identified for DateTime is: " << name1;
        LOG(INFO) << "=============================== " << name1;

        auto serializer2 = typeFromString("DateTime ('America/Phoenix')");
        std::string familyName2 = serializer2.get()->getFamilyName();
        std::string name2 = serializer2.get()->getName();

        LOG(INFO) << "family name identified for DateTime with time zone = America/Phoenix is: " << familyName2;
        LOG(INFO) << "name identified for DateTime for time zone = America/Phoenix is: " << name2;
        LOG(INFO) << "=============================== " << name2;
    }

    {
        auto serializer1 = typeFromString("DateTime64");
        std::string familyName1 = serializer1.get()->getFamilyName();
        std::string name1 = serializer1.get()->getName();

        LOG(INFO) << "family name identified for DateTime64 is: " << familyName1;
        LOG(INFO) << "name identified for DateTime64 is: " << name1;
        LOG(INFO) << "=============================== " << name1;

        auto serializer2 = typeFromString("DateTime64 (3)");
        std::string familyName2 = serializer2.get()->getFamilyName();
        std::string name2 = serializer2.get()->getName();

        LOG(INFO) << "family name identified for DateTime64 with precision = 3 is: " << familyName2;
        LOG(INFO) << "name identified for DateTime64  with precision = 3 is: " << name2;
        LOG(INFO) << "=============================== " << name2;

        auto serializer3 = typeFromString("DateTime64 (3, 'America/Phoenix')");
        std::string familyName3 = serializer3.get()->getFamilyName();
        std::string name3 = serializer3.get()->getName();

        LOG(INFO) << "family name identified for DateTime64 with precision = 3 and time zone = America/Phoenix is: "
                  << familyName3;
        LOG(INFO) << "name identified for DateTime64  with precision = 3  and time zone = America/Phoenix is: "
                  << name3;
        LOG(INFO) << "=============================== " << name3;
    }

    {
        auto serializer = typeFromString("String");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for String is: " << familyName;
        LOG(INFO) << "name identified for String is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("FixedString (8)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for FixedString(8) is: " << familyName;
        LOG(INFO) << "name identified for FixedString(8) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("Nullable (UInt32)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for Nullable (UInt32) is: " << familyName;
        LOG(INFO) << "name identified for Nullable (UInt32)) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("Nullable (DateTime)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for Nullable (DateTime) is: " << familyName;
        LOG(INFO) << "name identified for Nullable (DateTime)) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("Nullable (String)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for Nullable (String) is: " << familyName;
        LOG(INFO) << "name identified for Nullable (String)) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("LowCardinality (String)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for LowCardinality (String) is: " << familyName;
        LOG(INFO) << "name identified for LowCardinality (String)) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("LowCardinality (FixedString(8))");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for LowCardinality (FixedString(8)) is: " << familyName;
        LOG(INFO) << "name identified for LowCardinality (FixedString(8)) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("LowCardinality (Nullable(String))");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for LowCardinality (Nullable(String)) is: " << familyName;
        LOG(INFO) << "name identified for LowCardinality (Nullable(String)) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("LowCardinality (UInt64)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for LowCardinality (UInt64) is: " << familyName;
        LOG(INFO) << "name identified for LowCardinality (UInt64) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("LowCardinality (DateTime)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for LowCardinality (DateTime) is: " << familyName;
        LOG(INFO) << "name identified for LowCardinality (DateTime) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }

    {
        auto serializer = typeFromString("LowCardinality (Date)");
        std::string familyName = serializer.get()->getFamilyName();
        std::string name = serializer.get()->getName();

        LOG(INFO) << "family name identified for LowCardinality (Date) is: " << familyName;
        LOG(INFO) << "name identified for LowCardinality (Date) is: " << name;
        LOG(INFO) << "=============================== " << name;
    }
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
