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

#include <DataTypes/DataTypeString.h>
#include <common/LocalDateTime.h>

#include <Poco/Timespan.h>

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

static Poco::Timespan timespan(uint32_t interval_ms) {
    uint32_t second = interval_ms / 1000;
    uint32_t remaining_ms = interval_ms - second * 1000;

    return Poco::Timespan(second, remaining_ms * 1000);
}

/**
 * Function time_t time(time_t* timer) returns the number of seconds elapsed since 00:00 hours, Jan 1, 1970 UTC.
 * time_t now = time (nullptr);
 *
 */
TEST(SerializerForProtobuf, testTimeConversionFromMStoDate) {
    uint64_t ms = 1579817878440; // GMT: Thursday, January 23, 2020 10:17:58.440 PM
    time_t seconds = (time_t)(ms / 1000);
    auto today = DateLUT::instance().toDayNum(seconds).toUnderType();

    LOG(INFO) << "time stamp is: GMT: Thursday, January 23, 2020 10:17:58.440 PM";
    LOG(INFO) << " day num with type conversation to DB::UInt16 is:  " << (DB::UInt16)today;
    LOG(INFO) << " day num with static_cast to DB::UInt16 is: " << static_cast<DB::UInt16>(today);
    ASSERT_EQ((DB::UInt16)today, static_cast<DB::UInt16>(today));

    // time zone
    LOG(INFO) << " local time zone is: " << DateLUT::instance().getTimeZone();
}

TEST(SerializerForProtobuf, testTimeConversionFromSecondToDate) {
    uint64_t seconds = 1579089600; // GMT: Wednesday, January 15, 2020 12:00:00 PM
    time_t actual_seconds = (time_t)(seconds);
    auto today = DateLUT::instance().toDayNum(actual_seconds).toUnderType();

    LOG(INFO) << "time stamp is: GMT: Wednesday, January 15, 2020 12:00:00 PM";
    LOG(INFO) << " day num with type conversation to DB::UInt16 is:  " << (DB::UInt16)today;
    LOG(INFO) << " day num with static_cast to DB::UInt16 is: " << static_cast<DB::UInt16>(today);
    ASSERT_EQ((DB::UInt16)today, static_cast<DB::UInt16>(today));

    // time zone
    LOG(INFO) << " local time zone is: " << DateLUT::instance().getTimeZone();
}

TEST(AggregatorLoaderTimeSpan, testTimespanConversion) {
    uint32_t ms = 30000;
    Poco::Timespan timespan_result = timespan(ms);

    LOG(INFO) << "timespan result in millisecond is: " << timespan_result.totalMilliseconds();
}

int64_t now() { return std::chrono::system_clock::now().time_since_epoch().count(); }

TEST(KafkaConnectorTimeUsedNow, testFunctioNowUnit) {
    int64_t time_now = now();
    LOG(INFO) << " now is (in nano seconds): " << time_now;
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
