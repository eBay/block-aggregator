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

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>
#include <Aggregator/TableColumnsDescription.h>
#include <Aggregator/AggregatorLoaderManager.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

class ContextWrapper {
  public:
    ContextWrapper() :
            shared_context_holder(DB::Context::createShared()),
            context{DB::Context::createGlobal(shared_context_holder.get())} {
        context->makeGlobalContext();
    }

    DB::ContextMutablePtr getContext() { return context; }

    boost::asio::io_context& getIOContext() { return ioc; }

    ~ContextWrapper() { LOG(INFO) << "Global context wrapper is now deleted"; }

  private:
    DB::SharedContextHolder shared_context_holder;
    DB::ContextMutablePtr context;
    boost::asio::io_context ioc{1};
};

class ContextRequiredEnvironment : public ::testing::Environment {
  public:
    virtual ~ContextRequiredEnvironment() {}

    // override this to define how to set up the environment
    virtual void SetUp() {
        if (shared_context == nullptr) {
            shared_context = new ContextWrapper();
        }

        LOG(INFO) << "Context Required Environment SetUp is done";
    }

    // override this to define how to tear down the environment
    virtual void TearDown() {
        if (shared_context != nullptr) {
            delete shared_context;
            shared_context = nullptr;
        }
        LOG(INFO) << "Context Required Environment TearDown is done";
    }

    static ContextWrapper* shared_context;
};

class ContextRequiredTest : public ::testing::Test {
  protected:
    // Per-test-suite set-up
    // Called before the first test in this test suite
    // Can be omitted if not needed
    // NOTE: this method is not called SetUpTestSuite, as what is described in:
    // https://github.com/google/googletest/blob/master/googletest/docs/advanced.md
    /// Fxied from: https://stackoverflow.com/questions/54468799/google-test-using-setuptestsuite-doesnt-seem-to-work
    static void SetUpTestCase() { LOG(INFO) << "SetUpTestCase invoked..."; }

    // Per-test-suite tear-down
    // Called after the last test in this test suite.
    // Can be omitted if not needed
    static void TearDownTestCase() { LOG(INFO) << "TearDownTestCase invoked..."; }

    // Define per-test set-up logic as usual
    virtual void SetUp() {
        //...
    }

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    virtual ContextWrapper* GetSharedContext() { return ContextRequiredEnvironment::shared_context; }
};
