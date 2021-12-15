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

#include "GenericThreadPool.h"
#include <sstream>
#include <vector>
#include <thread>
#include <memory>
#include "common/logging.hpp"

namespace kafka {

/**
 * The simple thread pool that allows multiple KafkaConnector instances to be launched
 * from the pool with an array of std::thread
 */
class SimpleThreadPool : public GenericThreadPool {
  public:
    SimpleThreadPool() {}

    ~SimpleThreadPool() = default;

    void init() override { LOG_KAFKA(2) << "SimpleThreadPool gets initialized"; }

    void enqueue(const GenericThreadPool::TaskFunc& func) override {
        std::shared_ptr<std::thread> consumer_thread = std::make_shared<std::thread>(func);
        launched_threads.push_back(consumer_thread);
    }

    void shutdown() override {
        try {
            for (auto& thread : launched_threads) {
                thread->join();
            }

            launched_threads.clear();
        } catch (const std::exception& e) {
            std::stringstream description;
            description << "Exception: " << e.what() << " in thread pool shutdown";
            LOG(ERROR) << description.str();
            throw;
        }
    }

  private:
    std::vector<std::shared_ptr<std::thread>> launched_threads;
};

} // namespace kafka
