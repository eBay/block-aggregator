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

#include <functional>

namespace kafka {

/**
 * The definition on the generic thread pool that allows multiple KafkaConnector instances to be launched
 * from the pool.
 */
class GenericThreadPool {
  public:
    using TaskFunc = std::function<void()>;

    virtual ~GenericThreadPool() {}

    virtual void init() = 0;

    // require const reference to use closure in KafkaConnector.cpp
    virtual void enqueue(const TaskFunc& func) = 0;

    virtual void shutdown() = 0;
};

} // namespace kafka
