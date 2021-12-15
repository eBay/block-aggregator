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
#include "FactoryDefinitions.h"
#include "SimpleBuffer.h"
#include "memory"

namespace kafka {

/**
 * The Factory to create the corresponding buffer
 */
class SimpleBufferFactory : public IBufferFactory {

  public:
    SimpleBufferFactory() = default;

  public:
    ~SimpleBufferFactory() = default;

    std::shared_ptr<Buffer> createBuffer(int partition, const std::string& table, uint32_t batch_size,
                                         uint32_t batch_timeout, KafkaConnector* kafka_connector) override {
        return std::make_shared<SimpleBuffer>(partition, table, batch_size, batch_timeout, kafka_connector);
    }
};
} // namespace kafka
