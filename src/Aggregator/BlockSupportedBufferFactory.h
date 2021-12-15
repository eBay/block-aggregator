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

#include "Aggregator/AggregatorLoaderManager.h"
#include "Aggregator/BlockSupportedBuffer.h"
#include "KafkaConnector/FactoryDefinitions.h"
#include <Interpreters/Context.h>

#include <memory>

namespace nuclm {

/**
 * The Factory to create the corresponding buffer
 */
class BlockSupportedBufferFactory : public kafka::IBufferFactory {

  public:
    BlockSupportedBufferFactory(const AggregatorLoaderManager& loader_manager_, DB::ContextMutablePtr context_) :
            loader_manager(loader_manager_), context(context_) {}

  public:
    ~BlockSupportedBufferFactory() override = default;

    std::shared_ptr<kafka::Buffer> createBuffer(int partition, const std::string& table, uint32_t batch_size,
                                                uint32_t batch_timeout,
                                                kafka::KafkaConnector* kafka_connector) override {
        return std::make_shared<BlockSupportedBuffer>(loader_manager, partition, table, batch_size, batch_timeout,
                                                      context, kafka_connector);
    }

  private:
    const nuclm::AggregatorLoaderManager& loader_manager;
    DB::ContextMutablePtr context;
};
} // namespace nuclm
