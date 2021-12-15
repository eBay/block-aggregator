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
#include "Buffer.h"
#include "SimpleFlushTask.h"
namespace kafka {

class KafkaConnector;
/**
 * A simple buffer that holds the messages accumulated in the in-memory vector
 */
class SimpleBuffer : public Buffer {
  private:
    std::vector<std::shared_ptr<BufferMessage>> messages; // ToDo use your own data structure instead of this
                                                          // KafkaConnector* kafka_connector;

  public:
    SimpleBuffer(int partition, const std::string& table, uint32_t batch_size, uint32_t batch_timeout,
                 KafkaConnector* kafka_connector_) :
            Buffer(partition, table, batch_size, batch_timeout) {}

    virtual ~SimpleBuffer() {}

    bool append(const char* data, size_t data_size, int64_t offset, int64_t timestamp) override;
    FlushTaskPtr flush() override;
    bool flushable() override;

    bool empty() override;
};
} // namespace kafka
