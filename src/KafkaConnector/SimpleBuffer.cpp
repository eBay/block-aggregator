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

#include "SimpleBuffer.h"
#include "KafkaConnector/Buffer.h"
#include "KafkaConnector/SimpleFlushTask.h"
#include "common/logging.hpp"

namespace kafka {
bool SimpleBuffer::append(const char* data, size_t data_size, int64_t offset, int64_t timestamp) {
    if (begin_ == Metadata::EARLIEST_OFFSET) {
        begin_ = offset;
        flushedAt = now();
    }
    end_ = offset;
    messages.push_back(
        std::make_shared<BufferMessage>(data, data_size, offset)); // ToDo implement the consumption logic here
    LOG_KAFKA(1) << BUFF_ID(table, begin_, end_) "Message append to buffer. Size=" << messages.size()
                 << "message time stamp: " << timestamp;
    return true;
}

FlushTaskPtr SimpleBuffer::flush() {
    FlushTaskPtr task = nullptr;
    if (!messages.empty()) {
        task = std::make_shared<SimpleFlushTask>(
            partitionId, table, messages, begin_,
            end_); // ToDo copy buffer data to this this buffer will be used for future messages
        messages.clear();
    }
    begin_ = Metadata::EARLIEST_OFFSET;
    flushedAt = now();
    return task;
}

bool SimpleBuffer::flushable() {
    auto t_now = now();
    return messages.size() >= batchSize || t_now - flushedAt > batchTimeout;
}

bool SimpleBuffer::empty() { return messages.empty(); }

} // namespace kafka
