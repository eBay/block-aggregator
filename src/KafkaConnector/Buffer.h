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

#include "FlushTask.h"
#include "Metadata.h"

namespace kafka {
int64_t now();

/**
 * A generic buffer that can be extended to have different way to contain data to be processed for a table. The
 * buffer belongs to a table and has its own partition id.
 */
class Buffer {
  protected:
    int64_t begin_;
    int64_t end_;
    int64_t count_;    // count since the begining, not only for the current begin and end
    int64_t flushedAt; // time stamp
    std::string table; // the associated table

    uint32_t batchSize; // the limit of the batch size.
    uint32_t batchTimeout;

    // for testing purposes
    int partitionId;

  public:
    Buffer(int partition, const std::string& table_, uint32_t batch_size, uint32_t batch_timeout) :
            begin_(Metadata::EARLIEST_OFFSET),
            end_(Metadata::EARLIEST_OFFSET),
            count_(0),
            flushedAt(0),
            table(table_),
            batchSize(batch_size),
            batchTimeout(batch_timeout),
            partitionId(partition) {}

    virtual ~Buffer() = default;

    virtual bool append(const char* data, size_t data_size, int64_t offset, int64_t timestamp) = 0;

    virtual FlushTaskPtr flush() = 0;

    virtual bool flushable() = 0;

    virtual bool empty() = 0;

    int64_t begin() { return begin_; }

    int64_t end() { return end_; }

    int64_t count() { return count_; }

    void setCount(int64_t count) { count_ = count; }

    std::string getTable() { return table; }
};
} // namespace kafka
