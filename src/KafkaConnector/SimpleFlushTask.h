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
#include "InvariantChecker.h"
#include "FlushTask.h"

#include <condition_variable>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace kafka {
struct BufferMessage {
  public:
    BufferMessage(const char* data_, size_t data_size_, int64_t offset_) : data_size(data_size_), offset(offset_) {
        data = (char*)malloc(data_size);
        memcpy(data, data_, data_size);
    }

    ~BufferMessage() {
        free(data);
        data = nullptr;
    }

    char* data;
    size_t data_size;
    int64_t offset;
};
/**
 * The generic definition for Flush Task
 */
class SimpleFlushTask : public FlushTask {
  private:
    std::vector<std::shared_ptr<BufferMessage>> messages;
    static bool acks;
    bool done = false;

  public:
    static InvariantChecker invariantChecker;

    SimpleFlushTask(int partition, const std::string& table, std::vector<std::shared_ptr<BufferMessage>>& messages_,
                    int64_t begin, int64_t end);

    static void stopAcks() { acks = false; }
    static void startAcks() { acks = true; }
    static void clearInvariantChecker() { invariantChecker.clear(); }

    bool isDone() override { return done; }

    bool blockWait() override;
    void start() override;
};
} // namespace kafka

#define BUFF_ID(table, begin, end) "Buffer " << table << "[" << begin << "," << end << "]: "
