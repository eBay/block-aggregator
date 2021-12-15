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

#include "SimpleFlushTask.h"
#include "FileWriter.h"
#include "KafkaConnector/InvariantChecker.h"

#include "common/logging.hpp"
#include <cassert>

FileWriter* FileWriter::instance = nullptr;

namespace kafka {

InvariantChecker SimpleFlushTask::invariantChecker;
bool SimpleFlushTask::acks = true;

SimpleFlushTask::SimpleFlushTask(int partition, const std::string& table,
                                 std::vector<std::shared_ptr<BufferMessage>>& messages_, int64_t begin, int64_t end) :
        FlushTask(partition, table, begin, end), messages(messages_) {
    auto result = invariantChecker.insert(partition, table, begin, end); // if it is false it cause program to exit
    assert(result);
}

void SimpleFlushTask::start() {
    done = false;
    // ToDO write to ClickHouse in background
    std::vector<std::string> strMessages;
    // just for testing purposes
    strMessages.emplace_back("partition=" + std::to_string(partitionId) + "\ntable= " + table);
    LOG_KAFKA(1) << "messages size: " << messages.size();
    for (auto message : messages) {
        // if (message->data == nullptr) {
        //     LOG_KAFKA(1) << "current message is null";
        // } else {
        //     LOG_KAFKA(1) << "current message is " << message->data_size;
        // }
        strMessages.emplace_back(std::to_string(message->offset) + ": " +
                                 std::string(message->data, message->data_size));
    }
    id = FileWriter::getInstance("batches.txt")->insert(strMessages);
    LOG_KAFKA(1) << BUFF_ID(table, begin, end) "enqueued to be written to file.";
    done = true;
}

bool SimpleFlushTask::blockWait() {
    if (!acks)
        return false;
    // ToDo write for ClickHouse acknowledgement
    FileWriter::getInstance("batches.txt")->wait(id);
    return true; // ToDO if there is a problem for flushing we should return false
}
} // namespace kafka
