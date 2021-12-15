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

#include <string>
#include <vector>
#include <memory>

namespace kafka {
/**
 * The generic definition for Flush Task
 */
class FlushTask {
  protected:
    std::string table;
    int64_t begin;
    int64_t end;
    int64_t id;

    // for testing purposes
    int partitionId;

  public:
    FlushTask(int partition, const std::string& table_, int64_t begin, int64_t end) :
            table(table_), begin(begin), end(end), id(-1), partitionId(partition) {}

    virtual ~FlushTask() = default;

    virtual bool isDone() = 0;
    virtual bool blockWait() = 0;
    virtual void start() = 0;
};
using FlushTaskPtr = std::shared_ptr<FlushTask>;
} // namespace kafka
