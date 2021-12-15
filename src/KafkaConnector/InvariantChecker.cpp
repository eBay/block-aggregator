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

#include "InvariantChecker.h"
#include "common/logging.hpp"
#include <algorithm>
#include <mutex>

namespace kafka {
bool InvariantChecker::checkStatus() { return status; }
void InvariantChecker::clear() {
    lastBatches.clear();
    status = true;
}
bool InvariantChecker::insert(int partition, std::string table, int begin, int end) {
    LOG_KAFKA(2) << "Inserting to invariant checker, partition: " << partition << "table: " << table
                 << " begin: " << begin << " end: " << end << " getMaxEnd: " << getMaxEnd(partition);
    std::lock_guard<std::mutex> lck(mtx);
    auto it_partition = lastBatches.find(partition);
    if (it_partition != lastBatches.end()) {
        auto it = it_partition->second.find(table);
        if (it != it_partition->second.end()) {
            auto begin_ = it->second.first;
            auto end_ = it->second.second;

            if (end >= begin_ && begin <= end_) {
                if (end != end_ || begin != begin_) {
                    LOG(ERROR) << "Batching invariant: VIOLATED!"
                               << "\npartition: " << partition << "\ntable name: " << table
                               << "\nlast batch: begin= " << begin_ << " end= " << end_
                               << "\n next batch: begin= " << begin << " end= " << end;

                    return false;
                }
            }
        }
    }
    lastBatches[partition][table].first = begin;
    lastBatches[partition][table].second = end;

    LOG(ERROR) << "Batching invariant: PASS!"
               << "\npartition: " << partition << "\ntable name: " << table << "\nnext batch: begin= " << begin
               << " end= " << end;
    return status = true;
}

int InvariantChecker::getMaxEnd(int partition) {
    int result = -1;
    auto it_partition = lastBatches.find(partition);
    if (it_partition != lastBatches.end()) {
        auto it_table = it_partition->second.begin();
        while (it_table != it_partition->second.end()) {
            result = std::max(result, it_table->second.second);
            it_table++;
        }
    }
    return result;
}
} // namespace kafka
