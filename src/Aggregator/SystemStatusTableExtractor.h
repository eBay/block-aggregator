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

#include <Interpreters/Context.h>
#include <boost/asio.hpp>
#include <sstream>
#include <vector>
#include <memory>

namespace nuclm {

class AggregatorLoaderManager;

struct SystemReplicasExtractedResult {
    std::string table_name;
    bool is_leader;
    bool is_readonly;
    bool is_session_expired;
    uint32_t future_parts;
    uint32_t parts_to_check;
    uint32_t queue_size;
    uint32_t inserts_in_queue;
    uint32_t merges_in_queue;
    uint64_t log_max_index;
    uint64_t log_pointer;

    // Follow DataTypeDateTime.cpp, it uses time_t for internal representation. More specifically, it is
    // mapped to uint32_t, by following the C++ ClickHouse client library's ColumnDateTime implementation
    uint32_t last_queue_update; // from DateTime

    int32_t last_queue_update_time_diff_to_now; // difference to now, can be negative, if time is not synchronzied.
    uint64_t absolute_delay;                    // How big lag in seconds the current replica has.
    int32_t total_replicas;                     // In ClickHouse Types.h, UInt8 is defined as char8_t;
    int32_t active_replicas;                    // In ClickHouse Types.h, UInt8 is defined as char8_t;

    SystemReplicasExtractedResult() :
            table_name{},
            is_leader{false},
            is_readonly{false},
            is_session_expired{false},
            future_parts{0},
            parts_to_check{0},
            queue_size{0},
            inserts_in_queue{0},
            merges_in_queue{0},
            log_max_index{0},
            log_pointer{0},
            last_queue_update{0},
            last_queue_update_time_diff_to_now{0},
            absolute_delay{0},
            total_replicas{0},
            active_replicas{0} {}

    std::string str() const {
        std::stringstream description;
        description << "(";
        description << "table_name: " << table_name << ",";
        description << " is_leader: " << is_leader << ",";
        description << " is_readonly: " << is_readonly << ",";
        description << " is_session_expired: " << is_session_expired << ",";
        description << " future_parts: " << future_parts << ",";
        description << " parts_to_check: " << parts_to_check << ",";
        description << " queue_size: " << queue_size << ",";
        description << " inserts_in_queue: " << inserts_in_queue << ",";
        description << " merges_in_queue: " << merges_in_queue << ",";
        description << " log_max_index: " << log_max_index << ",";
        description << " log_pointer: " << log_pointer << ",";
        description << " last_queue_update: " << last_queue_update << ",";
        description << " last_queue_update_time_diff_to_now: " << last_queue_update_time_diff_to_now << ",";
        description << " absolute_delay (in seconds): " << absolute_delay << ",";
        description << " total_replicas: " << total_replicas << ",";
        description << " active_replicas: " << active_replicas;
        description << ")";

        return description.str();
    }
};

struct SystemTablesExtractedResult {
    std::string table_name;
    uint64_t total_rows;
    uint64_t total_bytes;

    SystemTablesExtractedResult() : table_name{}, total_rows{0}, total_bytes{0} {}

    std::string str() const {
        std::stringstream description;
        description << "(";
        description << "table_name: " << table_name << ",";
        description << " total_rows: " << total_rows;
        description << " total_bytes: " << total_bytes;
        description << ")";

        return description.str();
    }
};

class SystemStatusTableExtractor {
  public:
    SystemStatusTableExtractor(boost::asio::io_context& ioc, const AggregatorLoaderManager& loader_manager_,
                               DB::ContextMutablePtr context_);

    ~SystemStatusTableExtractor() = default;

    void start();
    void stop();

  protected:
    void doExtractTablesWork();
    bool doExtractSystemReplicasWork(std::vector<SystemReplicasExtractedResult>&);
    bool doExtractSystemTablesWork(std::vector<SystemTablesExtractedResult>&);

    void reportSystemReplicasMetrics(const std::vector<SystemReplicasExtractedResult>&);
    void reportSystemTablesMetrics(const std::vector<SystemTablesExtractedResult>&);

  private:
    void setupAsyncWaitTimer();
    void extractTables();

    const AggregatorLoaderManager& loader_manager;
    DB::ContextMutablePtr context;
    boost::asio::steady_timer timer;
    std::atomic<bool> running;
};

}; // namespace nuclm
