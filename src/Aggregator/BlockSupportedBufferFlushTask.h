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

#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/SerializationHelper.h>

#include <KafkaConnector/FlushTask.h>
#include <KafkaConnector/KafkaConnector.h>
#include <Interpreters/Context.h>

#include <Core/BackgroundSchedulePool.h>

#include <future>
#include <string>
#include <memory>
#include <vector>

namespace nuclm {
/**
 * To flush the block constructed from Kafka messages by following the FlushTask interface.
 */
class BlockSupportedBufferFlushTask : public kafka::FlushTask,
                                      public std::enable_shared_from_this<BlockSupportedBufferFlushTask> {
  public:
    static inline const int MAX_NUMBER_OF_LOADING_RETRIES = 10;
    static inline const int MAX_NUMBER_OF_LOADING_RETRIES_QUORUM = 1000;
    static inline const int MAX_NUMBER_OF_LOADING_RETRIES_READONLY = 50;
    // Too frequent retry will make clickhouse become read-only caused by zookeeper timeout.
    static inline const int LOADING_RETRY_MAX_DELAY_MS = 1500;
    static inline const int LOADING_RETRY_DELAY_MS = 500;

  public:
    BlockSupportedBufferFlushTask(int partition, const std::string& table, int64_t begin, int64_t end,
                                  DB::Block& block_to_load_, size_t total_block_bytes_, size_t total_rows_,
                                  std::pair<int64_t, int64_t> minmax_msg_timestamp_,
                                  const ColumnTypesAndNamesTableDefinition& columns_definition_,
                                  const AggregatorLoaderManager& loader_manager_, DB::ContextMutablePtr context_,
                                  kafka::KafkaConnector* kafka_connector_) :
            FlushTask(partition, table, begin, end),
            context(context_),
            pool(context_->getSchedulePool()),
            // block_to_load(block_to_load_),
            columns_definition(columns_definition_),
            loading_started{false},
            loading_done{false},
            loading_succeeded{false},
            executed_times{0},
            table_insert_query{},
            loader_manager(loader_manager_),
            total_block_bytes(total_block_bytes_),
            total_rows(total_rows_),
            minmax_msg_timestamp{minmax_msg_timestamp_},
            kafka_connector(kafka_connector_) {
        assigned_task_id = task_id++;
        send_loading_future = send_loading_done.get_future();
        moveBlock(block_to_load_);
    }

    ~BlockSupportedBufferFlushTask() override;

    void loadBuffer(); // The signature is defined by the scheduled pool task's signature.
    bool isDone() override;

    bool blockWait() override;
    void start() override;

    std::string formulateInsertQuery();

    // move the passed-in block
    void moveBlock(DB::Block& other_block);

  private:
    // To use distributed locking to govern block insertion to clickhouse across replicas in the shard
    void loadBufferWithoutPreventiveLocking();
    // To use busy-trying and no locking to perform block insertion to clickhouse across replicas in the shard
    void loadBufferWithPreventiveLocking();

  private:
    bool heartbeat();
    bool checkQuorumStatus();
    void doBlockInsertion(int& max_retry_times);

    // lag time = minimum kafka message timestamp captured in all rows - time stamp only when the block is successfully
    // loaded to ZooKeeper
    void updateLagTimeOnLoadedBlock();

    /**
     * Error handling strategy.
     *
     * @param errorcode
     * @param max_retry_times
     */
    void handleError(int errorcode, int& max_retry_times);

    DB::Block block_to_load;

    [[maybe_unused]] DB::ContextMutablePtr context;
    DB::BackgroundSchedulePool& pool;

    ColumnTypesAndNamesTableDefinition columns_definition;

    std::atomic_bool loading_started;
    std::atomic_bool loading_done;
    std::atomic_bool loading_succeeded;

    // Retry times.
    std::atomic_int executed_times; // include the original loading and the re-tried loading.

    std::string table_insert_query; // need to have the table insert query as part of the protocol to load block.

    [[maybe_unused]] const AggregatorLoaderManager& loader_manager;
    std::unique_ptr<AggregatorLoader> loader = nullptr;

    std::promise<void> send_loading_done;
    std::future<void> send_loading_future;

    void reloadBuffer(); // in case of the failure;

    void reloadBufferAfter(size_t delay_ms); // in case of the failure;

    // id
    static std::atomic<unsigned long> task_id;
    std::atomic<unsigned long> assigned_task_id;

    // total number of the bytes passed in
    [[maybe_unused]] size_t total_block_bytes;

    // total number of the rows passed in
    [[maybe_unused]] size_t total_rows;

    // min and max of the kafka message timestamp collected from all of the messages in this block buffer
    std::pair<int64_t, int64_t> minmax_msg_timestamp;

    std::function<bool(void)> load_predicate = nullptr;

    // holder to the kafka connector associated
    kafka::KafkaConnector* kafka_connector;

    DB::BackgroundSchedulePoolTaskHolder task_handle;
};

} // namespace nuclm
