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
#include "Buffer.h"

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

#include <mutex>
#include <condition_variable>
#include <queue>
#include <iostream>
#include "common/logging.hpp"

#include <glog/logging.h>
#include <string>

namespace kafka {

enum KafkaConnectorError { NO_ERROR = 0, KAFKA_ERROR = 1, CH_ERROR = 2, MSG_ERROR = 3 };
static std::vector<std::string> KafkaConnectorErrorNames = {"NO_ERROR", "KAFKA_ERROR", "CH_ERROR", "MSG_ERROR"};

inline std::string getKafkaConnectorErrorName(KafkaConnectorError error) { return KafkaConnectorErrorNames[error]; }

class KafkaConnector;

/**
 * It is responsible for consuming the message from a particular partition. Based on its current state
 * it consume the received message as part of the reply mode or the consume mode.
 */
class PartitionHandler {
    enum PartitionState { REPLAY = 1, CONSUME = 2 };
    // Note, none of the get functions race with the set function. It is only possilbe that toJson race with set
    // functions.
    class PartitionHandlerStatus {
      public:
        void init(Metadata* saved_metadata_, int64_t offset_, PartitionState state_) {
            std::lock_guard<std::mutex> lck(mtx);
            state = state_;
            metadata.addFrom(saved_metadata_);
            last_committed_metadata = metadata.serialize();
            initial_metadata = last_committed_metadata;
            last_committed_offset = offset_;
            last_known_offset = metadata.max();
        }

        int64_t getLastKnownOffset() {
            std::lock_guard<std::mutex> lck(mtx);
            return last_known_offset;
        }
        void setLastKnownOffset(int64_t offset_) {
            std::lock_guard<std::mutex> lck(mtx);
            last_known_offset = offset_;
        }

        PartitionState getState() {
            std::lock_guard<std::mutex> lck(mtx);
            return state;
        }
        void setState(PartitionState state_) {
            std::lock_guard<std::mutex> lck(mtx);
            state = state_;
        }

        std::string getSerializedMetadata() {
            std::lock_guard<std::mutex> lck(mtx);
            return metadata.serialize();
        }
        int64_t getMinOffset() {
            std::lock_guard<std::mutex> lck(mtx);
            return metadata.min();
        }
        int64_t getMaxOffset() {
            std::lock_guard<std::mutex> lck(mtx);
            return metadata.max();
        }
        Offset getOffset(const std::string& table) {
            std::lock_guard<std::mutex> lck(mtx);
            return metadata.getOffset(table);
        }
        void updateMetadata(const std::string& table, int64_t begin_, int64_t end_, int64_t count) {
            std::lock_guard<std::mutex> lck(mtx);
            metadata.update(table, begin_, end_, count);
        }

        void clearMetadata() {
            std::lock_guard<std::mutex> lck(mtx);
            metadata.clear();
        }

        void setReplicaId(const std::string& replica_id_) {
            std::lock_guard<std::mutex> lck(mtx);
            metadata.setReplicaId(replica_id_);
        }

        void setReference(int reference_) {
            std::lock_guard<std::mutex> lck(mtx);
            LOG_KAFKA(1) << "Setting reference to " << reference_;
            metadata.setReference(reference_);
        }

        int getReference() {
            std::lock_guard<std::mutex> lck(mtx);
            return metadata.getReference();
        }

        int64_t getLastCommittedOffset() {
            std::lock_guard<std::mutex> lck(mtx);
            return last_committed_offset;
        }
        void setLastCommittedOffset(int64_t offset_) {
            std::lock_guard<std::mutex> lck(mtx);
            last_committed_offset = offset_;
        }

        std::string getSerializedReplayedBatches() {
            std::lock_guard<std::mutex> lck(mtx);
            return replayed_batches.serialize();
        }
        void updateReplayedBatch(std::string table, int64_t begin_, int64_t end_, int64_t count) {
            std::lock_guard<std::mutex> lck(mtx);
            replayed_batches.update(table, begin_, end_, count);
        }

        nlohmann::json toJson() {
            std::lock_guard<std::mutex> lck(mtx);
            nlohmann::json j;
            j["state"] = (state == PartitionState::REPLAY) ? "REPLAY" : "CONSUME";
            j["last_known_offset"] = last_known_offset;
            j["current_metadata"] = metadata.serialize();
            j["last_committed_offset"] = last_committed_offset;
            j["last_committed_metadata"] = last_committed_metadata;
            return j;
        }

        void updateLastCommittedMetadataAndOffset() {
            std::lock_guard<std::mutex> lck(mtx);
            last_committed_metadata = metadata.serialize();
            last_committed_offset = metadata.min();
        }

        std::string getInitialMetadata() {
            std::lock_guard<std::mutex> lck(mtx);
            return initial_metadata;
        }

      private:
        int64_t last_known_offset = Metadata::EARLIEST_OFFSET;
        PartitionState state;
        Metadata metadata;
        int64_t last_committed_offset = -1;
        std::string last_committed_metadata = "";
        std::string initial_metadata = "";
        Metadata replayed_batches;
        std::mutex mtx;
    };

  private:
    PartitionHandlerStatus status;
    std::unordered_map<std::string, std::shared_ptr<Buffer>>
        buffers; // buffers that we are currently filling  table --> Buffer
    std::unordered_map<std::string, FlushTaskPtr> activeTasks; // table --> FlushTask

    // Metadata metadata;      // metadata containing metadata for all tables for this partition
    Metadata savedMetadata; // what we read from Kafka upon recovery. One time initialization, and we only remove from
                            // it
    std::string previousMetadata;

    std::string topic;
    int partitionId;
    // last consumed message from this partition. It helps use to move offset for this partitions
    // at the time of the re-assignment.
    // int64_t lastKnownOffset;
    // PartitionState state; // state of the partition: REPLY/CONSUME

    int64_t pos;
    int64_t end;   // initial max offset in partition metadata.
    int64_t begin; // initial min offset in partition metadata.

    // reference back to the kafka connector.
    KafkaConnector* kafkaConnector;

    bool append(std::string table, const char* data, size_t data_size, int64_t offset, int64_t msg_timestamp);
    void flushAll();

  public:
    static std::string print_toppar_list(const rd_kafka_topic_partition_list_t* list);

    ~PartitionHandler() = default;

    explicit PartitionHandler(const std::string& topic_, int partition_, int64_t offset_, std::string& metadata_,
                              KafkaConnector* kafkaConnector_);

    int64_t getOffset() { return status.getLastKnownOffset(); }
    bool consumeMode() { return status.getState() == CONSUME; }
    KafkaConnectorError consume(RdKafka::Message* msg);
    KafkaConnectorError checkBuffers(rd_kafka_t* rk);

    KafkaConnectorError commitMetadata(const rd_kafka_topic_partition_list_t* toppar_to_commit, rd_kafka_t* rk);
    int getPartitionId() { return partitionId; }
    nlohmann::json toJson() { return status.toJson(); }
    std::string getSerializedReplayedBatches() { return status.getSerializedReplayedBatches(); }
    std::string getInitialMetadata() { return status.getInitialMetadata(); }
};
} // namespace kafka
