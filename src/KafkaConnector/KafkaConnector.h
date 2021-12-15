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

#include "GenericThreadPool.h"
#include "KafkaConnector/Metadata.h"
#include "PartitionHandler.h"
#include "RebalanceCb.h"
#include "EventCb.h"
#include "global.h"

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <nlohmann/json.hpp>

#include <unordered_map>
#include <string>
#include <queue>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <iostream>
#include <stdio.h>

namespace kafka {

int64_t now();

using BackendServerHealthCheckFunction = std::function<bool()>;

struct Rebalance {
    std::string timestamp;

    Rebalance() {
        time_t rawtime;
        time(&rawtime);
        struct tm* timeinfo;
        timeinfo = localtime(&rawtime);
        timestamp = std::string(asctime(timeinfo));
    }
    /*
    partition id --> metadata of that partition when assigned.
    It is set by the rebalance handler upon partition assigment.
    */
    std::unordered_map<int, std::string> initialMetadata;

    /*
    partition id --> metadata containing the first batches sent for each table.
    It is set when we flush to a table during the REPLAY.
    */
    std::unordered_map<int, std::string> replayedBatches;

    std::string toString() {
        std::string result = "Timestamp: " + timestamp + "\n";
        result += "kafka_metadata: \n";
        for (auto it = initialMetadata.begin(); it != initialMetadata.end(); it++) {
            result += "\t [" + std::to_string(it->first) + "]: " + it->second + "\n";
        }
        result += "replayed_batches: \n";
        for (auto it = replayedBatches.begin(); it != replayedBatches.end(); it++) {
            result += "\t [" + std::to_string(it->first) + "]: " + it->second + "\n";
        }
        return result;
    }

    nlohmann::json toJson() {
        nlohmann::json j;
        j["time"] = timestamp;
        for (auto it = initialMetadata.begin(); it != initialMetadata.end(); it++)
            j["kafka_metadata"][std::to_string(it->first)] = it->second;
        for (auto it = replayedBatches.begin(); it != replayedBatches.end(); it++)
            j["replayed_batches"][std::to_string(it->first)] = it->second;
        return j;
    }
};

/**
 * This is the main class that handles connection to the Kafka for a a topic.
 */
class KafkaConnector {
  private:
    size_t idx; // index of this connector in the list of connectors
    std::unique_ptr<RdKafka::Conf> config;
    // const KafkaConnectorParameters& connector_parameters;
    std::shared_ptr<GenericThreadPool> thread_pool;
    std::atomic<bool> running;

    std::atomic<bool> freeze_traffic_flag; // the in-memory fag set from external command
    // the status on whether traffic gets really freezed or not after receiving freeze_traffic_flag
    std::atomic<bool> freeze_traffic_status;

    bool stopped;
    std::mutex stoppedMtx;
    std::condition_variable connector_stopped;

    bool stopCompleted;
    std::mutex stopCompletedMtx;
    std::condition_variable stopCompletedCv;

    // rdkafka variables
    std::unique_ptr<RdKafka::KafkaConsumer> consumer;

    RebalanceHandler rbCb;

    EventHandler evCb;

    // Backend database health checking function
    BackendServerHealthCheckFunction database_health_checker;

    void disconnectKafka();
    void connectKafka();
    void checkUntilBackendOK();
    void checkUntilPersistentFreezeFlagRemoved(const std::string& var_zone, const std::string& var_topic);

    void startConsumer();
    void notify_stopped();
    void wait_stopped();

    // to debug consume/replay issue
    bool forced_in_consume_mode{false};

  public:
    // todo: idx is bad, need to refactor later.
    KafkaConnector(size_t idx_, RdKafka::Conf* config_, std::shared_ptr<GenericThreadPool> thread_pool_,
                   BackendServerHealthCheckFunction database_health_checker_) :
            idx(idx_),
            config(config_),
            thread_pool(thread_pool_),
            running(false),
            freeze_traffic_flag(false),
            freeze_traffic_status(false),
            stopped(false),
            stopCompleted(false),
            consumer(nullptr),
            rbCb{this},
            evCb{idx_},
            database_health_checker(database_health_checker_) {
        std::string err;
        std::string autoOffsetReset;
        config->get("auto.offset.reset", autoOffsetReset);
        if (autoOffsetReset != "earliest") {
            LOG(WARNING) << "KafkaConnector [" << idx
                         << "]: auto.offset.reset must be earliest. Changing it to earliest forcefully...";
            if (config->set("auto.offset.reset", "earliest", err) != RdKafka::Conf::CONF_OK) {
                LOG(FATAL) << "KafkaConnector [" << idx
                           << "]: KafkaConnector: Failed to set auto.offset.reset: " << err;
            }
        }

        if (config->set("event_cb", &evCb, err) != RdKafka::Conf::CONF_OK) {
            LOG(FATAL) << "KafkaConnector [" << idx << "]: KafkaConnector: Failed to set event_cb: " << err;
        }

        // auto rCb = new RebalanceHandler(this);
        if (config->set("rebalance_cb", &rbCb, err) != RdKafka::Conf::CONF_OK) {
            LOG(FATAL) << "KafkaConnector [" << idx << "]: KafkaConnector: Failed to set rebalance_cb: " << err;
        }
    }
    ~KafkaConnector() = default;

    void start();
    void stop();
    void wait();

    // partitions & rebalances
    std::unordered_map<int, std::shared_ptr<PartitionHandler>> partitionHandlers;
    std::mutex partitionMtx;

    // NOTE: since the whole kafka connector is in a single thread, and this thread is also re-used for librdkafka for
    // the rebalancing purpose. And the partition handlers also belong to the kafka connector. Therefore, there is no
    // need to have locking in place.
    void clearPartitionHandlersNoLocking() { partitionHandlers.clear(); }

    void clearPartitionHandlers() {
        std::lock_guard<std::mutex> lck(partitionMtx);
        clearPartitionHandlersNoLocking();
    }

    void setFreezeTrafficInMemoryFlag() { freeze_traffic_flag = true; }

    void resetFreezeTrafficInMemoryFlag() { freeze_traffic_flag = false; }

    std::list<Rebalance> rebalances;
    static const uint32_t rebalanceSize = 100;

    // current batch
    void clearCurrentBatch();
    std::vector<RdKafka::Message*> currentBatch;
    KafkaConnectorError consumeBatch(size_t batch_size, int64_t batch_timeout); // return false on error

    size_t getId() { return idx; }
    bool isRunning() { return running && !freeze_traffic_flag; }

    // to debug consume/replay issue
    void forceConsumeMode(bool flag) { forced_in_consume_mode = flag; };
    bool isInConsumeMode() { return forced_in_consume_mode; };

    std::string& getStat() { return evCb.get_stat(); }

    // Only export minimum information, so that /api/v1/status becomes lightweight and less time to return.
    nlohmann::json toJson() const;
    // for debugging purpose to allow more status information to be exported with potential more time to complete
    // due to partition handler's locking, compared to toJson();
    void reportStatus(nlohmann::json& status);

    void storePreviousAssigmentInformation();

    // To set and get the metadata version
    int getMetadataVersion() const;
    bool setMetadataVersion(int version);
};
} // namespace kafka
