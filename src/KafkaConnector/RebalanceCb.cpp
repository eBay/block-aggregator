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

#include "RebalanceCb.h"
#include "KafkaConnector.h"
#include "KafkaConnector/Buffer.h"
#include "monitor/metrics_collector.hpp"
#include "common/logging.hpp"

#include <cstddef>
#include <string>
#include <unordered_set>
#include <cassert>

#define REB_ID(id) "RebalanceHandler [" << id << "]: "

namespace kafka {
void RebalanceHandler::rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                                    std::vector<RdKafka::TopicPartition*>& partitions) {
    std::lock_guard<std::mutex> lck(kafkaConnector->partitionMtx);
    auto [var_zone, var_topic] = with_settings([this](SETTINGS s) {
        size_t idx = kafkaConnector->getId();
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });

    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();

    std::chrono::time_point<std::chrono::high_resolution_clock> rebalancing_start_time =
        std::chrono::high_resolution_clock::now();

    if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
        std::string assigned_partitions = "";
        for (auto topic_partition : partitions) {
            assigned_partitions +=
                std::to_string(topic_partition->partition()) + "[" + std::to_string(topic_partition->offset()) + "],";
        }
        LOG(INFO) << REB_ID(kafkaConnector->getId())
                  << " [ASSIGN_PARTITIONS] assigning partitions with offset: " << assigned_partitions;
        if (!createPartitionHandlers(partitions, consumer)) {
            LOG(ERROR) << REB_ID(kafkaConnector->getId()) << "Failed to create partition handlers ";
            return;
        } else {
            LOG_KAFKA(2) << REB_ID(kafkaConnector->getId()) << "Initialized partition handlers";
        }
        auto errcode = consumer->assign(partitions);
        if (errcode == RdKafka::ERR_NO_ERROR) {
            LOG_KAFKA(1) << REB_ID(kafkaConnector->getId()) << "Succeeded to assign " << partitions.size()
                         << " partitions";
        } else {
            LOG(ERROR) << REB_ID(kafkaConnector->getId()) << "Failed to assign " << partitions.size() << " partitions";
        }

        kafkaconnector_metrics->partition_rebalancing_assigned_by_kafka_connector
            ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
            .increment(1);

    } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
        LOG(INFO) << REB_ID(kafkaConnector->getId()) << " [REVOKE_PARTITIONS] revoking partitions";
        auto errcode = consumer->unassign();
        if (errcode == RdKafka::ERR_NO_ERROR) {
            LOG_KAFKA(1) << REB_ID(kafkaConnector->getId()) << "Succeeded to unassign";
            // store partition information to the rebalances
            kafkaConnector->storePreviousAssigmentInformation();
            // Note that no locking method invocation is not necessary because the rebalance_cb from librdkafka runtime
            // and the main kafka connector (in which clear partition handler is also invoked) work in the same thread.
            kafkaConnector->clearPartitionHandlersNoLocking();
            kafkaConnector->clearCurrentBatch();
        } else {
            LOG(ERROR) << REB_ID(kafkaConnector->getId()) << "Failed to unassign due to: " << RdKafka::err2str(errcode);
        }

        kafkaconnector_metrics->partition_rebalancing_assigned_by_kafka_connector
            ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
            .decrement(1);
    } else {
        LOG(ERROR) << REB_ID(kafkaConnector->getId()) << "Got rebalance error:" << RdKafka::err2str(err);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> rebalancing_end_time =
        std::chrono::high_resolution_clock::now();
    uint64_t time_diff =
        std::chrono::duration_cast<std::chrono::microseconds>(rebalancing_end_time.time_since_epoch()).count() -
        std::chrono::duration_cast<std::chrono::microseconds>(rebalancing_start_time.time_since_epoch()).count();
    kafkaconnector_metrics->partition_rebalancing_time->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
        .observe(time_diff);
}

bool RebalanceHandler::createPartitionHandlers(std::vector<RdKafka::TopicPartition*>& new_assignment,
                                               RdKafka::KafkaConsumer* consumer) {
    // LOG_KAFKA(1) << REB_ID(kafkaConnector->getId()) << "Rebalancing is progress. Current size of rebalances history:
    // "
    //              << kafkaConnector->rebalances.size();
    // if (!kafkaConnector->rebalances.empty()) {
    //     LOG_KAFKA(1) << REB_ID(kafkaConnector->getId()) << "Last rebalance info:\n"
    //                  << kafkaConnector->rebalances.back().toString();
    // }
    std::string rebalance_info = "Rebalance in progress. Previous rebalances so far:\n";
    for (auto it = kafkaConnector->rebalances.begin(); it != kafkaConnector->rebalances.end(); it++) {
        rebalance_info += it->toString();
    }

    LOG_KAFKA(1) << REB_ID(kafkaConnector->getId()) << rebalance_info;

    kafkaConnector->rebalances.emplace_back();
    if (kafkaConnector->rebalances.size() > kafkaConnector->rebalanceSize)
        kafkaConnector->rebalances.pop_front();
    CHECK(kafkaConnector->partitionHandlers.empty());
    for (auto topic_partition : new_assignment) {
        LOG_KAFKA(1) << REB_ID(kafkaConnector->getId()) << "Assigning new partition: " << topic_partition->partition();
        auto toppar_to_check = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(toppar_to_check, topic_partition->topic().c_str(),
                                          topic_partition->partition());
        auto committed_toppar = get_committed_metadata(toppar_to_check, consumer->c_ptr());
        rd_kafka_topic_partition_list_destroy(toppar_to_check);
        if (committed_toppar != nullptr) {
            CHECK(committed_toppar->cnt > 0);
            std::string metadata((char*)committed_toppar->elems[0].metadata, committed_toppar->elems[0].metadata_size);
            auto offset = committed_toppar->elems[0].offset;
            kafkaConnector->partitionHandlers.insert(std::make_pair(
                topic_partition->partition(),
                std::make_shared<PartitionHandler>(topic_partition->topic(), topic_partition->partition(), offset,
                                                   metadata, kafkaConnector)));
            rd_kafka_topic_partition_list_destroy(committed_toppar);
        } else {
            LOG_KAFKA(1) << REB_ID(kafkaConnector->getId())
                         << "Unable to fetch commit metadata, so failed to create partition handler for topic: "
                         << topic_partition->topic() << ", partition: " << topic_partition->partition();
            return false;
        }
    }
    return true;
}

rd_kafka_topic_partition_list_t*
RebalanceHandler::get_committed_metadata(const rd_kafka_topic_partition_list_t* toppar_to_check, rd_kafka_t* rk) {
    rd_kafka_resp_err_t err;
    rd_kafka_topic_partition_list_t* committed_toppar;

    committed_toppar = rd_kafka_topic_partition_list_copy(toppar_to_check);
    for (int retry = 0; retry < MAX_NUMBER_OF_KAFKA_RETRIES; retry++) {
        if (retry > 0) {
            int delay_time = std::min(retry * KAFKA_RETRY_DELAY_MS, KAFKA_RETRY_MAX_DELAY_MS);
            LOG(ERROR) << "Retry to get metadata from Kafka after " << delay_time << " (ms)";
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_time));
        }
        err = rd_kafka_committed(rk, committed_toppar, 5000);
        if (!err) {
            return committed_toppar;
        } else {
            LOG(ERROR) << "Failed to get committed metadata: " << rd_kafka_err2str(err);
        }
    }
    LOG(ERROR) << "Number of retries to get metadata exceeded the maximum number of retries. Rerturning failure now.";
    rd_kafka_topic_partition_list_destroy(committed_toppar);
    return nullptr;
}
} // namespace kafka
