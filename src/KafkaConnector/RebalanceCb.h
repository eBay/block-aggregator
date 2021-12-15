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
#include "PartitionHandler.h"

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

#include <unordered_map>

namespace kafka {
class KafkaConnector;
/**
 * Handles the partition reassigment.
 */
class RebalanceHandler : public RdKafka::RebalanceCb {
  public:
    static constexpr int MAX_NUMBER_OF_KAFKA_RETRIES = 10;
    static constexpr int KAFKA_RETRY_MAX_DELAY_MS = 500;
    static constexpr int KAFKA_RETRY_DELAY_MS = 100;
    RebalanceHandler(KafkaConnector* kafkaConnector_) : kafkaConnector(kafkaConnector_) {}

    void rebalance_cb(RdKafka::KafkaConsumer* consumer, RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition*>& partitions) override;

    static rd_kafka_topic_partition_list_t*
    get_committed_metadata(const rd_kafka_topic_partition_list_t* toppar_to_check, rd_kafka_t* rk);

  private:
    bool createPartitionHandlers(std::vector<RdKafka::TopicPartition*>& new_assignment,
                                 RdKafka::KafkaConsumer* consumer);
    KafkaConnector* kafkaConnector;
};
} // namespace kafka
