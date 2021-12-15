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

#include <monitor_base/metrics.hpp>
#include <monitor_base/metrics_factory.hpp>

namespace nuclm {

struct KafkaConnectorMetrics {
    static const std::string TotalActiveKafkaConnectors_Metric_Name;
    static const std::string NumberOfPartitionsInKafkaConnector_Metric_Name;
    static const std::string TotalNumberOfKafkaBatchedMessagesProcessed_Metric_Name;
    static const std::string TotalBytesOfKafkaBatchedMessagesProcessed_Metric_Name; // as a counter

    // NOTE: need to understand the following metrics why received messages should be ignored?
    static const std::string TotalNumberOfKafkaBatchedMessagesIgnored_Metric_Name;
    static const std::string TotalBytesOfKafkaBatchedMessagesIgnored_Metric_Name;

    static const std::string ProcessingTimeForKafkaBatchedMessagesReceived_Metric_Name;
    static const std::string BytesFromKafkaBatchedMessagesProcessed_Metric_Name; // as a histogram

    static const std::string CommitTimeForOffsetCommitByKafkaConnectors_Metric_Name;
    static const std::string TaskCompletionWaitTimeByKafkaConnectors_Metric_Name;
    static const std::string AllTasksWaitTimeByKafkaConnectors_Metric_Name;

    static const std::string TaskBlockWaitFailedInKafkaConnectors_Metric_Name;

    static const std::string TotalNumberOfKafkaMessagesReplayed_Metric_Name;
    static const std::string BytesFromKafkaMessagesReplayed_Metric_Name;
    static const std::string TotalNumberOfKafkaMessagesIgnoredForReplay_Metric_Name;

    static const std::string CommitFailedByKafkaConnectors_Metric_Name;

    // keep track of the current offset processed and committed minimum offset by the Kafka Connector
    static const std::string CurrentOffsetBeingProcessedByKafkaConnector_Metric_Name;
    static const std::string CommittedOffsetByKafkaConnector_Metric_Name;

    // kafka partition re-balancing
    static const std::string PartitionRebalancingAssignedAtKafkaConnector_Metric_Name;
    // kafka partition re-balancing time
    static const std::string PartitionRebalancingTimeAtKafkaConnector_Metric_Name;
    // keep track of how many times the kafka connector main-loop being restarted
    static const std::string TotalNumberOfKafkaConnectorMainLoopRestarted_Metric_Name;

    // Warning-associated log messages in source code to be turned into metrics to monitor unexpected behaviors
    static const std::string KafkaOffsetWentBackward_Metric_Name;
    static const std::string KafkaOffsetReset_Metric_Name;
    static const std::string KafkaOffsetLargerThanExpected_Metric_Name;
    static const std::string MetadataCommittedByDifferentConnector_Metric_Name;
    static const std::string KafkaConnectorReplayBeingSkipped_Metric_Name;
    static const std::string KafkaConnectorAbnormalMessageReceived_Metric_Name;
    static const std::string KafkaConnectorFlushTaskNotFinished_Metric_Name;
    static const std::string KafkaBrokerMessageRewound_Metric_Name;
    static const std::string KafkaMessageHeaderInWrongFormat_Metric_Name;
    static const std::string PartitionHandlerStatusReferenceWrong_Metric_Name;
    static const std::string KafkaMetadataCommitFinallyFailed_Metric_Name;

    // Freeze/resume traffic related metrics
    static const std::string KafkaFreezeTrafficFlagReceived_Metric_Name; // on the command
    static const std::string
        KafkaFreeTrafficCurrentStatus_Metric_Name; // on the actual status after executing the command

    // Librdkafka related metrics
    static const std::string Librdkafka_Time_Metric_Name;
    static const std::string Librdkafka_ReplyQ_Metric_Name;
    static const std::string Librdkafka_MsgCnt_Metric_Name;
    static const std::string Librdkafka_MsgBytes_Metric_Name;
    static const std::string Librdkafka_Rx_Metric_Name;
    static const std::string Librdkafka_RxBytes_Metric_Name;
    static const std::string Librdkafka_RxMsgs_Metric_Name;
    static const std::string Librdkafka_RxMsgBytes_Metric_Name;
    static const std::string Librdkafka_CgrpRebalanceCnt_Metric_Name;
    static const std::string Librdkafka_CgrpStateUp_Metric_Name;
    static const std::string Librdkafka_CgrpAssignmentCount_Metric_Name;
    static const std::string Librdkafka_CgrpRebalanceAge_Metric_Name;

    static const std::string Librdkafka_TopicTotalConsumerLag_Metric_Name;
    static const std::string Librdkafka_TopicPartitionConsumerLag_Metric_Name;
    static const std::string Librdkafka_TopicPartitionLsOffset_Metric_Name;
    static const std::string Librdkafka_TopicPartitionAppOffset_Metric_Name;
    static const std::string Librdkafka_TopicPartitionCommittedOffset_Metric_Name;
    static const std::string Librdkafka_TopicPartitionLoOffset_Metric_Name;
    static const std::string Librdkafka_TopicPartitionHiOffset_Metric_Name;
    static const std::string Librdkafka_TopicPartitionMsgsInflight_Metric_Name;
    static const std::string Librdkafka_TopicPartitionRxMsgs_Metric_Name;
    static const std::string Librdkafka_TopicPartitionRxBytes_Metric_Name;
    static const std::string Librdkafka_TopicPartitionRxVerDrops_Metric_Name;
    static const std::string Librdkafka_TopicPartitionFetchQCnt_Metric_Name;
    static const std::string Librdkafka_TopicPartitionFetchQSize_Metric_Name;

    monitor::MetricFamily<monitor::_gauge>* kafka_connectors_total_number;
    monitor::MetricFamily<monitor::_gauge>* partitions_in_kafka_connectors;
    monitor::MetricFamily<monitor::_counter>* batched_messages_processed_by_kafka_connectors_total;
    monitor::MetricFamily<monitor::_counter>* batched_messages_processed_by_kafka_connectors_bytes_total;

    // NOTE: need to understand the following metrics why received messages should be ignored?
    monitor::MetricFamily<monitor::_counter>* batched_messages_ignored_by_kafka_connectors_total;
    monitor::MetricFamily<monitor::_counter>* batched_messages_ignored_by_kafka_connectors_bytes_total;

    monitor::MetricFamily<monitor::_histogram>* processing_time_for_batched_messages_by_kafka_connectors;
    monitor::MetricFamily<monitor::_histogram>* bytes_from_batched_messages_by_kafka_connectors;

    monitor::MetricFamily<monitor::_histogram>* commit_time_for_offset_commit_by_kafka_connectors;
    monitor::MetricFamily<monitor::_histogram>* task_completion_waittime_by_kafka_connectors;

    monitor::MetricFamily<monitor::_counter>* task_block_wait_failed_in_kafka_connectors;

    monitor::MetricFamily<monitor::_histogram>* all_tasks_completion_waittime_by_kafka_connectors;

    monitor::MetricFamily<monitor::_counter>* batched_messages_replayed_by_kafka_connectors_total;
    monitor::MetricFamily<monitor::_counter>* batched_messages_replayed_by_kafka_connectors_bytes_total;
    monitor::MetricFamily<monitor::_counter>* batched_messages_replay_ignored_by_kafka_connectors_total;

    monitor::MetricFamily<monitor::_counter>* commit_offset_by_kafka_connectors_failed_total;

    // keep track of the current offset processed and committed minimum offset by the Kafka Connector
    monitor::MetricFamily<monitor::_gauge>* current_offset_being_processed_by_kafka_connector;
    monitor::MetricFamily<monitor::_gauge>* committed_offset_by_kafka_connector;

    // kafka partition re-balancing (including, assigned and un-assigned)
    monitor::MetricFamily<monitor::_gauge>* partition_rebalancing_assigned_by_kafka_connector;
    // kafka partition re-balancing time (including, assigned and un-assigned)
    monitor::MetricFamily<monitor::_histogram>* partition_rebalancing_time;
    // keep track of how many times the kafka-connector main-loop restarted
    monitor::MetricFamily<monitor::_counter>* mainloop_restarted_at_kafka_connector_total;

    // keep track of how many times the aggregator received a message with an offset less than that of the previous
    // message
    monitor::MetricFamily<monitor::_counter>* kafka_offset_went_backward_total;
    // keep track of how many times the aggregator received a message with an offset 0 (i.e. offset reset)
    monitor::MetricFamily<monitor::_counter>* kafka_offset_reset_total;
    // keep track of how many times the aggregator received a message with an offset larger than expected
    monitor::MetricFamily<monitor::_counter>* kafka_offset_larger_than_expected_total;
    // keep track of how many times the metadata committed is not by the connector owning it
    monitor::MetricFamily<monitor::_counter>* metadata_committed_by_different_connector_total;
    // keep track of how many times the deterministic replay gets skipped by kafka connector
    monitor::MetricFamily<monitor::_counter>* kafka_connector_replay_being_skipped_total;
    // keep track of how many times abnormal messages being received earlier than committed offset by kafka connector
    monitor::MetricFamily<monitor::_counter>* kafka_connector_abnormal_message_received_total;
    // keep track of how many times flush tasks should have been finished but not.
    monitor::MetricFamily<monitor::_counter>* kafka_connector_flush_task_not_finished_total;
    // keep track of how many times kafka brokers have rewound kafka messages
    monitor::MetricFamily<monitor::_counter>* kafka_broker_message_rewound_total;
    // keep track of how many kafka messages with wrong headers
    monitor::MetricFamily<monitor::_counter>* kafka_message_in_wrong_format_total;
    // keep track of the situation when reference = -1 when buffer is not empty
    monitor::MetricFamily<monitor::_counter>* partition_handler_reference_wrong_total;
    // keep track of the kafka connector's metadata commit to Kafka encounters final failure (after many retries)
    monitor::MetricFamily<monitor::_counter>* kafka_commit_metadata_finally_failed_total;

    // Freeze/resume traffic related metrics
    monitor::MetricFamily<monitor::_gauge>* kafka_connector_traffic_freeze_command_flag_received;
    monitor::MetricFamily<monitor::_gauge>* kafka_connector_traffic_freeze_status;

    // Librdkafka related metrics
    monitor::MetricFamily<monitor::_gauge>* librdkafka_time;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_replyq;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_msg_cnt;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_msg_bytes;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_rx;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_rx_bytes;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_rxmsgs;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_rxmsg_bytes;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_cgrp_rebalance_cnt;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_cgrp_state_up;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_cgrp_assignment_size;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_cgrp_rebalanace_age;

    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_consumer_lag;

    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_consumer_lag;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_ls_offset;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_app_offset;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_committed_offset;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_lo_offset;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_hi_offset;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_msgs_inflight;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_rxmsgs;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_rxbytes;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_rx_ver_drops;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_fetchq_cnt;
    monitor::MetricFamily<monitor::_gauge>* librdkafka_topic_partition_fetchq_size;

    KafkaConnectorMetrics(monitor::NuDataMetricsFactory& factory);
};

} // namespace nuclm
