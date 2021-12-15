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

#include "kafka_connector_metrics.hpp"
#include "glog/logging.h"

namespace nuclm {

const std::string KafkaConnectorMetrics::TotalActiveKafkaConnectors_Metric_Name =
    "nucolumnar_aggregator_active_kafka_connectors";
const std::string KafkaConnectorMetrics::NumberOfPartitionsInKafkaConnector_Metric_Name =
    "nucolumnar_aggregator_partition_numbers_in_kafka_connector";
const std::string KafkaConnectorMetrics::TotalNumberOfKafkaBatchedMessagesProcessed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_processed_in_kafka_connector_total";
const std::string KafkaConnectorMetrics::TotalBytesOfKafkaBatchedMessagesProcessed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_processed_bytes_in_kafka_connector_total";

// NOTE: need to understand the following metrics why received messages should be ignored?
const std::string KafkaConnectorMetrics::TotalNumberOfKafkaBatchedMessagesIgnored_Metric_Name =
    "nucolumnar_aggregator_batched_messages_ignored_in_kafka_connector_total";
const std::string KafkaConnectorMetrics::TotalBytesOfKafkaBatchedMessagesIgnored_Metric_Name =
    "nucolumnar_aggregator_batched_messages_ignored_bytes_in_kafka_connector_total";

const std::string KafkaConnectorMetrics::ProcessingTimeForKafkaBatchedMessagesReceived_Metric_Name =
    "nucolumnar_aggregator_batched_messages_processing_in_kafka_connector_microseconds";
const std::string KafkaConnectorMetrics::BytesFromKafkaBatchedMessagesProcessed_Metric_Name =
    "nucolumnar_aggregator_bytes_from_batched_messages_in_kafka_connector";

const std::string KafkaConnectorMetrics::CommitTimeForOffsetCommitByKafkaConnectors_Metric_Name =
    "nucolumnar_aggregator_offset_commit_in_kafka_connector_in_microseconds";
const std::string KafkaConnectorMetrics::TaskCompletionWaitTimeByKafkaConnectors_Metric_Name =
    "nucolumnar_aggregator_task_completion_waittime_in_kafka_connector_in_microseconds";
const std::string KafkaConnectorMetrics::AllTasksWaitTimeByKafkaConnectors_Metric_Name =
    "nucolumnar_aggregator_all_tasks_completion_waittime_in_kafka_connector_in_microseconds";

const std::string KafkaConnectorMetrics::TaskBlockWaitFailedInKafkaConnectors_Metric_Name =
    "nucolumnar_aggregator_block_wait_failed_in_kafka_connector_in_total";

const std::string KafkaConnectorMetrics::TotalNumberOfKafkaMessagesReplayed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_replayed_in_kafka_connector_total";
const std::string KafkaConnectorMetrics::BytesFromKafkaMessagesReplayed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_replayed_bytes_in_kafka_connector_total";
const std::string KafkaConnectorMetrics::TotalNumberOfKafkaMessagesIgnoredForReplay_Metric_Name =
    "nucolumnar_aggregator_batched_messages_replay_ignored_in_kafka_connector_total";

const std::string KafkaConnectorMetrics::CommitFailedByKafkaConnectors_Metric_Name =
    "nucolumnar_aggregator_offset_commit_failed_in_kafka_connector_total";

// keep track of current offset processed and committed minimum offset by the Kafka Connector
const std::string KafkaConnectorMetrics::CurrentOffsetBeingProcessedByKafkaConnector_Metric_Name =
    "nucolumnar_aggregator_offset_processed_by_kafka_connector";
const std::string KafkaConnectorMetrics::CommittedOffsetByKafkaConnector_Metric_Name =
    "nucolumnar_aggregator_committed_offset_by_kafka_connector";

// partition rebalancing
const std::string KafkaConnectorMetrics::PartitionRebalancingAssignedAtKafkaConnector_Metric_Name =
    "nucolumnar_aggregator_partition_rebalance_assigned_at_kafka_connector";
// partition rebalancing time
const std::string KafkaConnectorMetrics::PartitionRebalancingTimeAtKafkaConnector_Metric_Name =
    "nucolumnar_aggregator_partition_rebalance_time_at_kafka_connector";

// keep track of how many times the kafka connector main-loop being restarted
const std::string KafkaConnectorMetrics::TotalNumberOfKafkaConnectorMainLoopRestarted_Metric_Name =
    "nucolumnar_aggregator_kafka_connector_main_loop_restarted_total";

// to keep track of how many times kafka offset went backward
const std::string KafkaConnectorMetrics::KafkaOffsetWentBackward_Metric_Name =
    "nucolumnar_aggregator_kafka_offset_went_backward_total";
// to keep track of how many times kafka offset reset
const std::string KafkaConnectorMetrics::KafkaOffsetReset_Metric_Name =
    "nucolumnar_aggregator_kafka_offset_reset_total";
// to keep track of how many times kafka offset was larger than expected
const std::string KafkaConnectorMetrics::KafkaOffsetLargerThanExpected_Metric_Name =
    "nucolumnar_aggregator_kafka_larger_than_expected_total";
// to keep track of how many times the metadata committed is not by the connector owning it
const std::string KafkaConnectorMetrics::MetadataCommittedByDifferentConnector_Metric_Name =
    "nucolumnar_aggregator_metadata_committed_by_different_connector_total";
// keep track of how many times the deterministic replay gets skipped by kafka connector
const std::string KafkaConnectorMetrics::KafkaConnectorReplayBeingSkipped_Metric_Name =
    "nucolumnar_aggregator_kafkaconnector_replay_skipped_total";
// keep track of how many times abnormal messages being received earlier than committed offset by kafka connector
const std::string KafkaConnectorMetrics::KafkaConnectorAbnormalMessageReceived_Metric_Name =
    "nucolumnar_aggregator_kafkaconnnector_abnormal_message_received_total";
// keep track of how many times flush tasks should have been finished but not.
const std::string KafkaConnectorMetrics::KafkaConnectorFlushTaskNotFinished_Metric_Name =
    "nucolumnar_aggregator_kafkaconnector_flush_task_not_finished_total";
// keep track of how many times kafka brokers have rewound the kafka message
const std::string KafkaConnectorMetrics::KafkaBrokerMessageRewound_Metric_Name =
    "nucolumnar_aggregator_kafkaconnector_message_rewound_total";
// keep track of how many kafka messages with wrong headers
const std::string KafkaConnectorMetrics::KafkaMessageHeaderInWrongFormat_Metric_Name =
    "nucolumnar_aggregator_kafka_message_in_wrong_format_total";
// keep track of the situation when reference = -1 when buffer is not empty
const std::string KafkaConnectorMetrics::PartitionHandlerStatusReferenceWrong_Metric_Name =
    "nucolumnar_aggregator_partition_handler_status_reference_wrong_total";
// keep track of the kafka connector's metadata commit to Kafka encounters final failure (after many retries)
const std::string KafkaConnectorMetrics::KafkaMetadataCommitFinallyFailed_Metric_Name =
    "nucolumnar_aggregator_kafkaconnector_metadata_commit_finally_failed_total";

// Freeze/resume traffic related metrics
const std::string KafkaConnectorMetrics::KafkaFreezeTrafficFlagReceived_Metric_Name =
    "nucolumnar_aggregator_kafakconnector_freeze_traffic_command_received";
const std::string KafkaConnectorMetrics::KafkaFreeTrafficCurrentStatus_Metric_Name =
    "nucolumnar_aggregator_kafakconnector_freeze_traffic_status";

// Librdkafka related metrics
const std::string KafkaConnectorMetrics::Librdkafka_Time_Metric_Name = "nucolumnar_kafka_consumer_stat_time";
const std::string KafkaConnectorMetrics::Librdkafka_ReplyQ_Metric_Name = "nucolumnar_kafka_consumer_ops_queue_count";
const std::string KafkaConnectorMetrics::Librdkafka_MsgCnt_Metric_Name = "nucolumnar_kafka_consumer_received_msg_count";
const std::string KafkaConnectorMetrics::Librdkafka_MsgBytes_Metric_Name =
    "nucolumnar_kafka_consumer_received_msg_bytes";
const std::string KafkaConnectorMetrics::Librdkafka_Rx_Metric_Name = "nucolumnar_kafka_consumer_received_count";
const std::string KafkaConnectorMetrics::Librdkafka_RxBytes_Metric_Name = "nucolumnar_kafka_consumer_received_bytes";
const std::string KafkaConnectorMetrics::Librdkafka_RxMsgs_Metric_Name = "nucolumnar_kafka_consumer_received_msg_count";
const std::string KafkaConnectorMetrics::Librdkafka_RxMsgBytes_Metric_Name =
    "nucolumnar_kafka_consumer_received_msg_bytes";
const std::string KafkaConnectorMetrics::Librdkafka_CgrpRebalanceCnt_Metric_Name =
    "nucolumnar_kafka_consumer_group_rebalance_count";
const std::string KafkaConnectorMetrics::Librdkafka_CgrpStateUp_Metric_Name =
    "nucolumnar_kafka_consumer_group_state_up";
const std::string KafkaConnectorMetrics::Librdkafka_CgrpAssignmentCount_Metric_Name =
    "nucolumnar_kafka_consumer_group_assignment_count";
const std::string KafkaConnectorMetrics::Librdkafka_CgrpRebalanceAge_Metric_Name =
    "nucolumnar_kafka_consumer_group_rebalance_age";

const std::string KafkaConnectorMetrics::Librdkafka_TopicTotalConsumerLag_Metric_Name =
    "nucolumnar_kafka_consumer_topic_consumer_lag";

const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionConsumerLag_Metric_Name =
    "nucolumnar_kafka_consumer_partition_consumer_lag";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionLsOffset_Metric_Name =
    "nucolumnar_kafka_consumer_partition_last_stable_offset";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionAppOffset_Metric_Name =
    "nucolumnar_kafka_consumer_partition_app_offset";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionCommittedOffset_Metric_Name =
    "nucolumnar_kafka_consumer_partition_committed_offset";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionLoOffset_Metric_Name =
    "nucolumnar_kafka_consumer_partition_low_watermark_offset";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionHiOffset_Metric_Name =
    "nucolumnar_kafka_consumer_partition_high_watermark_offset";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionMsgsInflight_Metric_Name =
    "nucolumnar_kafka_consumer_partition_inflight_msgs_count";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionRxMsgs_Metric_Name =
    "nucolumnar_kafka_consumer_partition_received_msgs_count";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionRxBytes_Metric_Name =
    "nucolumnar_kafka_consumer_partition_received_msg_bytes";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionRxVerDrops_Metric_Name =
    "nucolumnar_kafka_consumer_partition_dropped_outdated_msg";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionFetchQCnt_Metric_Name =
    "nucolumnar_kafka_consumer_partition_fetch_queue_count";
const std::string KafkaConnectorMetrics::Librdkafka_TopicPartitionFetchQSize_Metric_Name =
    "nucolumnar_kafka_consumer_partition_fetch_queue_bytes";

KafkaConnectorMetrics::KafkaConnectorMetrics(monitor::NuDataMetricsFactory& factory) {
    // metric: TotalActiveKafkaConnector_Metric_Name
    kafka_connectors_total_number = &factory.registerMetric<monitor::_gauge>(
        TotalActiveKafkaConnectors_Metric_Name, "nucolumnar aggregator active number of kafka connectors",
        {"on_topic", "on_zone"});

    // metric: NumberOfPartitionsInKafkaConnector_Metric_Name
    partitions_in_kafka_connectors = &factory.registerMetric<monitor::_gauge>(
        NumberOfPartitionsInKafkaConnector_Metric_Name,
        "nucolumnar aggregator number of partitions in the kafka connector", {"on_topic", "on_zone"});

    // metric: TotalNumberOfKafkaBatchedMessagesReceived_Metric_Name
    batched_messages_processed_by_kafka_connectors_total = &factory.registerMetric<monitor::_counter>(
        TotalNumberOfKafkaBatchedMessagesProcessed_Metric_Name,
        "nucolumnar aggregator total number of batched messages received by the kafka connector",
        {"on_topic", "on_zone"});

    batched_messages_processed_by_kafka_connectors_bytes_total = &factory.registerMetric<monitor::_counter>(
        TotalBytesOfKafkaBatchedMessagesProcessed_Metric_Name,
        "nucolumnar aggregator batched messages processed by the kafka connector in total bytes",
        {"on_topic", "on_zone"});

    // metric: TotalNumberOfKafkaBatchedMessagesReceived_Metric_Name
    batched_messages_ignored_by_kafka_connectors_total = &factory.registerMetric<monitor::_counter>(
        TotalNumberOfKafkaBatchedMessagesIgnored_Metric_Name,
        "nucolumnar aggregator total number of batched messages received but then ignored by the kafka connector",
        {"on_topic", "on_zone"});

    // metric: TotalBytesOfKafkaBatchedMessagesIgnored_Metric_Name
    batched_messages_ignored_by_kafka_connectors_bytes_total = &factory.registerMetric<monitor::_counter>(
        TotalBytesOfKafkaBatchedMessagesIgnored_Metric_Name,
        "nucolumnar aggregator batched messages received but then ignored by the kafka connector in bytes",
        {"on_topic", "on_zone"});

    // metric: ProcessingTimeForKafkaBatchedMessagesReceived_Metric_Name
    processing_time_for_batched_messages_by_kafka_connectors = &factory.registerMetric<monitor::_histogram>(
        ProcessingTimeForKafkaBatchedMessagesReceived_Metric_Name,
        "nucolumnar aggregator processing time in microseconds for batched messages received by the kafka connector in "
        "one processing loop",
        {"on_topic", "on_zone"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: BytesFromKakfaBatchedMessagesReceived_Metric_Name
    bytes_from_batched_messages_by_kafka_connectors = &factory.registerMetric<monitor::_histogram>(
        BytesFromKafkaBatchedMessagesProcessed_Metric_Name,
        "nucolumnar aggregator bytes from batched messages processed by the kafka connector", {"on_topic", "on_zone"},
        monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: CommitTimeForOffsetCommitByKafakConnectors_Metric_Name
    commit_time_for_offset_commit_by_kafka_connectors = &factory.registerMetric<monitor::_histogram>(
        CommitTimeForOffsetCommitByKafkaConnectors_Metric_Name,
        "nucolumnar aggregator time spent to commit offset to kafka by the kafka connector", {"on_topic", "on_zone"},
        monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: TaskCompletionWaitTimeByKafkaConnectors_Metric_Name
    task_completion_waittime_by_kafka_connectors = &factory.registerMetric<monitor::_histogram>(
        TaskCompletionWaitTimeByKafkaConnectors_Metric_Name,
        "nucolumnar aggregator task completion wait time for block persistence to backend by the kafka connector",
        {"on_topic", "on_zone"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: AllTasksWaitTimeByKafkaConnectors_Metric_Name
    all_tasks_completion_waittime_by_kafka_connectors = &factory.registerMetric<monitor::_histogram>(
        AllTasksWaitTimeByKafkaConnectors_Metric_Name,
        "nucolumnar aggregator all task completion wait time for block persistence to backend by the kafka connector",
        {"on_topic", "on_zone"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: TaskBlockWaitFailedInKafkaConnectors_Metric_Name
    task_block_wait_failed_in_kafka_connectors = &factory.registerMetric<monitor::_counter>(
        TaskBlockWaitFailedInKafkaConnectors_Metric_Name,
        "nucolumnar aggregator block waited in tasks by the kafka connector", {"on_topic", "on_zone"});

    // metric: TotalNumberOfKafkaMessagesReplayed_Metric_Name
    batched_messages_replayed_by_kafka_connectors_total = &factory.registerMetric<monitor::_counter>(
        TotalNumberOfKafkaMessagesReplayed_Metric_Name,
        "nucolumnar aggregator batched messages being replayed by the kafka connector", {"on_topic", "on_zone"});

    // metric: BytesFromKafkaMessagesReplayed_Metric_Name
    batched_messages_replayed_by_kafka_connectors_bytes_total = &factory.registerMetric<monitor::_counter>(
        BytesFromKafkaMessagesReplayed_Metric_Name,
        "nucolumnar aggregator batched messages being replayed in bytes by the kafka connector",
        {"on_topic", "on_zone"});

    // metric: TotalNumberOfKafkaMessagesIgnoredForReplay_Metric_Name
    // NOTE:  at this time, no code instrumented on the following metrics, as not sure how to capture this metric in the
    // source code.
    batched_messages_replay_ignored_by_kafka_connectors_total = &factory.registerMetric<monitor::_counter>(
        TotalNumberOfKafkaMessagesIgnoredForReplay_Metric_Name,
        "nucolumnar aggregator batched messages to be replayed but then ignored by the kafka connector",
        {"on_topic", "on_zone"});

    // metric: CommitFailedByKafkaConnectors_Metric_Name
    commit_offset_by_kafka_connectors_failed_total = &factory.registerMetric<monitor::_counter>(
        CommitFailedByKafkaConnectors_Metric_Name, "nucolumnar aggregator offset commit failed by the kafka connector",
        {"on_topic", "on_zone", "error_code"});

    // metric: CurrentOffsetBeingProcessedByKafkaConnector_Metric_Name
    current_offset_being_processed_by_kafka_connector = &factory.registerMetric<monitor::_gauge>(
        CurrentOffsetBeingProcessedByKafkaConnector_Metric_Name,
        "nucolumnar aggregator offset currently being processed by kafka connector", {"on_topic", "on_zone"});
    committed_offset_by_kafka_connector = &factory.registerMetric<monitor::_gauge>(
        CommittedOffsetByKafkaConnector_Metric_Name,
        "nucolumnar aggregator minimum offset currently being committed by kafka connector", {"on_topic", "on_zone"});

    // metric: PartitionRebalancingAtKafkaConnector_Metric_Name
    partition_rebalancing_assigned_by_kafka_connector = &factory.registerMetric<monitor::_gauge>(
        PartitionRebalancingAssignedAtKafkaConnector_Metric_Name,
        "nucolumnar aggregator partition rebalancing assigned happened at kafka connector", {"on_topic", "on_zone"});

    // metric: PartitionRebalancingTimeAtKafkaConnector_Metric_Name
    partition_rebalancing_time = &factory.registerMetric<monitor::_histogram>(
        PartitionRebalancingTimeAtKafkaConnector_Metric_Name,
        "nucolumnar aggregator partition rebalancing time at kafka connector", {"on_topic", "on_zone"},
        monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: TotalNumberOfKafkaConnectorMainLoopRestarted_Metric_Name
    mainloop_restarted_at_kafka_connector_total = &factory.registerMetric<monitor::_counter>(
        TotalNumberOfKafkaConnectorMainLoopRestarted_Metric_Name,
        "nucolumnar aggregator total number of the main-loop at kafka connector being restarted",
        {"on_topic", "on_zone"});
    // metric: KafkaOffsetWentBackward_Metric_Name
    kafka_offset_went_backward_total = &factory.registerMetric<monitor::_counter>(
        KafkaOffsetWentBackward_Metric_Name, "nucolumnar aggregator total number of time kafka offset went backward",
        {"on_topic", "partition", "on_zone"});
    // metric: KafkaOffsetReset_Metric_Name
    kafka_offset_reset_total = &factory.registerMetric<monitor::_counter>(
        KafkaOffsetReset_Metric_Name, "nucolumnar aggregator total number of time kafka offset reset",
        {"on_topic", "partition", "on_zone"});
    // metric: KafkaOffsetLargerThanExpected_Metric_Name
    kafka_offset_larger_than_expected_total = &factory.registerMetric<monitor::_counter>(
        KafkaOffsetLargerThanExpected_Metric_Name,
        "nucolumnar aggregator total number of time kafka offset is larger than what is expected",
        {"on_topic", "partition", "on_zone"});
    // metric: MetadataCommittedByDifferentConnector_Metric_Name
    metadata_committed_by_different_connector_total = &factory.registerMetric<monitor::_counter>(
        MetadataCommittedByDifferentConnector_Metric_Name,
        "nucolumnar aggregator total number of mismatches on metadata in terms of kafka connector that owns it and "
        "kafka connector that wrote it",
        {"on_topic", "on_zone"});
    // metric: KafkaConnectorReplayBeing_Skipped_Metric_Name
    kafka_connector_replay_being_skipped_total = &factory.registerMetric<monitor::_counter>(
        KafkaConnectorReplayBeingSkipped_Metric_Name,
        "nucolumnar aggregator total number of replays skipped by kafka connector", {"on_topic", "on_zone"});
    // metric: KafkaConnectorAbnormalMessagesReceived_Metric_Name
    kafka_connector_abnormal_message_received_total =
        &factory.registerMetric<monitor::_counter>(KafkaConnectorAbnormalMessageReceived_Metric_Name,
                                                   "nucolumnar aggregator total number of abnormal messages received "
                                                   "by kafka connector earlier than committed offset",
                                                   {"on_topic", "on_zone"});
    // metric: KafkaConnectorFlushTaskNotFinished_Metric_Name
    kafka_connector_flush_task_not_finished_total = &factory.registerMetric<monitor::_counter>(
        KafkaConnectorFlushTaskNotFinished_Metric_Name,
        "nucolumnar aggregator total number of flush tasks not finished", {"on_topic", "on_zone", "table"});
    // metric:  KafkaBrokerMessageRewound_Metric_Name
    kafka_broker_message_rewound_total = &factory.registerMetric<monitor::_counter>(
        KafkaBrokerMessageRewound_Metric_Name, "nucolumnar aggregator total number of kafka broker message rewound",
        {"on_topic", "on_zone", "table"});
    // metric: KafkaMessageHeaderInWrongFormat_Metric_Name
    kafka_message_in_wrong_format_total = &factory.registerMetric<monitor::_counter>(
        KafkaMessageHeaderInWrongFormat_Metric_Name,
        "nucolumnar aggregator total number of kafka messages received in bad format", {"on_topic", "on_zone"});
    // metric: PartitionHandlerStatusReferenceWrong_Metric_Name
    partition_handler_reference_wrong_total = &factory.registerMetric<monitor::_counter>(
        PartitionHandlerStatusReferenceWrong_Metric_Name,
        "nucolumnar aggregator kafkaconnector's partition handler status reference tracking is wrong",
        {"on_topic", "on_zone"});
    // metric: KafkaMetadataCommitFinallyFailed_Metric_Name
    kafka_commit_metadata_finally_failed_total = &factory.registerMetric<monitor::_counter>(
        KafkaMetadataCommitFinallyFailed_Metric_Name,
        "nucolumnar aggregator kafkaconnector metadata commit to kafka finally failed", {"on_topic", "on_zone"});

    // metric: KafkaFreezeTrafficFlagReceived_Metric_Name
    kafka_connector_traffic_freeze_command_flag_received = &factory.registerMetric<monitor::_gauge>(
        KafkaFreezeTrafficFlagReceived_Metric_Name,
        "nucolumnar aggregator kafkaconnector freeze traffic flag being received by connector",
        {"on_topic", "on_zone"});

    // metric: KafkaFreeTrafficCurrentStatus_Metric_Name
    kafka_connector_traffic_freeze_status = &factory.registerMetric<monitor::_gauge>(
        KafkaFreeTrafficCurrentStatus_Metric_Name,
        "nucolumnar aggregator kafkaconnector current freeze traffic status reported by connector",
        {"on_topic", "on_zone"});

    // Kafka consumer (librdkafka) Metrics. The labels used should be the same as what other metrics use, that is,
    // "on_topic" and "on_zone". If we use "zone", the same label will be overridden by the prometheus server when
    // it inject the pod-level label into the metric.

    librdkafka_time = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_Time_Metric_Name, "last update time in seconds since the epoch", {"on_topic", "on_zone"});

    librdkafka_replyq = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_ReplyQ_Metric_Name,
        "number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()",
        {"on_topic", "on_zone"});
    librdkafka_msg_cnt = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_MsgCnt_Metric_Name, "current number of messages in producer queues", {"on_topic", "on_zone"});
    librdkafka_msg_bytes = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_MsgBytes_Metric_Name, "current total size of messages in producer queues", {"on_topic", "on_zone"});
    librdkafka_rx = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_Rx_Metric_Name, "total number of responses received from kafka brokers", {"on_topic", "on_zone"});
    librdkafka_rx_bytes = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_RxBytes_Metric_Name, "total number of bytes received from kafka brokers", {"on_topic", "on_zone"});
    librdkafka_rxmsgs = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_RxMsgs_Metric_Name,
        "total number of messages consumed, not including ignored messages (due to offset, etc), from kafka brokers.",
        {"on_topic", "on_zone"});
    librdkafka_rxmsg_bytes = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_RxMsgBytes_Metric_Name,
        "total number of message bytes (including framing) received from Kafka brokers", {"on_topic", "on_zone"});
    librdkafka_cgrp_rebalance_cnt = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_CgrpRebalanceCnt_Metric_Name, "total number of rebalances (assign or revoke).",
        {"on_topic", "on_zone"});
    librdkafka_cgrp_state_up = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_CgrpStateUp_Metric_Name, "local consumer group handler's state is up.", {"on_topic", "on_zone"});
    librdkafka_cgrp_assignment_size = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_CgrpAssignmentCount_Metric_Name, "current assignment's partition count.", {"on_topic", "on_zone"});
    librdkafka_cgrp_rebalanace_age = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_CgrpRebalanceAge_Metric_Name, "time elapsed since last rebalance (assign or revoke) (milliseconds).",
        {"on_topic", "on_zone"});

    // Note: If the metric is reported over all of the partitions by each replica, then the summation at the shard level
    // will count the same partition-associated metric multiple times by multiple shards.
    librdkafka_topic_consumer_lag = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicTotalConsumerLag_Metric_Name, "sum of topic_partition_consumer_lag over all of the partitions",
        {"on_topic", "on_zone"});
    librdkafka_topic_partition_consumer_lag = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionConsumerLag_Metric_Name,
        "difference between (hi_offset or ls_offset) - max(app_offset, committed_offset). hi_offset is used when "
        "isolation.level=read_uncommitted, otherwise ls_offset.",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_ls_offset = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionLsOffset_Metric_Name,
        "partition's last stable offset on broker, or same as hi_offset is broker version is less than 0.11.0.0.",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_app_offset = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionAppOffset_Metric_Name, "offset of last message passed to application + 1",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_committed_offset =
        &factory.registerMetric<monitor::_gauge>(Librdkafka_TopicPartitionCommittedOffset_Metric_Name,
                                                 "last committed offset", {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_lo_offset = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionLoOffset_Metric_Name, "partition's low watermark offset on broker",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_hi_offset = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionHiOffset_Metric_Name, "partition's high watermark offset on broker",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_msgs_inflight = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionMsgsInflight_Metric_Name, "current number of messages in-flight to/from broker",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_rxmsgs = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionRxMsgs_Metric_Name,
        "total number of messages consumed, not including ignored messages (due to offset, etc).",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_rxbytes = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionRxBytes_Metric_Name, "total number of bytes received for rxmsgs",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_rx_ver_drops =
        &factory.registerMetric<monitor::_gauge>(Librdkafka_TopicPartitionRxVerDrops_Metric_Name,
                                                 "dropped outdated messages", {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_fetchq_cnt = &factory.registerMetric<monitor::_gauge>(
        Librdkafka_TopicPartitionFetchQCnt_Metric_Name, "number of pre-fetched messages in fetch queue",
        {"on_topic", "partition", "on_zone"});
    librdkafka_topic_partition_fetchq_size =
        &factory.registerMetric<monitor::_gauge>(Librdkafka_TopicPartitionFetchQSize_Metric_Name,
                                                 "consumer bytes in fetchq", {"on_topic", "partition", "on_zone"});
}
} // namespace nuclm
