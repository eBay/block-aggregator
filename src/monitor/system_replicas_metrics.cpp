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

#include "system_replicas_metrics.hpp"

namespace nuclm {

const std::string SystemReplicasMetrics::ReplicaIsLeader_Metric_Name = "nucolumnar_clickhouse_replica_is_leader";
const std::string SystemReplicasMetrics::ReplicaIsReadOnly_Metric_Name = "nucolumnar_clickhouse_replica_is_read_only";
const std::string SystemReplicasMetrics::ReplicaIsSessionExpired_Metric_Name =
    "nucolumnar_clickhouse_replica_is_session_expired";
const std::string SystemReplicasMetrics::ReplicaHavingFutureParts_Metric_Name =
    "nucolumnar_clickhouse_replica_number_of_future_parts";
const std::string SystemReplicasMetrics::ReplicaHavingPartsToCheck_Metric_Name =
    "nucolumnar_clickhouse_replica_number_of_parts_to_check";
const std::string SystemReplicasMetrics::ReplicaQueueSize_Metric_Name = "nucolumnar_clickhouse_replica_queue_size";
const std::string SystemReplicasMetrics::ReplicaHavingInsertsInQueue_Metric_Name =
    "nucolumnar_clickhouse_replica_number_of_inserts_in_queue";
const std::string SystemReplicasMetrics::ReplicaHavingMergesInQueue_Metric_Name =
    "nucolumnar_clickhouse_replica_number_of_merges_in_queue";
const std::string SystemReplicasMetrics::ReplicaLogMaxIndex_Metric_Name = "nucolumnar_clickhouse_replica_log_max_index";
const std::string SystemReplicasMetrics::ReplicaLogPointer_Metric_Name = "nucolumnar_clickhouse_replica_log_pointer";
const std::string SystemReplicasMetrics::ReplicaLogIndexToPointer_Difference_Metric_Name =
    "nucolumnar_clickhouse_replica_index_to_log_pointer_diff_size";
const std::string SystemReplicasMetrics::ReplicaLastQueueUpdateTimeDiffToNow_Metric_Name =
    "nucolumnar_clickhouse_replica_queue_update_time_diff_to_now_in_sec";
const std::string SystemReplicasMetrics::ReplicaLastQueueAbsoluteDelay_Metric_Name =
    "nucolumnar_clickhouse_replica_queue_absolute_delay_in_sec";
const std::string SystemReplicasMetrics::ReplicaTotalNumberOfReplicas_Metric_Name =
    "nucolumnar_clickhouse_replica_number_of_replicas_in_shard";
const std::string SystemReplicasMetrics::ReplicaNumberOfActiveReplicas_Metric_Name =
    "nucolumnar_clickhouse_replica_number_of_active_replicas_in_shard";

SystemReplicasMetrics::SystemReplicasMetrics(monitor::NuDataMetricsFactory& factory) {
    replica_is_leader = &factory.registerMetric<monitor::_gauge>(
        ReplicaIsLeader_Metric_Name, "nucolumnar clickhouse replica is leader in shard", {"table"});

    replica_is_readonly = &factory.registerMetric<monitor::_gauge>(
        ReplicaIsReadOnly_Metric_Name, "nucolumnar clickhouse replica is in read-only mode", {"table"});

    replica_is_session_expired = &factory.registerMetric<monitor::_gauge>(
        ReplicaIsSessionExpired_Metric_Name, "nucolumnar clickhouse replica has zookeeper session expired", {"table"});

    replica_having_future_parts = &factory.registerMetric<monitor::_gauge>(
        ReplicaHavingFutureParts_Metric_Name, "nucolumnar clickhouse replica has future_parts currently accumulated",
        {"table"});

    replica_having_parts_to_check = &factory.registerMetric<monitor::_gauge>(
        ReplicaHavingPartsToCheck_Metric_Name, "nucolumnar clickhouse replica has parts to check currently accumulated",
        {"table"});

    replica_queue_size = &factory.registerMetric<monitor::_gauge>(
        ReplicaQueueSize_Metric_Name, "nucolumnar clickhouse replica has queued item in current queue", {"table"});

    replica_having_inserts_in_queue = &factory.registerMetric<monitor::_gauge>(
        ReplicaHavingInsertsInQueue_Metric_Name, "nucolumnar clickhouse replica has number of inserts in current queue",
        {"table"});

    replica_having_merges_in_queue = &factory.registerMetric<monitor::_gauge>(
        ReplicaHavingMergesInQueue_Metric_Name, "nucolumnar clickhouse replica has number of merges in current queue",
        {"table"});

    replica_log_max_index = &factory.registerMetric<monitor::_gauge>(
        ReplicaLogMaxIndex_Metric_Name, "nucolumnar clickhouse replica current value on log max index", {"table"});

    replica_log_pointer = &factory.registerMetric<monitor::_gauge>(
        ReplicaLogPointer_Metric_Name, "nucolumnar clickhouse replica current value on log pointer", {"table"});

    replica_index_to_pointer_diff = &factory.registerMetric<monitor::_gauge>(
        ReplicaLogIndexToPointer_Difference_Metric_Name,
        "nucolumnar clickhouse replica current difference on log max index to log pointer", {"table"});

    replica_queue_update_time_diff_from_now_in_sec = &factory.registerMetric<monitor::_gauge>(
        ReplicaLastQueueUpdateTimeDiffToNow_Metric_Name,
        "nucolumnar clickhouse replica last queue update time compared to now in sec", {"table"});

    replica_absolute_delay_in_sec = &factory.registerMetric<monitor::_gauge>(
        ReplicaLastQueueAbsoluteDelay_Metric_Name, "nucolumnar clickhouse replica absolute delay in sec", {"table"});

    replica_reported_total_replicas_in_shard = &factory.registerMetric<monitor::_gauge>(
        ReplicaTotalNumberOfReplicas_Metric_Name,
        "nucolumnar clickhouse replica reported total number of replicas in shard", {"table"});

    replica_reported_active_replicas_in_shard = &factory.registerMetric<monitor::_gauge>(
        ReplicaNumberOfActiveReplicas_Metric_Name,
        "nucolumnar clickhouse replica reported total number of active replicas in shard", {"table"});
}
} // namespace nuclm
