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

struct SystemReplicasMetrics {
    static const std::string ReplicaIsLeader_Metric_Name;
    static const std::string ReplicaIsReadOnly_Metric_Name;
    static const std::string ReplicaIsSessionExpired_Metric_Name;
    static const std::string ReplicaHavingFutureParts_Metric_Name;
    static const std::string ReplicaHavingPartsToCheck_Metric_Name;
    static const std::string ReplicaQueueSize_Metric_Name;
    static const std::string ReplicaHavingInsertsInQueue_Metric_Name;
    static const std::string ReplicaHavingMergesInQueue_Metric_Name;
    static const std::string ReplicaLogMaxIndex_Metric_Name;
    static const std::string ReplicaLogPointer_Metric_Name;
    static const std::string ReplicaLogIndexToPointer_Difference_Metric_Name;

    static const std::string ReplicaLastQueueUpdateTimeDiffToNow_Metric_Name;

    static const std::string ReplicaLastQueueAbsoluteDelay_Metric_Name;
    static const std::string ReplicaTotalNumberOfReplicas_Metric_Name;
    static const std::string ReplicaNumberOfActiveReplicas_Metric_Name;

    monitor::MetricFamily<monitor::_gauge>* replica_is_leader;
    monitor::MetricFamily<monitor::_gauge>* replica_is_readonly;
    monitor::MetricFamily<monitor::_gauge>* replica_is_session_expired;
    monitor::MetricFamily<monitor::_gauge>* replica_having_future_parts;
    monitor::MetricFamily<monitor::_gauge>* replica_having_parts_to_check;
    monitor::MetricFamily<monitor::_gauge>* replica_queue_size;
    monitor::MetricFamily<monitor::_gauge>* replica_having_inserts_in_queue;
    monitor::MetricFamily<monitor::_gauge>* replica_having_merges_in_queue;
    monitor::MetricFamily<monitor::_gauge>* replica_log_max_index;
    monitor::MetricFamily<monitor::_gauge>* replica_log_pointer;
    monitor::MetricFamily<monitor::_gauge>* replica_index_to_pointer_diff;

    monitor::MetricFamily<monitor::_gauge>* replica_queue_update_time_diff_from_now_in_sec;

    monitor::MetricFamily<monitor::_gauge>* replica_absolute_delay_in_sec;
    monitor::MetricFamily<monitor::_gauge>* replica_reported_total_replicas_in_shard;
    monitor::MetricFamily<monitor::_gauge>* replica_reported_active_replicas_in_shard;

    SystemReplicasMetrics(monitor::NuDataMetricsFactory& factory);
};

} // namespace nuclm
