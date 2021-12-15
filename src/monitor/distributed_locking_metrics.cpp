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

#include "distributed_locking_metrics.hpp"

namespace nuclm {

const std::string DistributedLockingMetrics::DistributedLockingSuccessfulTotal_Metric_Name =
    "nucolumnar_aggregator_distributed_locking_successful_total";
const std::string DistributedLockingMetrics::DistributedLockingFailedTotal_Metric_Name =
    "nucolumnar_aggregator_distributed_locking_failed_total";
const std::string DistributedLockingMetrics::DistributedLockingRetriedTotal_Metric_Name =
    "nucolumnar_aggregator_distributed_locking_retried_total";

const std::string DistributedLockingMetrics::DistributedLockingZooKeeperExceptionHandlingCount_Metric_Name =
    "nucolumnar_aggregator_zookeeper_exception_total";
const std::string DistributedLockingMetrics::DistributedLockingZooKeeperExceptionHandlingLatency_Metric_Name =
    "nucolumnar_aggregator_zookeeper_exception_latency_in_us";
const std::string DistributedLockingMetrics::DistributedLockingLockTime_Metric_Name =
    "nucolumnar_aggregator_distributed_locking_locktime_in_us";
const std::string DistributedLockingMetrics::DistributedLockingLockTimeDistribution_Metric_Name =
    "nucolumnar_aggregator_distributed_locking_locktime_distribution_in_us";

// To capture the issue when lock initialization having problem due to ZK cluster health that we can not connect to
// a ZK node, or a ZK path cannot be created
const std::string DistributedLockingMetrics::DistributedLockingTrappedInLockInitialization_Metric_Name =
    "nucolumnar_aggregator_distributed_locking_trapped_at_initialization_total";

DistributedLockingMetrics::DistributedLockingMetrics(monitor::NuDataMetricsFactory& factory) {
    distributed_locking_successful_total = &factory.registerMetric<monitor::_counter>(
        DistributedLockingSuccessfulTotal_Metric_Name, "nucolumnar aggregator distributed locking succeeded in total",
        {"table"});

    distributed_locking_failed_total = &factory.registerMetric<monitor::_counter>(
        DistributedLockingFailedTotal_Metric_Name, "nucolumnar aggregator distributed locking failed in total",
        {"table"});

    distributed_locking_retried_total = &factory.registerMetric<monitor::_counter>(
        DistributedLockingRetriedTotal_Metric_Name, "nucolumnar aggregator distributed locking retried in total",
        {"table"});

    // The categories will include (1) hardware  (2) znode, (3) other
    distributed_locking_zookeeper_exception_total = &factory.registerMetric<monitor::_counter>(
        DistributedLockingZooKeeperExceptionHandlingCount_Metric_Name,
        "nucolumnar aggregator distributed locking zookeeper exceptions encountered in total", {"table", "category"});

    distributed_locking_zookeeper_exception_handling_time = &factory.registerMetric<monitor::_histogram>(
        DistributedLockingZooKeeperExceptionHandlingLatency_Metric_Name,
        "nucolumnar aggregator distributed locking zookeeper exception handling latency in microseconds",
        {"table", "category"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // The lifetime of a distributed lock observed by the monitor. status can be: {succeeded, failed}.
    distributed_locking_lock_time = &factory.registerMetric<monitor::_gauge>(
        DistributedLockingLockTime_Metric_Name, "nucolumnar aggregator distributed locking lock time in microseconds",
        {"table", "status"});

    // The status can be: {succeeded, failed}
    distributed_locking_lock_distribution_time = &factory.registerMetric<monitor::_histogram>(
        DistributedLockingLockTimeDistribution_Metric_Name,
        "nucolumnar aggregator distributed locking lock time distribution in microseconds", {"table", "status"},
        monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // The lock name is always called "lock". This is to define the metric that always needs at least one label based
    // on current monitor library limitation.
    distributed_locking_trapped_at_initialization_total = &factory.registerMetric<monitor::_counter>(
        DistributedLockingTrappedInLockInitialization_Metric_Name,
        "nucolumnar aggregator distributed locking trapped at lock initialization in total", {"lock_name"});
}
} // namespace nuclm
