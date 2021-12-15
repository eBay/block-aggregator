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

struct DistributedLockingMetrics {
    static const std::string DistributedLockingSuccessfulTotal_Metric_Name;
    static const std::string DistributedLockingFailedTotal_Metric_Name;
    static const std::string DistributedLockingRetriedTotal_Metric_Name;
    static const std::string DistributedLockingZooKeeperExceptionHandlingCount_Metric_Name;
    static const std::string DistributedLockingZooKeeperExceptionHandlingLatency_Metric_Name;

    static const std::string DistributedLockingLockTime_Metric_Name;             // gauge
    static const std::string DistributedLockingLockTimeDistribution_Metric_Name; // histogram

    // To capture the issue when lock initialization having problem due to ZK cluster health
    static const std::string DistributedLockingTrappedInLockInitialization_Metric_Name; // counter

    monitor::MetricFamily<monitor::_counter>* distributed_locking_successful_total;
    monitor::MetricFamily<monitor::_counter>* distributed_locking_failed_total;
    monitor::MetricFamily<monitor::_counter>* distributed_locking_retried_total;

    monitor::MetricFamily<monitor::_counter>* distributed_locking_zookeeper_exception_total;
    monitor::MetricFamily<monitor::_histogram>* distributed_locking_zookeeper_exception_handling_time;

    // A lock's lifetime observed by the distributed locking monitor
    monitor::MetricFamily<monitor::_gauge>* distributed_locking_lock_time;
    monitor::MetricFamily<monitor::_histogram>* distributed_locking_lock_distribution_time;

    monitor::MetricFamily<monitor::_counter>* distributed_locking_trapped_at_initialization_total;

    DistributedLockingMetrics(monitor::NuDataMetricsFactory& factory);
};

} // namespace nuclm
