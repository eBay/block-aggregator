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

#include "application_metrics.hpp"
#include "kafka_connector_metrics.hpp"
#include "loader_metrics.hpp"
#include "schema_tracking_metrics.hpp"
#include "system_replicas_metrics.hpp"
#include "system_tables_metrics.hpp"
#include "server_status_metrics.hpp"
#include "distributed_locking_metrics.hpp"

#include <memory>

namespace nuclm {

class MetricsCollector {
  public:
    MetricsCollector();

    // return a singleton instance of this class
    static MetricsCollector& instance();

    ~MetricsCollector() = default;

    std::shared_ptr<ApplicationMetrics> getApplicationMetrics() { return application_metrics; }

    std::shared_ptr<KafkaConnectorMetrics> getKafkaConnectorMetrics() { return kafkaconnector_metrics; }

    std::shared_ptr<LoaderMetrics> getLoaderMetrics() { return loader_metrics; }

    std::shared_ptr<SchemaTrackingMetrics> getSchemaTrackingMetrics() { return schema_tracking_metrics; }

    std::shared_ptr<SystemReplicasMetrics> getSystemReplicasMetrics() { return system_replicas_metrics; }

    std::shared_ptr<SystemTablesMetrics> getSystemTablesMetrics() { return system_tables_metrics; }

    std::shared_ptr<ServerStatusMetrics> getServerStatusMetrics() { return server_status_metrics; }

    std::shared_ptr<DistributedLockingMetrics> getDistributedLockingMetrics() { return distributed_locking_metrics; }

  private:
    void init(monitor::NuDataMetricsFactory& factory);

    std::shared_ptr<ApplicationMetrics> application_metrics;
    std::shared_ptr<KafkaConnectorMetrics> kafkaconnector_metrics;
    std::shared_ptr<LoaderMetrics> loader_metrics;
    std::shared_ptr<SchemaTrackingMetrics> schema_tracking_metrics;

    std::shared_ptr<SystemReplicasMetrics> system_replicas_metrics;
    std::shared_ptr<SystemTablesMetrics> system_tables_metrics;
    std::shared_ptr<ServerStatusMetrics> server_status_metrics;

    std::shared_ptr<DistributedLockingMetrics> distributed_locking_metrics;
};
} // namespace nuclm
