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

#include "metrics_collector.hpp"
#include "monitor_base/metrics_factory.hpp"

#include <glog/logging.h>

namespace nuclm {

MetricsCollector::MetricsCollector() :
        application_metrics{nullptr},
        kafkaconnector_metrics{nullptr},
        loader_metrics{nullptr},
        schema_tracking_metrics{nullptr},
        system_replicas_metrics{nullptr},
        system_tables_metrics{nullptr},
        server_status_metrics{nullptr},
        distributed_locking_metrics{nullptr} {

    init(monitor::NuDataMetricsFactory::instance());
    LOG(INFO) << "metrics collector is initialized";
}

MetricsCollector& MetricsCollector::instance() {
    static MetricsCollector uinstance;
    return uinstance;
};

void MetricsCollector::init(monitor::NuDataMetricsFactory& factory) {
    application_metrics = std::make_shared<ApplicationMetrics>(factory);
    kafkaconnector_metrics = std::make_shared<KafkaConnectorMetrics>(factory);
    loader_metrics = std::make_shared<LoaderMetrics>(factory);
    schema_tracking_metrics = std::make_shared<SchemaTrackingMetrics>(factory);

    system_replicas_metrics = std::make_shared<SystemReplicasMetrics>(factory);
    system_tables_metrics = std::make_shared<SystemTablesMetrics>(factory);
    server_status_metrics = std::make_shared<ServerStatusMetrics>(factory);

    distributed_locking_metrics = std::make_shared<DistributedLockingMetrics>(factory);
}

} // namespace nuclm
