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

struct SchemaTrackingMetrics {
    static const std::string SchemaHashInKafkaMessageNotWithLatestSchema_Metric_Name;
    static const std::string NewSchemaBeingFetched_Metric_Name;
    static const std::string BlockSchemaMigrationToMatchLatestSchema_Metric_Name;
    static const std::string CurrentSchemaVersion_Metric_Name;
    static const std::string ServicePassedSchemaVersion_Metric_Name;
    static const std::string SchemaUpdateAtGlobalTable_Metric_Name;
    static const std::string SchemaVersionAtGlobalTable_Metric_Name;

    monitor::MetricFamily<monitor::_counter>* schema_tracking_not_with_latest_schema_total;
    monitor::MetricFamily<monitor::_counter>* schema_tracking_new_schema_fetched_total;
    monitor::MetricFamily<monitor::_counter>* schema_tracking_block_schema_migration_total;
    monitor::MetricFamily<monitor::_gauge>* schema_tracking_current_schema_used;
    monitor::MetricFamily<monitor::_gauge>* schema_tracking_service_schema_passed;

    monitor::MetricFamily<monitor::_counter>* schema_update_at_global_table_total;
    monitor::MetricFamily<monitor::_gauge>* schema_version_at_global_table;

    SchemaTrackingMetrics(monitor::NuDataMetricsFactory& factory);
};

} // namespace nuclm
