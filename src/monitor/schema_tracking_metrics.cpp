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

#include "schema_tracking_metrics.hpp"

namespace nuclm {

const std::string SchemaTrackingMetrics::SchemaHashInKafkaMessageNotWithLatestSchema_Metric_Name =
    "nucolumnar_aggregator_schema_tracking_not_with_latest_schema_total";
const std::string SchemaTrackingMetrics::NewSchemaBeingFetched_Metric_Name =
    "nucolumnar_aggregator_new_schema_fetched_total";
const std::string SchemaTrackingMetrics::BlockSchemaMigrationToMatchLatestSchema_Metric_Name =
    "nucolumnar_aggregator_block_schema_migration_total";
const std::string SchemaTrackingMetrics::CurrentSchemaVersion_Metric_Name =
    "nucolumnar_aggregator_current_schema_version";
const std::string SchemaTrackingMetrics::ServicePassedSchemaVersion_Metric_Name =
    "nucolumnar_agregator_received_service_schema_version";
const std::string SchemaTrackingMetrics::SchemaUpdateAtGlobalTable_Metric_Name =
    "nucolumnar_aggregator_schema_update_at_global_table_total";
const std::string SchemaTrackingMetrics::SchemaVersionAtGlobalTable_Metric_Name =
    "nucolumnar_aggregator_schema_version_at_global_table";

SchemaTrackingMetrics::SchemaTrackingMetrics(monitor::NuDataMetricsFactory& factory) {
    schema_tracking_not_with_latest_schema_total = &factory.registerMetric<monitor::_counter>(
        SchemaHashInKafkaMessageNotWithLatestSchema_Metric_Name,
        "nucolumnar aggregator total number of events when schema hash is not with latest schema", {"table"});

    schema_tracking_new_schema_fetched_total = &factory.registerMetric<monitor::_counter>(
        NewSchemaBeingFetched_Metric_Name, "nucolumnar aggregator total number of events when new schema is fetched",
        {"table"});

    schema_tracking_block_schema_migration_total = &factory.registerMetric<monitor::_counter>(
        BlockSchemaMigrationToMatchLatestSchema_Metric_Name,
        "nucolumnar aggregator total number of events when block schema migration happens", {"table"});

    schema_tracking_current_schema_used = &factory.registerMetric<monitor::_gauge>(
        CurrentSchemaVersion_Metric_Name, "nucolumnar aggregator current schema version", {"table", "version"});

    schema_tracking_service_schema_passed = &factory.registerMetric<monitor::_gauge>(
        ServicePassedSchemaVersion_Metric_Name, "nucolumnar aggregator received schema version from nucolumnar service",
        {"table", "version"});

    schema_update_at_global_table_total = &factory.registerMetric<monitor::_counter>(
        SchemaUpdateAtGlobalTable_Metric_Name,
        "nucolumnar aggregator schema version update from clickhouse at aggregator loader manager", {"table"});

    schema_version_at_global_table = &factory.registerMetric<monitor::_gauge>(
        SchemaVersionAtGlobalTable_Metric_Name,
        "nucolumnar aggregator table schema version for aggregator loader manager's global table",
        {"table", "version"});
}
} // namespace nuclm
