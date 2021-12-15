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

#include "system_tables_metrics.hpp"

namespace nuclm {

const std::string SystemTablesMetrics::ReplicaTotalBytes_Metric_Name = "nucolumnar_clickhouse_replica_total_bytes";
const std::string SystemTablesMetrics::ReplicaTotalRows_Metric_Name = "nucolumnar_clickhouse_replica_total_rows";

SystemTablesMetrics::SystemTablesMetrics(monitor::NuDataMetricsFactory& factory) {
    replica_total_bytes = &factory.registerMetric<monitor::_gauge>(
        ReplicaTotalBytes_Metric_Name, "nucolumnar clickhouse replica has bytes stored locally", {"table"});

    replica_total_rows = &factory.registerMetric<monitor::_gauge>(
        ReplicaTotalRows_Metric_Name, "nucolumnar clickhouse replica has rows stored locally", {"table"});
}
} // namespace nuclm
