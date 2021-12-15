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

#include "application_metrics.hpp"

namespace nuclm {

const std::string ApplicationMetrics::AppStarted_Metric_Name = "nucolumnar_aggregator_application_start";

ApplicationMetrics::ApplicationMetrics(monitor::NuDataMetricsFactory& factory) {
    appStarted = &factory.registerMetric<monitor::_gauge>(AppStarted_Metric_Name,
                                                          "nucolumnar aggregator application is started", {"version"});
}
} // namespace nuclm
