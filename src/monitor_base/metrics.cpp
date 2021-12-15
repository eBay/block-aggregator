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

#include <nlohmann/json.hpp>
#include "metrics.hpp"

namespace monitor {

fds::ThreadBuffer<NuDataMetrics> NuDataMetrics::MetricsBuffer;

std::shared_mutex NuDataMetrics::gaugesMutex;
std::unordered_map<std::string, _gauge> NuDataMetrics::gauges;

std::mutex NuDataMetrics::ghistogramsMutex;
std::unordered_map<std::string, _histogram> NuDataMetrics::histograms;

} // namespace monitor
