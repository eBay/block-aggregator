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

#include "metrics_factory.hpp"
#include <prometheus/text_serializer.h>

namespace monitor {

void NuDataMetricsFactory::publish() {
    // Publish gauges to Prometheus
    {
        for (auto& item : _gauges) {
            auto& g = item.second;
            g.publishGauges();
        }
    }

    // Access all thread-local buffer to publish counters and hitograms to Prometheus
    NuDataMetrics::MetricsBuffer.access_all_threads([](NuDataMetrics* m) {
        {
            {
                std::shared_lock lock(m->countersMutex());
                for (auto& item : m->getCounters()) {
                    auto& c = item.second;
                    c.getPrometheus()->Increment(c.getAndReset());
                }
            }

            {
                std::shared_lock lock(m->histogramsMutex());
                for (auto& item : m->getHistograms()) {
                    auto& metricName = item.first;

                    // std::lock_guard<std::mutex> hlock(NuDataMetrics::ghistogramsMutex);
                    auto const& it = NuDataMetrics::histograms.find(metricName);
                    if (it == NuDataMetrics::histograms.end()) {
                        _histogram h(item.second.getPrometheus(), item.second.buckets());
                        item.second.mergeToAndReset(h);
                        NuDataMetrics::histograms.insert(std::make_pair(metricName, h));
                    } else {
                        auto& h = item.second;
                        h.mergeToAndReset(it->second);
                    }
                }
            }
        }
    });

    {
        // std::lock_guard<std::mutex> hlock(NuDataMetrics::ghistogramsMutex);
        for (auto& item : NuDataMetrics::histograms) {
            auto& h = item.second;
            h.getPrometheus()->TransferBucketCounters(h.collect(), h.sum());
        }
    }
}

std::string NuDataMetricsFactory::report() {
    prometheus::TextSerializer serializer;
    return serializer.Serialize(_registry->Collect());
}

}; // namespace monitor
