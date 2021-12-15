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

#include <string>
#include <prometheus/registry.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include "histogram_buckets.hpp"
#include "nudata_metric_exception.hpp"
#include "libutils/fds/thread/thread_buffer.hpp"
#include "metrics.hpp"

namespace monitor {

class NuDataMetricsFactory {
  public:
    NuDataMetricsFactory() : _registry(std::make_shared<prometheus::Registry>()) {}
    NuDataMetricsFactory(std::shared_ptr<prometheus::Registry> registry) : _registry(registry) {}

    ~NuDataMetricsFactory() {}

    static NuDataMetricsFactory& instance() {
        static NuDataMetricsFactory s_metrics_factory;
        return s_metrics_factory;
    }

    // Publish the metrics to Prometheus
    void publish();

    // Produce Prometheus report
    std::string report();

    template <typename T> MetricFamily<T>& getMetric(const std::string& name) {
        if (std::is_same<T, _counter>::value) {
            auto const& it = _counters.find(name);
            if (it == _counters.end()) {
                throw NuDataMetricException("Metric name (type Counter) not registered " + name);
            }
            return *reinterpret_cast<MetricFamily<T>*>(&it->second);
        } else if (std::is_same<T, _gauge>::value) {
            auto const& it = _gauges.find(name);
            if (it == _gauges.end()) {
                throw NuDataMetricException("Metric name (type Gauge) not registered " + name);
            }
            return *reinterpret_cast<MetricFamily<T>*>(&it->second);
        } else if (std::is_same<T, _histogram>::value) {
            auto const& it = _histograms.find(name);
            if (it == _histograms.end()) {
                throw NuDataMetricException("Metric name (type Histogram) not registered " + name);
            }
            return *reinterpret_cast<MetricFamily<T>*>(&it->second);
        } else {
            throw NuDataMetricException("Metric type not supported");
        }
    }

    MetricFamily<_counter>& registerCounter(const std::string& name, const std::string& desc,
                                            const std::vector<std::string>& labels) {
        auto const& it = _counters.find(name);
        if (it == _counters.end()) {
            _counters.insert(std::make_pair(name, MetricFamily<_counter>(_registry, name, desc, labels)));
        }
        return _counters.at(name);
    }

    MetricFamily<_gauge>& registerGauge(const std::string& name, const std::string& desc,
                                        const std::vector<std::string>& labels) {
        auto const& it = _gauges.find(name);
        if (it == _gauges.end()) {
            _gauges.insert(std::make_pair(name, MetricFamily<_gauge>(_registry, name, desc, labels)));
        }
        return _gauges.at(name);
    }

    MetricFamily<_histogram>&
    registerHistogram(const std::string& name, const std::string& desc, const std::vector<std::string>& labels,
                      const std::vector<double>& buckets = monitor::HistogramBuckets::DefaultBuckets) {
        auto const& it = _histograms.find(name);
        if (it == _histograms.end()) {
            _histograms.insert(std::make_pair(name, MetricFamily<_histogram>(_registry, name, desc, labels, buckets)));
        }
        return _histograms.at(name);
    }

    template <typename T>
    MetricFamily<T>& registerMetric(const std::string& name, const std::string& desc,
                                    const std::vector<std::string>& labels,
                                    const std::vector<double>& buckets = monitor::HistogramBuckets::DefaultBuckets) {
        if (std::is_same<T, _counter>::value) {
            auto& c = registerCounter(name, desc, labels);
            return (MetricFamily<T>&)c;
        } else if (std::is_same<T, _gauge>::value) {
            auto& g = registerGauge(name, desc, labels);
            return (MetricFamily<T>&)g;
        } else if (std::is_same<T, _histogram>::value) {
            auto& h = registerHistogram(name, desc, labels, buckets);
            return (MetricFamily<T>&)h;
        } else {
            throw NuDataMetricException("Metric type not supported");
        }
    }

    std::shared_ptr<prometheus::Registry> registry() { return _registry; }

  private:
    std::unordered_map<std::string, MetricFamily<_counter>> _counters;
    std::unordered_map<std::string, MetricFamily<_gauge>> _gauges;
    std::unordered_map<std::string, MetricFamily<_histogram>> _histograms;

    std::shared_ptr<prometheus::Registry> _registry;
};

#define METRICS monitor::NuDataMetricsFactory::instance()

} // namespace monitor
