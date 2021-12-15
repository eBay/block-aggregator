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

#include "common/obj_life_counter.hpp"
#include "libutils/fds/thread/thread_buffer.hpp"

#include <boost/variant.hpp>
#include <vector>
#include <memory>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <glog/logging.h>
#include "nudata_metric_exception.hpp"
#include "histogram_buckets.hpp"

#include "prometheus/registry.h"
#include "prometheus/counter.h"
#include "prometheus/gauge.h"
#include "prometheus/histogram.h"

#include <string>
#include <functional>
#include <map>
#include <unordered_map>
#include <thread>

namespace monitor {

/**
 * Local counter
 */
class _counter {
  public:
    explicit _counter(prometheus::Counter* counter, int64_t value = 0) : m_prometheus(counter), m_value(value) {}

    _counter(const _counter& c) : m_prometheus(c.m_prometheus) { m_value.store(c.m_value.load()); }

    ~_counter() {}

    void increment(int64_t n = 1) {
        if (n <= 0) {
            return;
        }

        m_value.fetch_add(n);
    }

    int64_t value() const { return m_value.load(); }

    prometheus::Counter* getPrometheus() { return m_prometheus; }

    int64_t getAndReset() {
        int64_t val = 0;
        do {
            val = m_value.load();
        } while (!m_value.compare_exchange_weak(val, 0));
        return val;
    }

  private:
    prometheus::Counter* m_prometheus;
    std::atomic<int64_t> m_value;
};

/**
 * Local gauge
 */
class _gauge {
  public:
    explicit _gauge(prometheus::Gauge* gauge, int64_t default_val = 0) : m_prometheus(gauge), m_value(default_val) {}

    _gauge(const _gauge& obj) {
        m_prometheus = obj.m_prometheus;
        m_value.store(obj.value());
    }

    ~_gauge() {}

    void increment(int64_t n = 1) {
        if (n <= 0) {
            return;
        }
        m_value.fetch_add(n);
    }

    void decrement(int64_t n = 1) {
        if (n <= 0) {
            return;
        }
        m_value.fetch_sub(n);
    }

    void update(int64_t n) { m_value.store(n); }

    int64_t value() const { return m_value.load(); }

    prometheus::Gauge* getPrometheus() { return m_prometheus; }

  private:
    prometheus::Gauge* m_prometheus;
    std::atomic<int64_t> m_value;
};

/**
 * Local histogram
 */
class _histogram {
  public:
    template <class T> friend class Metric;

    _histogram(prometheus::Histogram* histogram,
               const std::vector<double>& boundaries = monitor::HistogramBuckets::DefaultBuckets) :
            m_prometheus(histogram), m_buckets(boundaries), m_freqs(boundaries.size() + 1) {
        reset();
    }

    _histogram(const _histogram& h) :
            m_prometheus(h.m_prometheus), m_buckets(h.m_buckets), m_freqs(h.m_freqs), m_sum(h.m_sum) {}

    ~_histogram() {}

    void observe(double value) {
        std::unique_lock lock(m_mutex);
        auto lower = std::lower_bound(m_buckets.begin(), m_buckets.end(), value);
        auto bkt_idx = lower - m_buckets.begin();
        m_freqs[bkt_idx]++;
        m_sum += value;
    }

    const std::vector<double>& collect() { return m_freqs; }

    // Merge data from this object to the specified object
    void mergeToAndReset(_histogram& h) {
        std::unique_lock lock(m_mutex);
        for (size_t i = 0; i < m_freqs.size(); ++i) {
            h.m_freqs[i] += m_freqs[i];
        }
        h.m_sum += m_sum;
        reset();
    }

    double sum() { return m_sum; }
    const prometheus::Histogram::BucketBoundaries& buckets() { return m_buckets; }

    prometheus::Histogram* getPrometheus() { return m_prometheus; }

  private:
    size_t total_bkts() const { return m_buckets.size() + 1; }
    void reset() {
        std::fill(m_freqs.begin(), m_freqs.end(), 0);
        m_sum = 0;
    }

    // Using int64_t instead of _counter, to avoid calling _counter constructor during initialization
    mutable std::shared_mutex m_mutex;
    prometheus::Histogram* m_prometheus;
    const prometheus::Histogram::BucketBoundaries m_buckets;
    std::vector<double> m_freqs;
    double m_sum;
};

class NuDataMetrics {
  public:
    static fds::ThreadBuffer<NuDataMetrics> MetricsBuffer;

    static std::shared_mutex gaugesMutex;
    static std::unordered_map<std::string, _gauge> gauges;

    // This mutex is not needed because the global histograms map is only accessed by the publish thread.
    static std::mutex ghistogramsMutex;
    static std::unordered_map<std::string, _histogram> histograms;

    NuDataMetrics() {}
    ~NuDataMetrics() {}

    std::unordered_map<std::string, _counter>& getCounters() { return _counters; }

    std::unordered_map<std::string, _histogram>& getHistograms() { return _histograms; }

    template <typename T>
    T& get(void* prometheus, const std::string name,
           const std::vector<double>& buckets = monitor::HistogramBuckets::DefaultBuckets) {
        if (std::is_same<T, _counter>::value) {
            std::shared_lock read_lock(_countersMutex);
            auto const& it = _counters.find(name);
            if (it == _counters.end()) {
                read_lock.unlock();
                std::unique_lock write_lock(_countersMutex);
                if (_counters.find(name) == _counters.end()) {
                    _counters.insert(std::make_pair(name, _counter((prometheus::Counter*)prometheus)));
                }
                return (T&)_counters.at(name);
            } else {
                return (T&)_counters.at(name);
            }
        } else if (std::is_same<T, _histogram>::value) {
            std::shared_lock read_lock(_histogramsMutex);
            auto const& it = _histograms.find(name);
            if (it == _histograms.end()) {
                read_lock.unlock();
                std::unique_lock write_lock(_histogramsMutex);
                if (_histograms.find(name) == _histograms.end()) {
                    _histograms.insert(std::make_pair(name, _histogram((prometheus::Histogram*)prometheus, buckets)));
                    DLOG(INFO) << "Create histogram in thread " << std::this_thread::get_id();
                }
                return (T&)_histograms.at(name);
            } else {
                return (T&)_histograms.at(name);
            }
        } else {
            throw NuDataMetricException("Cannot get metric this type");
        }
    }

    std::shared_mutex& countersMutex() { return _countersMutex; }
    std::shared_mutex& histogramsMutex() { return _histogramsMutex; }

  private:
    mutable std::shared_mutex _countersMutex;
    std::unordered_map<std::string, _counter> _counters;

    mutable std::shared_mutex _histogramsMutex;
    std::unordered_map<std::string, _histogram> _histograms;
};

class NuDataMetricsFactory;

template <typename T> class MetricFamily {
    friend class NuDataMetricsFactory;

  public:
    MetricFamily(std::shared_ptr<prometheus::Registry> registry, const std::string& name, const std::string& desc,
                 const std::vector<std::string>& labels,
                 const std::vector<double>& buckets = monitor::HistogramBuckets::DefaultBuckets) :
            _name(name), _desc(desc), _labels(labels), _buckets(buckets), _p_metricFamily(nullptr) {
        if (std::is_same<T, _counter>::value) {
            _p_metricFamily = &prometheus::BuildCounter().Name(name).Help(desc).Register(*registry.get());
        } else if (std::is_same<T, _gauge>::value) {
            _p_metricFamily = &prometheus::BuildGauge().Name(name).Help(desc).Register(*registry.get());
        } else if (std::is_same<T, _histogram>::value) {
            _p_metricFamily =
                &prometheus::BuildHistogram().Name(name).Help(desc).Buckets(_buckets).Register(*registry.get());
        }
    }

    // Must define a copy constructor as the class contains mutex
    MetricFamily(const MetricFamily& m) :
            _name(m._name),
            _desc(m._desc),
            _labels(m._labels),
            _buckets(m._buckets),
            _p_metricFamily(m._p_metricFamily) {}

    ~MetricFamily() {}

    T& labels(const std::map<std::string, std::string> labelValueMap) {
        if (labelValueMap.size() != _labels.size()) {
            LOG(ERROR) << "Metric dimension does not match, expected " << _labels.size() << ", got "
                       << labelValueMap.size();
            throw NuDataMetricException("Dimension does not match");
        }

        std::string metricName = createMetricName(_name, _labels, labelValueMap);

        if (std::is_same<T, _counter>::value) {
            void* prometheus = nullptr;
            {
                std::shared_lock read_lock(_pMetricsMutex);
                if (_pMetrics.find(metricName) == _pMetrics.end()) {
                    read_lock.unlock();

                    std::unique_lock write_lock(_pMetricsMutex);
                    if (_pMetrics.find(metricName) == _pMetrics.end()) {
                        auto p = reinterpret_cast<prometheus::Family<prometheus::Counter>*>(_p_metricFamily);
                        auto& pCounter = p->Add(labelValueMap);
                        _pMetrics.insert(std::make_pair(metricName, &pCounter));
                        prometheus = (void*)&pCounter;
                    } else {
                        prometheus = _pMetrics.at(metricName);
                    }
                } else {
                    prometheus = _pMetrics.at(metricName);
                }
            }

            return NuDataMetrics::MetricsBuffer.get()->get<T>(prometheus, metricName);
        } else if (std::is_same<T, _gauge>::value) {
            void* prometheus = nullptr;
            {
                std::shared_lock read_lock(_pMetricsMutex);
                if (_pMetrics.find(metricName) == _pMetrics.end()) {
                    read_lock.unlock();

                    std::unique_lock write_lock(_pMetricsMutex);
                    if (_pMetrics.find(metricName) == _pMetrics.end()) {
                        auto p = reinterpret_cast<prometheus::Family<prometheus::Gauge>*>(_p_metricFamily);
                        auto& pGauge = p->Add(labelValueMap);
                        _pMetrics.insert(std::make_pair(metricName, &pGauge));
                        prometheus = (void*)&pGauge;
                    } else {
                        prometheus = _pMetrics.at(metricName);
                    }
                } else {
                    prometheus = _pMetrics.at(metricName);
                }
            }

            std::shared_lock read_lock(NuDataMetrics::gaugesMutex);
            auto const& it = NuDataMetrics::gauges.find(metricName);
            if (it == NuDataMetrics::gauges.end()) {
                read_lock.unlock();

                std::unique_lock write_lock(NuDataMetrics::gaugesMutex);
                auto const& it2 = NuDataMetrics::gauges.find(metricName);
                if (it2 == NuDataMetrics::gauges.end()) {
                    NuDataMetrics::gauges.insert(std::make_pair(metricName, _gauge((prometheus::Gauge*)prometheus)));
                }
                return (T&)NuDataMetrics::gauges.at(metricName);
            } else {
                return (T&)NuDataMetrics::gauges.at(metricName);
            }
        } else if (std::is_same<T, _histogram>::value) {
            void* prometheus = nullptr;
            {
                std::shared_lock read_lock(_pMetricsMutex);
                if (_pMetrics.find(metricName) == _pMetrics.end()) {
                    read_lock.unlock();

                    std::unique_lock write_lock(_pMetricsMutex);
                    if (_pMetrics.find(metricName) == _pMetrics.end()) {
                        auto p = reinterpret_cast<prometheus::Family<prometheus::Histogram>*>(_p_metricFamily);
                        auto& pHistogram = p->Add(labelValueMap, _buckets);
                        _pMetrics.insert(std::make_pair(metricName, &pHistogram));
                        DLOG(INFO) << "Create histogram prometheus in thread " << std::this_thread::get_id();
                        prometheus = (void*)&pHistogram;
                    } else {
                        prometheus = _pMetrics.at(metricName);
                    }
                } else {
                    prometheus = _pMetrics.at(metricName);
                }
            }

            return NuDataMetrics::MetricsBuffer.get()->get<T>(prometheus, metricName, _buckets);
        } else {
            LOG(ERROR) << "Metric type invalid";
            throw NuDataMetricException("Metric type invalid");
        }
    }

    void publishGauges() {
        std::shared_lock lock(NuDataMetrics::gaugesMutex);
        for (auto& item : NuDataMetrics::gauges) {
            auto& g = item.second;
            g.getPrometheus()->Set(g.value());
        }
    }

    void publishHistograms() {
        for (auto& item : NuDataMetrics::histograms) {
            auto& h = item.second;
            h.getPrometheus()->TransferBucketCounters(h.collect(), h.sum());
        }
    }

  private:
    std::string createMetricName(const std::string& name, const std::vector<std::string>& labels,
                                 const std::map<std::string, std::string> labelValueMap) {
        size_t len = std::strlen(name.c_str()) + 1;

        for (size_t i = 0; i < labels.size(); ++i) {
            auto& label = labels[i];
            auto it = labelValueMap.find(label);
            if (it == labelValueMap.end())
                throw NuDataMetricException("Cannot not label " + label);
            len += std::strlen(label.c_str()) + std::strlen(labelValueMap.at(label).c_str()) + 1;
        }

        std::string result;
        result.reserve(len); // <--- preallocate result

        result += name;
        result += ":";
        for (size_t i = 0; i < labels.size(); ++i) {
            auto& label = labels[i];
            result += label;
            result += ",";
            result += labelValueMap.at(label);
        }
        return result;
    }

    const std::string _name;
    const std::string _desc;
    const std::vector<std::string> _labels;
    const std::vector<double> _buckets;

    // Hold all pointers of prometheus metrics
    mutable std::shared_mutex _pMetricsMutex;
    std::unordered_map<std::string, void*> _pMetrics;

    // Hold all pointers of MetricFamily
    void* _p_metricFamily;
};

} // namespace monitor
