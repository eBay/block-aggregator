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

#include "EventCb.h"
#include "common/logging.hpp"
#include "monitor/metrics_collector.hpp"
#include "../common/settings_factory.hpp"

#include <nlohmann/json.hpp>

// enum Severity {
//    EVENT_SEVERITY_EMERG = 0,
//    EVENT_SEVERITY_ALERT = 1,
//    EVENT_SEVERITY_CRITICAL = 2,
//    EVENT_SEVERITY_ERROR = 3,
//    EVENT_SEVERITY_WARNING = 4,
//    EVENT_SEVERITY_NOTICE = 5,
//    EVENT_SEVERITY_INFO = 6,
//    EVENT_SEVERITY_DEBUG = 7
//};
static std::vector<std::string> SEVERITY_NAMES = {"EMERG", "ALERT",  "CRITICAL", "ERROR",
                                                  "WARN",  "NOTICE", "INFO",     "DEBUG"};

void kafka::EventHandler::event_cb(RdKafka::Event& event) {
    switch (event.type()) {
    case RdKafka::Event::Type::EVENT_STATS: {
        // update important statistics
        LOG_KAFKA(5) << "Kafka STAT: " << event.str();
        stat_.replace(new urcu_node<std::string>{event.str()});
        update_metrics(event.str());
        break;
    }
    case RdKafka::Event::Type::EVENT_LOG:
        switch (event.severity()) {
        case RdKafka::Event::EVENT_SEVERITY_EMERG:
        case RdKafka::Event::EVENT_SEVERITY_ALERT:
        case RdKafka::Event::EVENT_SEVERITY_CRITICAL:
        case RdKafka::Event::EVENT_SEVERITY_ERROR:
            LOG(ERROR) << SEVERITY_NAMES[event.severity()] << " [" << event.fac() << "] " << event.str()
                       << (event.err() == RdKafka::ErrorCode::ERR_NO_ERROR
                               ? ""
                               : " (errno: " + std::to_string(event.err()) + ")");
            break;
        case RdKafka::Event::EVENT_SEVERITY_WARNING:
        case RdKafka::Event::EVENT_SEVERITY_NOTICE:
            LOG(WARNING) << SEVERITY_NAMES[event.severity()] << " [" << event.fac() << "] " << event.str()
                         << (event.err() == RdKafka::ErrorCode::ERR_NO_ERROR
                                 ? ""
                                 : " (errno: " + std::to_string(event.err()) + ")");
            break;
        case RdKafka::Event::EVENT_SEVERITY_DEBUG:
            LOG_KAFKA(3) << SEVERITY_NAMES[event.severity()] << " [" << event.fac() << "] " << event.str()
                         << (event.err() == RdKafka::ErrorCode::ERR_NO_ERROR
                                 ? ""
                                 : " (errno: " + std::to_string(event.err()) + ")");
            break;
        default:
            LOG_KAFKA(2) << SEVERITY_NAMES[event.severity()] << " [" << event.fac() << "] " << event.str()
                         << (event.err() == RdKafka::ErrorCode::ERR_NO_ERROR
                                 ? ""
                                 : " (errno: " + std::to_string(event.err()) + ")");
            break;
        }
        break;
    case RdKafka::Event::Type::EVENT_ERROR:
        LOG(ERROR) << "ERROR " << event.str() << " (code: " << event.err() << ")";
        break;
    default:
        // reduce to log level 5, as we can experience excessive log message on some QA pods, and we more care about
        // the errors and other logs
        LOG_KAFKA(5) << "THROTTLE: " << event.str();
    }
}

std::string& kafka::EventHandler::get_stat() const {
    auto ptr = stat_.get();
    return *ptr.get();
}

#define UPDATE_PARTITION_METRIC(...)                                                                                   \
    kafkaconnector_metrics->librdkafka_topic_partition_##__VA_ARGS__                                                   \
        ->labels({{"on_zone", zone_}, {"on_topic", topic_name}, {"partition", partition_id}})                          \
        .update(partition_stat.value(#__VA_ARGS__, -1))
#define UPDATE_CONSUMER_METRIC(...)                                                                                    \
    kafkaconnector_metrics->librdkafka_##__VA_ARGS__->labels({{"on_topic", topic_}, {"on_zone", zone_}})               \
        .update(js.value(#__VA_ARGS__, -1))

void kafka::EventHandler::update_metrics(const std::string& stat_json) {
    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();

    nlohmann::json js = nlohmann::json::parse(stat_json);

    if (js.empty()) {
        LOG_KAFKA(3) << "Empty stat json: " << stat_json;
        return;
    }

    UPDATE_CONSUMER_METRIC(replyq);
    UPDATE_CONSUMER_METRIC(msg_cnt);
    UPDATE_CONSUMER_METRIC(msg_bytes);
    UPDATE_CONSUMER_METRIC(rx);
    UPDATE_CONSUMER_METRIC(rx_bytes);
    UPDATE_CONSUMER_METRIC(rxmsgs);
    UPDATE_CONSUMER_METRIC(rxmsg_bytes);

    if (js.find("cgrp") != js.end()) {
        auto& cgrp = js["cgrp"];
        kafkaconnector_metrics->librdkafka_cgrp_state_up->labels({{"on_topic", topic_}, {"on_zone", zone_}})
            .update(cgrp["state"] == "up" ? 1 : 0);
        kafkaconnector_metrics->librdkafka_cgrp_rebalance_cnt->labels({{"on_topic", topic_}, {"on_zone", zone_}})
            .update(cgrp["rebalance_cnt"]);
        kafkaconnector_metrics->librdkafka_cgrp_assignment_size->labels({{"on_topic", topic_}, {"on_zone", zone_}})
            .update(cgrp["assignment_size"]);
        kafkaconnector_metrics->librdkafka_cgrp_rebalanace_age->labels({{"on_topic", topic_}, {"on_zone", zone_}})
            .update(cgrp["rebalance_age"]);
    }
    if (js.find("topics") != js.end()) {
        auto& topics = js["topics"];
        for (auto& [topic_name, topic_stat] : topics.items()) {
            long topic_consumer_lag = -1;
            auto& partitions = topic_stat["partitions"];
            for (auto& [partition_id, partition_stat] : partitions.items()) {
                if (partition_id != "-1") {
                    long lag = partition_stat["consumer_lag"];
                    if (lag >= 0) {
                        if (topic_consumer_lag < 0) {
                            topic_consumer_lag = 0;
                        }
                        topic_consumer_lag += lag;
                    }
                    UPDATE_PARTITION_METRIC(consumer_lag);
                    UPDATE_PARTITION_METRIC(ls_offset);
                    UPDATE_PARTITION_METRIC(app_offset);
                    UPDATE_PARTITION_METRIC(committed_offset);
                    UPDATE_PARTITION_METRIC(lo_offset);
                    UPDATE_PARTITION_METRIC(hi_offset);
                    UPDATE_PARTITION_METRIC(msgs_inflight);
                    UPDATE_PARTITION_METRIC(rxmsgs);
                    UPDATE_PARTITION_METRIC(rxbytes);
                    UPDATE_PARTITION_METRIC(rx_ver_drops);
                    UPDATE_PARTITION_METRIC(fetchq_cnt);
                    UPDATE_PARTITION_METRIC(fetchq_size);
                }
            }
            kafkaconnector_metrics->librdkafka_topic_consumer_lag
                ->labels({{"on_zone", zone_}, {"on_topic", topic_name}})
                .update(topic_consumer_lag);
        }
    }
    UPDATE_CONSUMER_METRIC(time);
}
