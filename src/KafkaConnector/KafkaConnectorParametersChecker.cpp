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

#include "KafkaConnectorParametersChecker.h"

uint64_t KafkaConnectorParametersChecker::adjustMaxPollIntervalMs() {
    uint64_t result;
    with_settings([&result](SETTINGS s) {
        auto& cons_config = s.config.kafka.consumerConf;
        auto retry_delay = cons_config.max_number_of_kafka_commit_metadata_retries *
            cons_config.kafka_commit_metadata_max_retry_delay_ms;
        if (cons_config.max_poll_interval_ms < retry_delay) {
            LOG(WARNING) << "max.poll.interval.ms (" << cons_config.max_poll_interval_ms
                         << ") is smaller than retry delay (" << retry_delay
                         << "). max.poll.interval.ms is going to be adjusted to " << retry_delay;
            result = retry_delay;
        } else {
            result = cons_config.max_poll_interval_ms;
        }
    });
    return result;
}

uint64_t KafkaConnectorParametersChecker::adjustSessionTimeoutMs() {
    uint64_t result;
    with_settings([&result](SETTINGS s) {
        auto& cons_config = s.config.kafka.consumerConf;
        if (cons_config.session_timeout_ms < cons_config.max_poll_interval_ms) {
            LOG(WARNING) << "session.timeout.ms (" << cons_config.session_timeout_ms
                         << ") is smaller than max.poll.interval.ms (" << cons_config.max_poll_interval_ms
                         << "). session.timeout.ms is going to be adjusted to " << cons_config.max_poll_interval_ms;
            result = cons_config.max_poll_interval_ms;
        } else {
            result = cons_config.session_timeout_ms;
        }
    });
    return result;
}
