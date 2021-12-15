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

#include "KafkaConnector.h"
#include "GlobalContext.h"
#include "../common/settings_factory.hpp"
#include "../common/file_command_flags.hpp"
#include "KafkaConnector/Metadata.h"
#include "KafkaConnector/PartitionHandler.h"
#include "common/logging.hpp"
#include "monitor/metrics_collector.hpp"

#include <mutex>
#include <unordered_set>

#include <cassert>
#include <string>
#include <set>
#include <chrono>
#include <thread>

#ifdef _PRERELEASE
#include <flip/flip.hpp>
#endif

#define KCON_ID(id) "KafkaConnector [" << id << "]: "

namespace kafka {

// the following function returns the Epoc time in nano seconds.
int64_t now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

KafkaConnectorError KafkaConnector::consumeBatch(size_t batch_size, int64_t batch_timeout) {
    clearCurrentBatch();
    currentBatch.reserve(batch_size);

    int64_t end = now() + batch_timeout; // convert ms in batch_timeout to nano seconds
    int64_t remaining_timeout = batch_timeout;

    // ToDo: read these from config
    int delay_in_ms = 50;
    double decay_factor = 2.5;
    int max_error = 3;

    int error = 0; // for doing retries
    while (currentBatch.size() < batch_size && error <= max_error) {
        if (error > 0) {
            int exp_delay_ms = pow(decay_factor, error) * delay_in_ms;
            LOG(INFO) << KCON_ID(getId()) << "Kafka consumer consumeBatch delay for " << exp_delay_ms << " (ms)"
                      << " with current error count: " << error;
            std::this_thread::sleep_for(std::chrono::milliseconds(exp_delay_ms));
        }

        RdKafka::Message* msg = consumer->consume(remaining_timeout);
        switch (msg->err()) {
        case RdKafka::ERR__TIMED_OUT:
            error = 0;
            delete msg;
            break;

        case RdKafka::ERR_NO_ERROR:
            currentBatch.push_back(msg);
            error = 0;
            break;

        default:
            LOG(ERROR) << KCON_ID(getId()) << "Kafka consumer->consume() returned error (" << error << "/" << max_error
                       << "): " << msg->errstr();
            error++;
            delete msg;
            continue;
        }

#ifdef _PRERELEASE
        LOG(INFO) << KCON_ID(getId())
                  << "Consume one message in consumBatch. CurrentBatch size: " << currentBatch.size();

        // Since when connection between kafka connector and kafka broker resume, the code may locate
        // at this while loop, more precisely, locate at above consumer->consume. So we also need
        // reset batch_size here
        if (auto consum_batch_size = flip::Flip::instance().get_test_flip<int>("[consum-batch-size]")) {
            batch_size = consum_batch_size.get();
            LOG(INFO) << KCON_ID(getId())
                      << "In consumeBatch, [consum-batch-size]: set kafka connector batch size in consumeBatch: "
                      << batch_size;
        }

        // Even if kafka connector can not connect to kafka broker, consumeBatch will be also invoked. So we should
        // guarantee that the flip can not be hit too quickly under such condition.
        boost::optional<int> sleep_ms_after_consum;
        if (currentBatch.size() == 0 &&
            (sleep_ms_after_consum = flip::Flip::instance().get_test_flip<int>("[sleep-ms-after-consum]"))) {
            LOG(INFO) << KCON_ID(getId())
                      << "In consumeBatch, [sleep-ms-after-consum]: sleep time after kafka connector consume(ms): "
                      << sleep_ms_after_consum.get() << ". currentBatch size: " << currentBatch.size();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_after_consum.get()));
        }
#endif

        remaining_timeout = end - now();
        if (remaining_timeout < 0)
            break;
    }
#ifdef _PRERELEASE
    // LOG(INFO) << "[message-consumption-error]: enter _PRERELEASE";
    if (flip::Flip::instance().test_flip("[message-consumption-error]")) {
        LOG(INFO) << KCON_ID(getId()) << "[message-consumption-error]: simulate kafka connector consumption error";
        error = max_error + 1;
    }
#endif
    if (error <= max_error)
        return KafkaConnectorError::NO_ERROR;
    LOG(ERROR) << KCON_ID(getId()) << "Maximum error allowed (" << max_error
               << ") exceeded. consumeBatch returns error now.";
    return KafkaConnectorError::KAFKA_ERROR;
}

void KafkaConnector::startConsumer() { // thread function
    try {
        auto [kafka_batch_size, kafka_batch_timeout_ms] = with_settings([](SETTINGS s) {
            return std::make_tuple(s.config.kafka.consumerConf.kafka_batch_size,
                                   s.config.kafka.consumerConf.kafka_batch_timeout_ms);
        });

        auto [var_zone, var_topic] = with_settings([this](SETTINGS s) {
            auto& var = s.config.kafka.configVariants[idx];
            return std::make_tuple(var.zone, var.topic);
        });

        LOG_KAFKA(1) << KCON_ID(getId()) << "KafkaConnector: batch_size : " << kafka_batch_size
                     << " kafka_batch_timeout_ms: " << kafka_batch_timeout_ms << " on zone: " << var_zone
                     << " on topic: " << var_topic;

        std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
            nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();
        kafkaconnector_metrics->mainloop_restarted_at_kafka_connector_total
            ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
            .increment(1);

        while (running) {
            LOG_KAFKA(1) << KCON_ID(getId()) << "Start the main consumer loop";

            // check whether the freeze traffic command is on
            auto check_traffic_freeze_start = std::chrono::steady_clock::now();
            checkUntilPersistentFreezeFlagRemoved(var_zone, var_topic);
            auto check_traffic_freeze_end = std::chrono::steady_clock::now();
            auto check_traffic_freeze_elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                                                         check_traffic_freeze_end - check_traffic_freeze_start)
                                                         .count();
            LOG_KAFKA(1) << KCON_ID(getId()) << " traffic frozen with duration: " << check_traffic_freeze_elapsed_time
                         << " (ms)";

            // have CH blocking health checking.
            auto check_backend_start = std::chrono::steady_clock::now();
            checkUntilBackendOK();
            auto check_backend_end = std::chrono::steady_clock::now();
            auto check_backend_elapsed_time =
                std::chrono::duration_cast<std::chrono::milliseconds>(check_backend_end - check_backend_start).count();
            LOG_KAFKA(1) << "Checking backend elapsed time: " << check_backend_elapsed_time << " (ms)";

            auto connect_kafka_start = std::chrono::steady_clock::now();
            connectKafka();
            auto connect_kafka_end = std::chrono::steady_clock::now();
            auto connect_kafka_elapsed_time =
                std::chrono::duration_cast<std::chrono::milliseconds>(connect_kafka_end - connect_kafka_start).count();
            LOG_KAFKA(1) << "Connecting Kafka elapsed time: " << connect_kafka_elapsed_time << " (ms)";

            bool is_ok = true;
            size_t empty_batches_encountered = 0;

            // If freeze traffic flag shows up within the loop, we will have the loop finished but some messages may
            // have the constructed blocks not persisted in the block retry loop and that will lead to is_ok to be
            // false.
            while (running && is_ok && !freeze_traffic_flag) {
                std::chrono::time_point<std::chrono::high_resolution_clock> batched_messsages_processing_start =
                    std::chrono::high_resolution_clock::now();

#ifdef _PRERELEASE
                if (auto consum_batch_size = flip::Flip::instance().get_test_flip<int>("[consum-batch-size]")) {
                    kafka_batch_size = consum_batch_size.get();
                    LOG(INFO) << KCON_ID(getId())
                              << "Before consumeBatch, [consum-batch-size]: set kafka connector batch size: "
                              << kafka_batch_size;
                }
#endif

                if (consumeBatch(kafka_batch_size, kafka_batch_timeout_ms) != KafkaConnectorError::NO_ERROR) {
                    LOG(ERROR) << KCON_ID(getId()) << "Error in consuming data.";
                    break;
                }

#ifdef _PRERELEASE
                if (auto sleep_ms_after_consum = flip::Flip::instance().get_test_flip<int>("[sleep-ms-after-consum]")) {
                    LOG(INFO)
                        << KCON_ID(getId())
                        << "After consumeBatch, [sleep-ms-after-consum]: sleep time after kafka connector consume(ms): "
                        << sleep_ms_after_consum.get() << ". currentBatch size: " << currentBatch.size();
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_after_consum.get()));
                }
#endif

                if (currentBatch.size() == 0) {
                    empty_batches_encountered++;
                    // for empty batches, only shown it for every 5 minutes, to show that the connector is still
                    // running.
                    if (empty_batches_encountered % 300 == 0) {
                        LOG_KAFKA(3) << KCON_ID(getId())
                                     << "read empty batch, encounter: " << empty_batches_encountered;
                    }
                } else {
                    LOG_KAFKA(3) << KCON_ID(getId()) << "read batch size: " << currentBatch.size()
                                 << ", last offset: " << currentBatch.back()->offset();
                }
                // during processing a batch we cannot change the partitions
                for (auto msg : currentBatch) {
                    auto it = partitionHandlers.find(msg->partition());
                    assert(it != partitionHandlers.end());

                    LOG_KAFKA(5) << KCON_ID(getId()) << "Consuming Message offset: " << msg->offset()
                                 << " partition: " << msg->partition() << " length: " << msg->len()
                                 << " message timestamp type: " << msg->timestamp().type
                                 << " message timestamp: " << msg->timestamp().timestamp
                                 << " message lag time (millisecond): "
                                 << std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count() -
                            msg->timestamp().timestamp
                                 << " payload: " << std::string((const char*)msg->payload(), msg->len());

                    kafkaconnector_metrics->batched_messages_processed_by_kafka_connectors_bytes_total
                        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                        .increment(msg->len());
                    kafkaconnector_metrics->bytes_from_batched_messages_by_kafka_connectors
                        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                        .observe(msg->len()); // histogram
                    kafkaconnector_metrics->batched_messages_processed_by_kafka_connectors_total
                        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                        .increment();
                    kafkaconnector_metrics->current_offset_being_processed_by_kafka_connector
                        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                        .update(msg->offset());

                    LOG_KAFKA(5) << KCON_ID(getId()) << "Going to call consume of the partition handler for message ["
                                 << msg->offset() << "]";
                    auto error = it->second->consume(msg);
                    if (error != NO_ERROR) {
                        LOG(ERROR) << KCON_ID(getId()) << "KafkaConnector [" << idx << "] failed to append msg due to "
                                   << getKafkaConnectorErrorName(error) << ", ending consumer loop ...";
                        is_ok = false; // Recreate kafka connector from last commit.
                        break;
                    }
                }

                if (!is_ok) {
                    break;
                }

                size_t number_of_partitions = partitionHandlers.size();
                kafkaconnector_metrics->partitions_in_kafka_connectors
                    ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                    .update(number_of_partitions);

                LOG_KAFKA(4) << KCON_ID(getId()) << "Flushing " << partitionHandlers.size() << " partition handlers";
                // ToDo later we can do this for loop in parallel
                for (auto& entry : partitionHandlers) {
                    if (entry.second->consumeMode() && entry.second->getOffset() != Metadata::EARLIEST_OFFSET) {
                        auto error = entry.second->checkBuffers(consumer->c_ptr());
                        if (error != KafkaConnectorError::NO_ERROR) {
                            LOG(ERROR) << KCON_ID(getId())
                                       << "Check flush buffers failed in partition: " << entry.second->getPartitionId()
                                       << " due to: " << getKafkaConnectorErrorName(error);
                            is_ok = false; // Recreate kafka connector from last commit.
                            break;
                        }
                    }
                }

                std::chrono::time_point<std::chrono::high_resolution_clock> batched_messsages_processing_end =
                    std::chrono::high_resolution_clock::now();
                uint64_t time_diff = std::chrono::duration_cast<std::chrono::microseconds>(
                                         batched_messsages_processing_end.time_since_epoch())
                                         .count() -
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        batched_messsages_processing_start.time_since_epoch())
                        .count();
                kafkaconnector_metrics->processing_time_for_batched_messages_by_kafka_connectors
                    ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                    .observe(time_diff);
            }

            disconnectKafka();
            clearCurrentBatch();
            clearPartitionHandlers(); // Both rebalance_cb and persist failure will trigger rebuild of partition
                                      // handlers.

            LOG_KAFKA(1) << KCON_ID(getId()) << "End the main consumer loop";
        }

        // exit normally
        LOG_KAFKA(1) << KCON_ID(getId()) << "exiting ...";
    } catch (...) {
        LOG(ERROR) << KCON_ID(getId()) << "caught unhandled exception, exiting ...";
    }
    notify_stopped();
}

void KafkaConnector::clearCurrentBatch() {
    for (auto msg : currentBatch) {
        delete msg;
    }
    currentBatch.clear();
}
void KafkaConnector::notify_stopped() {
    std::unique_lock<std::mutex> lock(stoppedMtx);
    stopped = true;
    connector_stopped.notify_all();
}

void KafkaConnector::wait_stopped() {
    std::unique_lock<std::mutex> lock(stoppedMtx);
    connector_stopped.wait(lock, [this] { return stopped; });
}

void KafkaConnector::start() {
    if (!running) {
        LOG_KAFKA(1) << KCON_ID(getId()) << "Starting Kafkaconnector [" << idx << "]";
        stopCompleted = false;
        stopped = false;
        running = true;
        thread_pool->enqueue([this] { startConsumer(); });
    }
}

void KafkaConnector::stop() {
    if (running) {
        running = false;
        // running will make the thread exist from the loop, but we still need to wait until the execution in
        // the loop gets completed.
        wait_stopped();
        // after that, we can perform consumer and partition handler clean-up.

        // Should not close kafka here as it may have potential thread-safe problem.
        // disconnectKafka();
        // clearPartitionHandlers();
    }
    std::lock_guard<std::mutex> lck(stopCompletedMtx);
    stopCompleted = true;
    stopCompletedCv.notify_all();
}

void KafkaConnector::wait() {
    std::unique_lock<std::mutex> lck(stopCompletedMtx);
    stopCompletedCv.wait(lck, [this]() { return stopCompleted == true; });
}

void KafkaConnector::disconnectKafka() {
    if (consumer) {
        LOG(INFO) << KCON_ID(getId()) << "Closing Kafka consumer";

        /**
         * This call will block until the following operations are finished:
         *  - Trigger a local rebalance to void the current assignment
         *  - Stop consumption for current assignment
         *  - Commit offsets
         *  - Leave group
         */
        consumer->close();
        LOG(INFO) << KCON_ID(getId()) << "Kafka consumer closed";
        consumer = nullptr;
    }
}

void KafkaConnector::connectKafka() {
    auto topic = with_settings([this](SETTINGS s) {
        CHECK_LT(idx, s.config.kafka.configVariants.size()) << "idx is out of bounds";
        return s.config.kafka.configVariants[idx].topic;
    });

    std::string err = "";
    // ToDo read from config
    int kafka_retry_interval = 1000;

    while (true) {
        if (!err.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(kafka_retry_interval));
        std::unique_ptr<RdKafka::KafkaConsumer> new_consumer{RdKafka::KafkaConsumer::create(config.get(), err)};
        if (new_consumer) {
            RdKafka::ErrorCode errCode = new_consumer->subscribe({topic});
            if (!errCode) {
                consumer.swap(new_consumer);
                LOG(INFO) << KCON_ID(getId()) << "Succeeded to subscribe to topics: " << topic;
                break;
            } else {
                LOG(ERROR) << KCON_ID(getId()) << "Failed to subscribe to topics: " << topic
                           << ", error: " << RdKafka::err2str(errCode);
            }
        } else {
            LOG(ERROR) << KCON_ID(getId()) << "Failed to create consumer: " << err;
        }
    }
}

// Only in this method that freeze traffic command gets to steady state and can be reported via metrics and RESTful
// calls
void KafkaConnector::checkUntilPersistentFreezeFlagRemoved(const std::string& var_zone, const std::string& var_topic) {
    LOG_KAFKA(3) << KCON_ID(getId()) << "Kafkaconnector to check persistent freeze flag status";
    size_t check_persistent_freeze_flag_period_in_ms = 500;

    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();
    bool flag_exists =
        PersistentCommandFlags::check_command_flag_exists(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
    while (flag_exists) {
        if (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(check_persistent_freeze_flag_period_in_ms));
            flag_exists = PersistentCommandFlags::check_command_flag_exists(
                PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
            freeze_traffic_flag = flag_exists;
            freeze_traffic_status = flag_exists;
            LOG_KAFKA(3) << KCON_ID(getId()) << "Kafkaconnector checking persistent freeze flag status is: "
                         << (flag_exists ? "exists" : "disappeared")
                         << "Kafkaconnector in steady state of freeze traffic status: "
                         << (freeze_traffic_status ? "frozen" : "non-frozen");

            // update metrics:
            kafkaconnector_metrics->kafka_connector_traffic_freeze_command_flag_received
                ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                .update(freeze_traffic_flag ? 1 : 0);
            kafkaconnector_metrics->kafka_connector_traffic_freeze_status
                ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
                .update(freeze_traffic_status ? 1 : 0);

        } else {
            LOG_KAFKA(3) << KCON_ID(getId())
                         << "In Kafkaconnector checking persistent freeze flag status loop, exit now due to not-running"
                         << "Kafkaconnector not in freeze state, due to not-running";
            break;
        }

        LOG_KAFKA(3) << KCON_ID(getId()) << "Kafkaconnector final checking persistent freeze flag status is: "
                     << (flag_exists ? "exists" : "disappeared") << "Kafkaconnector final free flag status is: "
                     << (freeze_traffic_status ? "frozen" : "non-frozen");
    }

    freeze_traffic_flag = false;
    freeze_traffic_status = false;
    kafkaconnector_metrics->kafka_connector_traffic_freeze_command_flag_received
        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
        .update(0);
    kafkaconnector_metrics->kafka_connector_traffic_freeze_status
        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
        .update(0);
}

void KafkaConnector::checkUntilBackendOK() {
    LOG_KAFKA(3) << KCON_ID(getId()) << "Kafkaconnector to check database status";

    size_t check_backend_health_period_in_ms = 100;
    if (database_health_checker != nullptr) {
        bool healthy = database_health_checker();
        while (!healthy) {
            if (running) {
                std::this_thread::sleep_for(std::chrono::milliseconds(check_backend_health_period_in_ms));
                healthy = database_health_checker();
                LOG_KAFKA(3) << KCON_ID(getId())
                             << "Kafkaconnector checking database status is: " << (healthy ? "healthy" : "unhealthy");
            } else {
                LOG_KAFKA(3) << KCON_ID(getId())
                             << "In Kafkaconnector backend database checking loop, exit now due to not-running";
                break;
            }
        }

        LOG_KAFKA(3) << KCON_ID(getId())
                     << "Kafkaconnector final checking database status is: " << (healthy ? "healthy" : "unhealthy");
    } else {
        LOG(WARNING)
            << KCON_ID(getId())
            << "Database health checker assigned to Kafkaconnector is null and thus no checking on database status";
    }
}

nlohmann::json KafkaConnector::toJson() const {
    nlohmann::json j;
    j["running"] = (bool)running;
    j["freeze_traffic_flag"] = (bool)freeze_traffic_flag;     // the command to be received.
    j["freeze_traffic_status"] = (bool)freeze_traffic_status; // the actual frozen or not status

    auto [var_zone, var_topic] = with_settings([this](SETTINGS s) {
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });

    j["zone"] = var_zone;
    j["topic"] = var_topic;

    return j;
}

void KafkaConnector::reportStatus(nlohmann::json& status) {
    status["running"] = (bool)running;
    status["freeze_traffic_flag"] = (bool)freeze_traffic_flag;     // the command to be received.
    status["freeze_traffic_status"] = (bool)freeze_traffic_status; // the actual frozen or not status

    auto [var_zone, var_topic] = with_settings([this](SETTINGS s) {
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });
    status["zone"] = var_zone;
    status["topic"] = var_topic;

    if (running) {
        std::lock_guard<std::mutex> lck(partitionMtx);
        storePreviousAssigmentInformation();
        for (auto& partitionHandler : partitionHandlers) {
            status["partitions"][std::to_string(partitionHandler.second->getPartitionId())] =
                partitionHandler.second->toJson();
        }

        status["rebalances"] = nlohmann::json::array();
        for (auto it = rebalances.begin(); it != rebalances.end(); it++) {
            status["rebalances"].push_back(it->toJson());
        }
    }
}

void KafkaConnector::storePreviousAssigmentInformation() {
    for (auto& entry : partitionHandlers) {
        rebalances.back().initialMetadata[entry.first] = entry.second->getInitialMetadata();
        rebalances.back().replayedBatches[entry.first] = entry.second->getSerializedReplayedBatches();
    }
}

bool KafkaConnector::setMetadataVersion(int version) {
    auto result = Metadata::setVersion(version);
    if (result)
        LOG_KAFKA(1) << "metadata version set to " << version;
    return result;
}

int KafkaConnector::getMetadataVersion() const { return Metadata::getVersion(); }

} // namespace kafka
