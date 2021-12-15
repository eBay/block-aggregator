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

#include "Aggregator/KafkaConnectorManager.h"
#include "Aggregator/BlockSupportedBufferFactory.h"
#include "KafkaConnector/GlobalContext.h"
#include "KafkaConnector/KafkaConnectorParametersChecker.h"
#include "Aggregator/IoServiceBasedThreadPool.h"

#include "monitor/metrics_collector.hpp"

#include "common/logging.hpp"
#include "common/settings_factory.hpp"
#include "common/file_command_flags.hpp"

#include <stdexcept>
#include <librdkafka/rdkafkacpp.h>
#include <jwt/jwt.hpp>

namespace nuclm {

namespace ErrorCodes {
extern const int KAFKA_CONNECTOR_CONFIGURATION_ERROR;
extern const int KAFKA_CONNECTOR_START_ERROR;
} // namespace ErrorCodes

// NOTE: at this time the number of the kafka connector supported is one.
bool KafkaConnectorManager::initKafkaEnvironment() {
    kafka_env_initialized = false;
    try {
        with_settings([this](SETTINGS s) {
            if (s.config.kafka.configVariants.size() > 0) {
                kafka::GlobalContext::instance().setBufferFactory(
                    std::make_shared<BlockSupportedBufferFactory>(loader_manager, context));
                size_t number_of_threads_in_pool = s.config.kafka.configVariants.size();
                thread_pool = std::make_shared<IOServiceBasedThreadPool>(&io_service, number_of_threads_in_pool);
                thread_pool->init();
                kafka_env_initialized = true;
                LOG(INFO) << " KafkaConnector Manager has initialized thread pool for kafka environment.";

            } else {
                LOG(ERROR) << " KafkaConnector Manager has no kafka connector information defined.";
                throw DB::Exception("KafkaConnector Manager cannot kafka connector information.",
                                    ErrorCodes::KAFKA_CONNECTOR_CONFIGURATION_ERROR);
            }
        });
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "KafkaConnector Manager failed to initialize kafka environment with with exception return code: "
                   << code;
        kafka_env_initialized = false;
    }

    return kafka_env_initialized;
}

bool KafkaConnectorManager::startKafkaConnectors() {
    kafka_connectors_started = false;
    try {
        with_settings([this](SETTINGS s) {
            for (size_t i = 0; i < s.config.kafka.configVariants.size(); i++) {
                KafkaConnectorManager::KafkaConnectorPtr ptr = startKafkaConnector(i);
                kafka_connector_instances.push_back(ptr);
            }

            kafka_connectors_started = true;
            LOG(INFO) << " KafkaConnector Manager has started kafka connectors.";
        });
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "KafkaConnector Manager failed to start kafka connectors with with exception return code: "
                   << code;
        kafka_connectors_started = false;
    }

    return kafka_connectors_started;
}

// sets a kafka parameters, checks the result: if success => log what was set; else log error details and throw
template <typename T> void set_param(RdKafka::Conf& conf, const std::string& param, const T& arg) noexcept(false) {
    std::string errstr;
    if (conf.set(param, arg, errstr) == RdKafka::Conf::CONF_OK) {
        LOG_KAFKA(3) << "set '" << param << "' to '" << arg << "'";
    } else {
        LOG(ERROR) << "failed to set '" << param << "' to '" << arg << "': " << errstr;
        throw std::invalid_argument(errstr);
    }
}

void set_params(RdKafka::Conf& conf) {}

template <typename N, typename T, typename... Pairs>
void set_params(RdKafka::Conf& conf, N param, const T& arg, Pairs&&... pairs) noexcept(false) {
    set_param(conf, param, arg);
    set_params(conf, pairs...);
}

KafkaConnectorManager::KafkaConnectorPtr KafkaConnectorManager::startKafkaConnector(size_t idx) {
    LOG_KAFKA(1) << "Creating KafkaConnector[" << idx << "]";

    return with_settings([this, idx](SETTINGS s) {
        auto& var = s.config.kafka.configVariants[idx];
        auto& cons_conf = s.config.kafka.consumerConf;
        // TODO: Memory leak.
        RdKafka::Conf& conf = *RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        try {
            /* clang-format off */
            set_params(conf, 
                "bootstrap.servers", var.hosts, 
                "group.id","nucolumnar_aggregator." + var.variantName + "_" + var.zone + "_" + var.topic,
                "enable.auto.commit", cons_conf.enable_auto_commit ? "true" : "false", 
                "auto.offset.reset", cons_conf.auto_offset_reset, 
                "max.poll.interval.ms", std::to_string(KafkaConnectorParametersChecker::adjustMaxPollIntervalMs()), 
                "session.timeout.ms", std::to_string(KafkaConnectorParametersChecker::adjustSessionTimeoutMs()),
                "metadata.request.timeout.ms", std::to_string(cons_conf.metadata_request_timeout_ms),
                "statistics.interval.ms", std::to_string(10000),
                "partition.assignment.strategy", "range"
            );
            const char * kafka_debug = getenv("KAFKA_DEBUG");
            if (kafka_debug != nullptr) {
                auto kafka_debug_str = std::string(kafka_debug);
                if (kafka_debug_str.empty()) {
                    kafka_debug_str = "all";
                }
                LOG_KAFKA(3) << "Enable kafka consumer debug with KAFKA_DEBUG: " << kafka_debug_str;
                set_params(conf, "debug", kafka_debug_str);
            } else if (!cons_conf.debug.empty()) {
                LOG_KAFKA(3) << "Enable kafka consumer debug with config: " << cons_conf.debug;
                set_params(conf, "debug", cons_conf.debug);
            }

            // turn on TLS setting if the tls-enable flag is turned on
            if (cons_conf.tlsEnabled) {
                //protocol can be either of: sasl_plaintext, sasl_ssl
                if ((cons_conf.securityProtocol.compare("sasl_plaintext") != 0)
                    && (cons_conf.securityProtocol.compare("sasl_ssl") != 0)) {
                    LOG(FATAL) << "kafka security protocol chosen: " << cons_conf.securityProtocol
                               << " not from: sasl_plaintext or sasl_ssl.";
                }
                LOG_KAFKA(3) << "kafka security protocol chosen is:  " << cons_conf.securityProtocol << " for subject: " << var.subject;
                set_params (conf, "security.protocol", cons_conf.securityProtocol);
                if (cons_conf.securityProtocol.compare("sasl_ssl") == 0 ) {
                    set_params(conf, "ssl.ca.location", cons_conf.brokerCacert);
                }
            }

            // only configure Kafka security for authentication/authorization settings when the secure flag is turned on.
            if (var.secure) {
                const auto [token, subject] = with_settings([idx](SETTINGS s) {
                    auto& jwt_params = s.processed.authKafkaClient[idx];
                    if (!jwt_params.subject.empty()) {
                        LOG_KAFKA(3) << "sasl enabled for subject=" << jwt_params.subject;

                        // set config for the producer
                        jwt::jwt_object obj;
                        obj.header().algo(jwt::algorithm::HS256);
                        obj.add_claim("sub", jwt_params.subject)
                            .add_claim("iat", kafka::now() / 1000)
                            .add_claim("$int_roles", json_t::array_t{jwt_params.subject})
                            .add_claim("id", jwt_params.id)
                            .add_claim("username", jwt_params.username)
                            .add_claim("ip", jwt_params.ip)
                            .secret(jwt_params.secret[0]);
                        return std::pair{obj.signature(), jwt_params.subject};
                    } else {
                        LOG_KAFKA(3) << "sasl disabled due to subject empty";
                        return std::pair < std::string, std::string > {};
                    }
                });

                if (!cons_conf.tlsEnabled) {
                    set_params(conf, "security.protocol", "sasl_plaintext");
                }

                if (!token.empty()) {
                    /* clang-format off */
                    set_params(conf,
                               "sasl.mechanisms", "JWT",
                               "sasl.username", subject,
                               "sasl.password", token
                    );
                    /* clang-format on */
                } else {
                    LOG_KAFKA(3) << "jwt token is empty";
                }
            } else {
                LOG_KAFKA(3) << "KafkaConnector [" << idx << "] secure disabled";
            }

            //            rd_kafka_conf_set_log_cb(conf.c_ptr_global(),
            //                                     [](const rd_kafka_t* rk, int level, const char* fac, const char* buf)
            //                                     {
            //                                         LOG_KAFKA(2) << "librdkafka log: fac=" << fac << ", buf=" << buf;
            //                                     });

            auto database_health_checker = [this]() {
                ServerStatusInspector::ServerStatus server_status = loader_manager.reportDatabaseStatus();
                bool result = (server_status == ServerStatusInspector::ServerStatus::UP) ? true : false;
                if (result) {
                    // Perform full table definitions reloading to have the most up-to-date schema definitions
                    // next time when this kafkaconnector starts to consume kafka messages again. This is used
                    // when we have dynamic schema update.
                    loader_manager.initLoaderTableDefinitions();
                }
                return result;
            };

            KafkaConnectorPtr kafka_connector =
                std::make_shared<kafka::KafkaConnector>(idx, &conf, thread_pool, database_health_checker);
            // if (kafka_connector->start() ==0) {
            kafka_connector->start();
            std::string consumer_group_id_assigned =
                "nucolumnar_aggregator." + var.variantName + "_" + var.zone + "_" + var.topic;
            LOG(INFO) << "consumer group id assigned is: " << consumer_group_id_assigned;
            LOG(INFO) << "Kafka connector started for (topic= " << var.topic << ","
                      << " zone = " << var.zone << ")";
            std::shared_ptr<KafkaConnectorMetrics> kafkaconnector_metrics =
                MetricsCollector::instance().getKafkaConnectorMetrics();
            kafkaconnector_metrics->kafka_connectors_total_number
                ->labels({{"on_topic", var.topic}, {"on_zone", var.zone}})
                .increment(1.0);
            // }
            // else {
            //     LOG(INFO) << "Kafka connector failed to start for (topic= " << var.topic << "," <<  " zone = " <<
            //     var.zone << ")";
            // }

            return kafka_connector;

        } catch (...) {
            LOG(ERROR) << " KafkaConnector Manager failed to initialize a Kafka Connector.";
            throw DB::Exception("KafkaConnector Manager failed to initialize a Kafka Connector.",
                                ErrorCodes::KAFKA_CONNECTOR_START_ERROR);
        }
    });
} // namespace nuclm

bool KafkaConnectorManager::shutdownKafkaConnectors() {
    LOG(INFO) << " KafkaConnector Manager is shutting down Kafka Connectors.";
    bool result = false;

    try {
        for (const auto& p : kafka_connector_instances) {
            p->stop();
        }

        if (kafka_env_initialized) {
            thread_pool->shutdown();
        }

        result = true;
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "KafkaConnector Manager failed to start kafka connectors with with exception return code: "
                   << code;
        result = false;
    }

    LOG(INFO) << " KafkaConnector Manager has shutdown Kafka Connectors.";
    return result;
}

/**
 * Disable the report of kafka consumers herein, as it reports the statistics related to the interaction with
 * each kafka broker and for a large kafka cluster the amount of information is too much for /api/v1/status.
 * The API called reportKafkaStatus is dedicated to report detailed status for kafka connectors, and it can be invoked
 * via:  /api/v1/getKafkaStatus, which calls reportKafkaStatus in this class.
 */
nlohmann::json KafkaConnectorManager::to_json() {
    nlohmann::json j;
    j["KafkaConnectors"] = nlohmann::json::object();
    for (KafkaConnectorPtr& instance : kafka_connector_instances) {
        j["KafkaConnectors"][std::to_string(instance->getId())] = instance->toJson();
    }

    return j;
}

void KafkaConnectorManager::reportKafkaStatus(nlohmann::json& status) {
    status["KafkaConnectors"] = nlohmann::json::object();
    for (KafkaConnectorPtr& instance : kafka_connector_instances) {
        if (instance->isRunning()) {
            nlohmann::json js;
            instance->reportStatus(js);
            status["KafkaConnectors"][std::to_string(instance->getId())] = js;
        } else {
            status["KafkaConnectors"][std::to_string(instance->getId())] = nlohmann::json::object();
        }
    }

    status["KafkaConsumers"] = nlohmann::json::object();
    for (KafkaConnectorPtr& instance : kafka_connector_instances) {
        if (instance->isRunning()) {
            if (instance->getStat().empty()) {
                status["KafkaConsumers"][std::to_string(instance->getId())] = nlohmann::json::object();
            } else {
                status["KafkaConsumers"][std::to_string(instance->getId())] =
                    nlohmann::json::parse(instance->getStat());
            }
        } else {
            status["KafkaConsumers"][std::to_string(instance->getId())] = nlohmann::json::object();
        }
    }
}

// Remove both persistent flag and in-memory flag
void KafkaConnectorManager::resumeTraffic() {
    LOG(INFO) << "KafkaConnector Manager to resume traffic from Kafka";
    if (checkPersistedFreezeFlagExists()) {
        LOG(INFO) << "Persisted traffic free flag does exist, to remove the free flag";
        removePersistedFreezeFlag();
    }

    for (const auto& p : kafka_connector_instances) {
        p->resetFreezeTrafficInMemoryFlag();
    }
}

// Set both persistent flag and in-memory flag
void KafkaConnectorManager::freezeTraffic() {
    LOG(INFO) << "KafkaConnector Manager to freeze traffic from Kafka";
    if (!checkPersistedFreezeFlagExists()) {
        LOG(INFO) << "Persisted traffic free flag does not exist, to create the free flag";
        createPersistedFreezeFlag();
    }
    for (const auto& p : kafka_connector_instances) {
        p->setFreezeTrafficInMemoryFlag();
    }
}

void KafkaConnectorManager::getKafkaConnectorFreezeStatus(nlohmann::json& status) {
    status["KafkaConnectors"] = nlohmann::json::object();
    for (KafkaConnectorPtr& instance : kafka_connector_instances) {
        status["KafkaConnectors"][std::to_string(instance->getId())] = instance->toJson();
    }
}

bool KafkaConnectorManager::setMetadataVersion(int version) {
    if (!kafka_connector_instances.empty()) {
        return kafka_connector_instances[0]->setMetadataVersion(version);
    }
    return false;
}

int KafkaConnectorManager::getMetadataVersion() const {
    if (!kafka_connector_instances.empty()) {
        return kafka_connector_instances[0]->getMetadataVersion();
    }
    return -1;
}

void KafkaConnectorManager::forceConsumeMode(bool flag) {
    LOG(INFO) << "KafkaConnector Manager to force KafkaConnector to be in consume mode (0: false, 1: true): " << flag;
    for (const auto& p : kafka_connector_instances) {
        p->forceConsumeMode(flag);
    }
}

bool KafkaConnectorManager::isInConsumeMode() const {
    bool result = false;
    for (const auto& p : kafka_connector_instances) {
        result = p->isInConsumeMode();
        LOG(INFO) << " KafkaConnector with id: " << p->getId() << " in consume mode (0: false, 1:true): " << result;
    }
    // for debugging purpose, pick the last one.
    return result;
}

bool KafkaConnectorManager::checkPersistedFreezeFlagExists() const {
    return (
        PersistentCommandFlags::check_command_flag_exists(PersistentCommandFlags::FreezeTrafficCommandFlagFileName));
}

void KafkaConnectorManager::createPersistedFreezeFlag() {
    bool result = PersistentCommandFlags::create_command_flag(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
    if (!result) {
        LOG(ERROR) << "Cannot create persisted freeze flag at file: "
                   << PersistentCommandFlags::FreezeTrafficCommandFlagFileName
                   << " under dir: " << PersistentCommandFlags::get_command_flags_dir();
    }
}

void KafkaConnectorManager::removePersistedFreezeFlag() {
    bool result = PersistentCommandFlags::remove_command_flag(PersistentCommandFlags::FreezeTrafficCommandFlagFileName);
    if (!result) {
        LOG(ERROR) << "Cannot remove persistent freeze flag at file: "
                   << PersistentCommandFlags::FreezeTrafficCommandFlagFileName
                   << " under dir: " << PersistentCommandFlags::get_command_flags_dir();
    }
}

} // namespace nuclm
