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

#include "Aggregator/AggregatorLoaderManager.h"
#include "KafkaConnector/KafkaConnector.h"
#include "KafkaConnector/GlobalContext.h"
#include <nlohmann/json.hpp>
#include <Interpreters/Context.h>
#include <librdkafka/rdkafkacpp.h>
#include <boost/asio/io_service.hpp>

#include <memory>

namespace nuclm {

/**
 * To manage the configuration, launching and shutdown of the Kafka Connectors, which can be more than
 * one when involving more than one data-center deployment
 */
class KafkaConnectorManager {
  public:
    using KafkaConnectorPtr = std::shared_ptr<kafka::KafkaConnector>;
    using KafkaConnectorPtrs = std::vector<KafkaConnectorPtr>;

    KafkaConnectorManager(AggregatorLoaderManager& loader_manager_, DB::ContextMutablePtr context_) :
            context(context_),
            loader_manager(loader_manager_),
            kafka_connector_instances{},
            thread_pool{nullptr},
            kafka_env_initialized{false},
            kafka_connectors_started{false} {}

    ~KafkaConnectorManager() = default;

    bool initKafkaEnvironment();

    bool startKafkaConnectors();

    KafkaConnectorPtr startKafkaConnector(size_t index);

    bool shutdownKafkaConnectors();

    DB::ContextMutablePtr getContext() const { return context; }

    // for debugging purpose
    // void initKafkaConnectorInfo(const std::string& host, const std::string& port, const std::string& topic,
    //                             const std::string& group_id);
    nlohmann::json to_json();

    // for debugging purpose to report detailed kafka behavior to communicate with the brokers
    void reportKafkaStatus(nlohmann::json& status);

    // commands: to resume traffic is to resume accepting the Kafka messages at each Kafka Connector;
    //          to freeze traffic is to stop accepting the Kafka messages at each Kafka Connector;
    void resumeTraffic();
    void freezeTraffic();

    // for debugging purpose, to set and re-set all of the kafka connectors in consume mode.
    void forceConsumeMode(bool flag);
    bool isInConsumeMode() const;

    // for debugging purpose to get and set metadata version
    int getMetadataVersion() const;
    bool setMetadataVersion(int version);

    // commands: to remove the freeze flag on the persistent file system
    //          to check the freeze flag on the persistent file system.
    bool checkPersistedFreezeFlagExists() const; // return true if  the file flag exists;
    void removePersistedFreezeFlag();
    void createPersistedFreezeFlag();
    void getKafkaConnectorFreezeStatus(nlohmann::json& status);

  private:
    DB::ContextMutablePtr context;
    AggregatorLoaderManager& loader_manager;

    KafkaConnectorPtrs kafka_connector_instances;

    boost::asio::io_service io_service;
    std::shared_ptr<kafka::GenericThreadPool> thread_pool;

    std::atomic<bool> kafka_env_initialized;
    std::atomic<bool> kafka_connectors_started;
};

} // namespace nuclm
