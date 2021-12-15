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

#ifdef __APPLE__
#include <ctime>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

#include <Aggregator/LoaderConnectionPool.h>
#include <Aggregator/DBConnectionParameters.h>
#include <Aggregator/ServerStatusInspector.h>
#include <Aggregator/SystemStatusTableExtractor.h>
#include <Aggregator/TableColumnsDescription.h>
#include <Aggregator/SerializationHelper.h>
#include <nlohmann/json.hpp>

#include <Interpreters/Context.h>

#include <boost/asio.hpp>
#include <memory>
#include <unordered_map>
#include <utility>

#include <glog/logging.h>

namespace nuclm {

class AggregatorLoaderManager {
  public:
    // moved to configuration json file based settings
    // static const size_t DEFAULT_MAX_ALLOWED_BLOCK_SIZE_IN_BYTES = 1024 * 256;
    // static const size_t DEFAULT_MAX_ALLOWED_BLOCK_SIZE_IN_ROWS = 100000;
    // static const size_t DEFAULT_SLEEP_TIME_FOR_RETRY_INITIAL_CONNECTION = 1000; //1000 millionsecond
    // static const size_t DEFAULT_NUMBER_OF_LOADER_INITIAL_CONNECTION_RETRY = 200 ;//for totally 200 seconds
    // static const size_t DEFAULT_SLEEP_TIME_FOR_RETRY_TABLE_DEFINITION_RETRIEVAL = 20; //milliseconds
    // static const size_t DEFAULT_NUMBER_OF_TABLE_DEFINITION_RETRIEVAL_RETRY = 100; // for totally 2 seconds

  public:
    using LoaderTableDefinitions = std::unordered_map<std::string, TableColumnsDescription>;

    // Number of times the corresponding table schema has been retrieved from the backend.
    using LoaderTableDefinitionsRetrievalTimes = std::unordered_map<std::string, size_t>;

  public:
    AggregatorLoaderManager(DB::ContextMutablePtr context_, boost::asio::io_context& ioc_);

    ~AggregatorLoaderManager() = default;

    // The aggregator loader can only be started if we found that the connection to the local clickhouse server is OK.
    bool checkLoaderConnection();

    bool initLoaderTableDefinitions();

    void initLoaderLocks();
    void initLoaderDistributedLocks();
    void initLoaderLocalLocks();

    void startDatabaseInspector();
    void startSystemTablesExtractor();

    std::vector<std::string> getDefinedTableNames() const;

    // we may update the table if it is not in the current table definitions.
    const TableColumnsDescription& getTableColumnsDefinition(const std::string& table, bool use_cache = true) const;

    void startCredentialRotationTimer();

    void shutdown();

    DB::ContextMutablePtr getContext() const { return context; }

    std::string getDatabase() const { return database_name; }

    std::shared_ptr<LoaderConnectionPool> getConnectionPool() const { return connection_pool; }

    const DatabaseConnectionParameters& getConnectionParameters() const { return connectionParameters; }

    ServerStatusInspector::ServerStatus reportDatabaseStatus() const;

    nlohmann::json to_json() const;

    void reportDatabaseStatus(nlohmann::json& status) const;

    bool syncDatabaseStatus(nlohmann::json& status) const;

    void db_credential_rotation();

  public:
    std::vector<std::string> retrieveDefinedTables();

    size_t getTableColumnsDefinitionRetrievalTimes(const std::string& table_name) const;

  private:
    // To filter out materialized view from the retrieved table names.
    static const std::string MATERIALIZED_VIEW_PREFIX_NAME;

  private:
    void updateTableColumnsDefinitionRetrievalTimes(const std::string& table_name) const;

  private:
    DB::ContextMutablePtr context;
    boost::asio::io_context& ioc;
    boost::asio::steady_timer credential_rotation_timer;
    std::atomic<bool> timer_running;

    std::string database_name;

    mutable LoaderTableDefinitions table_definitions; // need to update the cached data
    mutable LoaderTableDefinitionsRetrievalTimes table_definitions_retrieved_times;

    // the connection pool configured by the connection parameters.
    DatabaseConnectionParameters connectionParameters;
    std::shared_ptr<LoaderConnectionPool> connection_pool;

    // server status inspector and racing condition with http server to access the object.
    mutable std::mutex server_status_inspector_mutex;
    std::unique_ptr<ServerStatusInspector> server_status_inspector;

    // system tables metrics extractor
    std::unique_ptr<SystemStatusTableExtractor> system_status_table_extractor;

    // to protect new table dynamic registration
    mutable std::mutex dynamic_table_registration_mutex;
};

} // namespace nuclm
