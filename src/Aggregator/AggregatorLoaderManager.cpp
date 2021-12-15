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

#include "common/settings_factory.hpp"
#include "common/logging.hpp"
#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/DistributedLoaderLock.h>
#include "monitor/metrics_collector.hpp"

#include <Common/Exception.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>

#include <Functions/registerFunctions.h>
#include <Formats/registerFormats.h>

#include <glog/logging.h>
#include <chrono>
#include <boost/algorithm/string/trim.hpp>

namespace nuclm {

namespace ErrorCodes {
extern const int CANNOT_RETRIEVE_DEFINED_TABLES;
extern const int TABLE_DEFINITION_NOT_FOUND;
} // namespace ErrorCodes

static Poco::Timespan timespan(uint32_t interval_ms) {
    uint32_t second = interval_ms / 1000;
    uint32_t remaining_ms = interval_ms - second * 1000;

    return Poco::Timespan(second, remaining_ms * 1000);
}

template <typename T> decltype(auto) retry(T&& func, size_t max_retries = 100, size_t sleep_time_ms = 1000) {
    std::exception_ptr exception;
    for (size_t try_number = 0; try_number < max_retries; try_number++) {
        try {
            return func();
        } catch (...) {
            exception = std::current_exception();
            if (try_number < max_retries) {
                std::string start_message = "Current retry: " + std::to_string(try_number) +
                    ", sleep_time_ms=" + std::to_string(sleep_time_ms) + ", with exception: ";
                LOG(ERROR) << start_message + DB::getCurrentExceptionMessage(true);

                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time_ms));
            }
        }
    }

    // the final retry's result can be the exception.
    std::rethrow_exception(exception);
}

static int64_t now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// to create a singleton object for one-time initialization
class RegisterFunctionsOnce {
  public:
    static RegisterFunctionsOnce& getInstance() {
        static RegisterFunctionsOnce instance;
        return instance;
    }

  private:
    RegisterFunctionsOnce() {
        // to register built-in evaluation functions such as today()
        LOG(INFO) << " to register ClickHouse built-in Formats and Functions for evaluation purpose";
        DB::registerFormats();
        DB::registerFunctions();
    }

    ~RegisterFunctionsOnce() = default;
    RegisterFunctionsOnce(const RegisterFunctionsOnce&) = delete;
    RegisterFunctionsOnce& operator=(const RegisterFunctionsOnce&) = delete;
};

const std::string AggregatorLoaderManager::MATERIALIZED_VIEW_PREFIX_NAME = ".inner.";

AggregatorLoaderManager::AggregatorLoaderManager(DB::ContextMutablePtr context_, boost::asio::io_context& ioc_) :
        context(context_), ioc(ioc_), credential_rotation_timer(ioc_), timer_running(true), connection_pool{nullptr} {
    with_settings([this](SETTINGS s) {
        auto& dbconf = s.config.databaseServer;
        database_name = dbconf.default_database.empty() ? std::string("default") : dbconf.default_database;

        // to initialize the connection parameters.
        connectionParameters.host = dbconf.host;
        connectionParameters.port = dbconf.port;
        connectionParameters.default_database =
            dbconf.default_database.empty() ? std::string("default") : dbconf.default_database;
        std::string username, password;
        if (dbconf.dbauth.dbauth_enabled) {
            username = s.processed.clickhouseUsername;
            password = s.processed.clickhousePassword;
        }

        connectionParameters.security =
            dbconf.tlsEnabled ? DB::Protocol::Secure::Enable : DB::Protocol::Secure::Disable;

        connectionParameters.compression =
            dbconf.compression ? DB::Protocol::Compression::Enable : DB::Protocol::Compression::Disable;

        // for Connection Timeout parameters, choose default parameters (that are in seconds).
        connectionParameters.timeouts = {timespan(dbconf.connection_timeout_ms), timespan(dbconf.send_timeout_ms),
                                         timespan(dbconf.receive_timeout_ms),
                                         timespan(dbconf.tcp_keep_alive_timeout_ms),
                                         timespan(dbconf.http_keep_alive_timeout_ms)};

        LOG_AGGRPROC(3) << "Connection to clickhouse server has defined connection settings: " << connectionParameters;

        uint64_t max_pooled_connections = dbconf.number_of_pooled_connections;
        LOG_AGGRPROC(3) << "Connection to clickhouse server with max number of pooled connections: "
                        << max_pooled_connections;

        connection_pool = std::make_shared<LoaderConnectionPool>(username, password, max_pooled_connections, "client",
                                                                 connectionParameters);
    });

    RegisterFunctionsOnce::getInstance();
}

bool AggregatorLoaderManager::checkLoaderConnection() {
    auto init_loader = [&]() {
        AggregatorLoader loader(context, connection_pool, connectionParameters);
        return loader.init();
    };
    bool result = false;
    auto number_of_initial_connection_retries =
        with_settings([this](SETTINGS s) { return s.config.aggregatorLoader.number_of_initial_connection_retries; });

    auto sleep_time_for_retry_initial_connection_ms = with_settings(
        [this](SETTINGS s) { return s.config.aggregatorLoader.sleep_time_for_retry_initial_connection_ms; });

    try {
        result = retry(init_loader, number_of_initial_connection_retries, sleep_time_for_retry_initial_connection_ms);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "checkLoaderConnection finally failed with with exception return code: " << code;
    }

    return result;
}

std::vector<std::string> AggregatorLoaderManager::retrieveDefinedTables() {
    std::vector<std::string> collected_defined_tables;

    try {
        AggregatorLoader loader(context, connection_pool, connectionParameters);
        loader.init();
        DB::Block query_result;

        bool status = loader.getDefinedTables(query_result);
        // the block header has the definition of:
        // name String String(size = 3), type String String(size = 3), default_type String String(size = 3),
        //       default_expression String String(size = 3), comment String String(size = 3),
        //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
        if (status) {
            std::shared_ptr<AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

            std::shared_ptr<SelectQueryStateMachine> sm =
                std::static_pointer_cast<SelectQueryStateMachine>(state_machine);
            // header definition for the table definition block:
            DB::Block sample_block;
            sm->loadSampleHeader(sample_block);
            const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
            int column_index = 0;

            for (auto& p : columns_with_type_and_name) {
                LOG_AGGRPROC(4) << "column index: " << column_index++ << " column type: " << p.type->getName()
                                << " column name: " << p.name << " number of rows: " << p.column->size();
            }

            DB::MutableColumns columns = query_result.mutateColumns();
            // only one column, with type of string;
            auto& column_string_0 = assert_cast<DB::ColumnString&>(*columns[0]);

            size_t total_row_count = column_string_0.size();

            for (size_t i = 0; i < total_row_count; i++) {
                std::string column_0 = column_string_0.getDataAt(i).toString();
                // the column that represents the table name
                if (column_0.find(MATERIALIZED_VIEW_PREFIX_NAME) == std::string::npos) {
                    collected_defined_tables.push_back(column_0);
                    LOG_AGGRPROC(4) << " row: " << i << ": " << column_0 << " (a regular table)";
                } else {
                    LOG_AGGRPROC(4) << " row: " << i << ": " << column_0
                                    << " (not a regular table due to naming, could be a materialized view)";
                }
            }
            LOG_AGGRPROC(4) << " total number of rows retrieved:  " << total_row_count;
        } else {
            std::string err_msg =
                "AggregatorLoader Manager failed to retrieve names of defined tables from backend server";
            LOG(ERROR) << err_msg;
            // need to throw the exception, as otherwise, the outter retry loop will not sleep before next retry due to
            // no table names returned
            throw DB::Exception(err_msg, ErrorCodes::CANNOT_RETRIEVE_DEFINED_TABLES);
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        std::string err_msg = "AggregatorLoader Manager failed to retrieve names of defined tables from backend server";

        throw DB::Exception(err_msg, ErrorCodes::CANNOT_RETRIEVE_DEFINED_TABLES);
    }

    return collected_defined_tables;
}

/**
 * If the configuration file specifies that the number of the tables is > 0, then the total time spent on waiting for
 * the initial table definitions to be ready at ClickHouse server will be:
            number_of_table_definition_retrieval_retries *
                    (number_of_table_definition_retrieval_retries  * sleep_time_for_retry_table_definition_retrieval_ms)
 * with default values, the whole loop will take:  120*(120*0.05) = 720 seconds.
 *
 */
bool AggregatorLoaderManager::initLoaderTableDefinitions() {
    std::vector<std::string> defined_tables;
    LoaderTableDefinitions local_table_definitions;
    auto init_loader_table_definitions = [&]() {
        defined_tables = retrieveDefinedTables();
        LOG(INFO) << " current total number of defined tables retrieved is: " << defined_tables.size();

        std::shared_ptr<SchemaTrackingMetrics> schema_tracking_metrics =
            MetricsCollector::instance().getSchemaTrackingMetrics();

        for (const auto& table_name : defined_tables) {
            try {
                auto search = local_table_definitions.find(table_name);
                if (search == local_table_definitions.end()) {
                    TableColumnsDescription table_columns_description(table_name);
                    AggregatorLoader loader(context, connection_pool, connectionParameters);
                    table_columns_description.buildColumnsDescription(loader);

                    local_table_definitions.insert({table_name, table_columns_description});

                    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
                    size_t total_number_of_columns = table_columns_description.getColumnsDescription().size();
                    loader_metrics->number_of_columns_in_tables_metrics->labels({{"table", table_name}})
                        .update(total_number_of_columns);

                    LOG(INFO) << "loaded table: " << table_name
                              << " with total number of columns: " << total_number_of_columns << " with definition: "
                              << "\n"
                              << table_columns_description.str();
                    schema_tracking_metrics->schema_update_at_global_table_total->labels({{"table", table_name}})
                        .increment(1);
                    size_t hash_code = table_columns_description.getSchemaHash();
                    schema_tracking_metrics->schema_version_at_global_table
                        ->labels({{"table", table_name}, {"version", std::to_string(hash_code)}})
                        .update(1);
                }
            } catch (...) {
                LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                auto code = DB::getCurrentExceptionCode();

                LOG(ERROR) << "with exception return code: " << code;

                std::string err_msg = "Aggregator Loader Manager cannot find table definition for table: " + table_name;
                LOG(ERROR) << err_msg;
                throw DB::Exception(err_msg, ErrorCodes::TABLE_DEFINITION_NOT_FOUND);
            }
        }
    };

    auto number_of_table_definition_retrieval_retries = with_settings(
        [this](SETTINGS s) { return s.config.aggregatorLoader.number_of_table_definition_retrieval_retries; });

    auto sleep_time_for_retry_table_definition_retrieval_ms = with_settings(
        [this](SETTINGS s) { return s.config.aggregatorLoader.sleep_time_for_retry_table_definition_retrieval_ms; });

    auto number_of_tables_defined_in_configuration = with_settings([this](SETTINGS s) {
        auto& databases = s.config.schema.databases;
        for (const auto& database : databases) {
            if (database.databaseName.compare("default") == 0) {
                return database.tables.size();
            }
        }
        return (size_t)0; // if no database name matches defult.
    });

    bool result = false;
    try {
        if (number_of_tables_defined_in_configuration == 0) {
            retry(init_loader_table_definitions, number_of_table_definition_retrieval_retries,
                  sleep_time_for_retry_table_definition_retrieval_ms);
            result = true;
        } else {
            // to retry until the table definitions are ready at the server side. This is because database connection
            // is ready, does not mean the table definition is ready. There is still some delay.
            int counter = number_of_table_definition_retrieval_retries;
            // the outer loop at most takes the following time:
            // number_of_table_definition_retrieval_retries * (number_of_table_definition_retrieval_retries
            //     * sleep_time_for_retry_table_definition_retrieval_ms)
            // with default values, the whole loop will take:  120 (120*0.05) = 720 seconds.
            while (counter > 0) {
                if (defined_tables.empty()) {
                    // each retry takes: number_of_table_definition_retrieval_retries *
                    // sleep_time_for_retry_table_definition_retrieval_ms
                    retry(init_loader_table_definitions, number_of_table_definition_retrieval_retries,
                          sleep_time_for_retry_table_definition_retrieval_ms);

                    LOG(INFO) << "to retrieve the table definition with retrieved table size:  "
                              << defined_tables.size()
                              << " compared to the expected size for tables defined in configuration: "
                              << number_of_tables_defined_in_configuration;
                } else {
                    result = true;
                    break; // finally the table definitions at the server side is ready.
                }
                counter--;
            }
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "checkLoaderConnection finally failed with with exception return code: " << code;
    }

    if (result) {
        // transfer the local cached table definitions to the global table definitions
        std::lock_guard<std::mutex> g{dynamic_table_registration_mutex};
        table_definitions.clear();
        table_definitions = std::move(local_table_definitions);
    }

    return result;
}

void AggregatorLoaderManager::initLoaderLocks() {
    initLoaderLocalLocks();
    LOG(INFO) << "finished local locks for all loaded tables";

    initLoaderDistributedLocks();
    LOG(INFO) << "finished distributed locks for all loaded tables";
}

// NOTE: hardcode distributed lock path for now. Needs to retrieve it from clickhouse system table. The lock path is
// per-shard per-table
void AggregatorLoaderManager::initLoaderDistributedLocks() {
    std::vector<std::string> defined_table_names = getDefinedTableNames();
    for (auto& table_name : defined_table_names) {
        DistributedLoaderLockManager::getInstance().ensureLockExists(table_name);
    }
}

void AggregatorLoaderManager::initLoaderLocalLocks() {
    std::vector<std::string> defined_table_names = getDefinedTableNames();
    for (auto& table_name : defined_table_names) {
        LocalLoaderLockManager::getInstance().ensureLockExists(table_name);
    }
}

const TableColumnsDescription& AggregatorLoaderManager::getTableColumnsDefinition(const std::string& table_name,
                                                                                  bool use_cache) const {
    if (use_cache) {
        std::lock_guard<std::mutex> g{dynamic_table_registration_mutex};
        auto search = table_definitions.find(table_name);
        if (search != table_definitions.end()) {
            LOG_AGGRPROC(3) << "AggregatorLoader Manager getTableColumnsDefinition for table: " << table_name;
            // NOTE: later on, we will need to have the table hash to be included and check it against the hash sent
            // from the client to guarantee the freshness of the table.
            return search->second;
        }
    }

    // to allow to fetch the table definition from the database server, and then put into the cache.
    auto init_loader_table_definition = [&]() {
        try {
            TableColumnsDescription table_columns_description(table_name);
            AggregatorLoader loader(context, connection_pool, connectionParameters);
            table_columns_description.buildColumnsDescription(loader);

            LOG(INFO) << "loaded table: " << table_name << " with definition: "
                      << "\n"
                      << table_columns_description.str();
            {
                std::lock_guard<std::mutex> g{dynamic_table_registration_mutex};

                // if the table name already exists, replace the table definition.
                auto it = table_definitions.find(table_name);
                if (it != table_definitions.end()) {
                    it->second = table_columns_description;
                } else {
                    table_definitions.insert({table_name, table_columns_description});
                }
                updateTableColumnsDefinitionRetrievalTimes(table_name);
            }
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            auto code = DB::getCurrentExceptionCode();

            LOG(ERROR) << "with exception return code: " << code;

            std::string err_msg = "Aggregator Loader Manager cannot find table definition for table: " + table_name;
            LOG(ERROR) << err_msg;
            throw DB::Exception(err_msg, ErrorCodes::TABLE_DEFINITION_NOT_FOUND);
        }
    };

    auto number_of_table_definition_retrieval_retries = with_settings(
        [this](SETTINGS s) { return s.config.aggregatorLoader.number_of_table_definition_retrieval_retries; });

    auto sleep_time_for_retry_table_definition_retrieval_ms = with_settings(
        [this](SETTINGS s) { return s.config.aggregatorLoader.sleep_time_for_retry_table_definition_retrieval_ms; });

    retry(init_loader_table_definition, number_of_table_definition_retrieval_retries,
          sleep_time_for_retry_table_definition_retrieval_ms);

    // leave the final exception to be handled by the caller.
    std::lock_guard<std::mutex> g{dynamic_table_registration_mutex};
    auto new_entry = table_definitions.find(table_name);
    if (new_entry != table_definitions.end()) {
        return new_entry->second;
    }

    LOG(ERROR) << "can not find table definition entry that just got retrieved";
    throw DB::Exception("Aggregator Loader Manager cannot retrieved table definition for table: " + table_name +
                            " that just got retrieved.",
                        ErrorCodes::TABLE_DEFINITION_NOT_FOUND);
}

std::vector<std::string> AggregatorLoaderManager::getDefinedTableNames() const {
    // transfer the local cached table definitions to the global table definitions
    std::vector<std::string> results;
    std::lock_guard<std::mutex> g{dynamic_table_registration_mutex};
    for (auto& it : table_definitions) {
        results.push_back(it.first);
    }
    return results;
}

void AggregatorLoaderManager::startDatabaseInspector() {
    {
        std::lock_guard<std::mutex> g{server_status_inspector_mutex};
        if (server_status_inspector == nullptr) {
            server_status_inspector = std::make_unique<ServerStatusInspector>(ioc, *this, context);
        }
    }
    server_status_inspector->start();
}

void AggregatorLoaderManager::startSystemTablesExtractor() {
    if (system_status_table_extractor == nullptr) {
        system_status_table_extractor = std::make_unique<SystemStatusTableExtractor>(ioc, *this, context);
    }
    system_status_table_extractor->start();
}

ServerStatusInspector::ServerStatus AggregatorLoaderManager::reportDatabaseStatus() const {
    ServerStatusInspector::ServerStatus db_status{ServerStatusInspector::ServerStatus::UNKNOWN};
    // Some other components (such as http-server) may come to ask for database status before aggregator loader manager
    // starts the server-status-inspector.
    {
        std::lock_guard<std::mutex> g{server_status_inspector_mutex};
        if (server_status_inspector == nullptr) {
            return db_status;
        }
    }

    db_status = server_status_inspector->reportStatus();
    return db_status;
}

void AggregatorLoaderManager::reportDatabaseStatus(nlohmann::json& status) const {
    ServerStatusInspector::ServerStatus db_status{ServerStatusInspector::ServerStatus::UNKNOWN};
    // Some other components (such as http-server) may come to ask for database status before aggregator loader manager
    // starts the server-status-inspector.
    {
        std::lock_guard<std::mutex> g{server_status_inspector_mutex};
        if (server_status_inspector == nullptr) {
            status = {{"apiVersion", "v1"},
                      {"kind", "Replica"},
                      {"data", {{"state", nucolumnar::toServerStatusName(db_status)}, {"timestamp", now()}}}};

            return;
        }
    }

    db_status = server_status_inspector->reportStatus();
    status = {{"apiVersion", "v1"},
              {"kind", "Replica"},
              {"data", {{"state", nucolumnar::toServerStatusName(db_status)}, {"timestamp", now()}}}};
}

bool AggregatorLoaderManager::syncDatabaseStatus(nlohmann::json& status) const {
    std::string err_msg;
    // some other components (such as http-server) may come to ask for database status before aggregator loader manager
    // starts the server-status-inspector.
    {
        std::lock_guard<std::mutex> g{server_status_inspector_mutex};
        if (server_status_inspector == nullptr) {
            status = {{"apiVersion", "v1"}, {"message", "Server status inspector object has not been created yet"}};
            return false;
        }
    }

    if (!server_status_inspector->syncStatus(err_msg, true)) {
        status = {{"apiVersion", "v1"}, {"message", err_msg}};
        return false;
    } else {
        status = {{"apiVersion", "v1"}, {"message", "Succeeded to sync database status to local coordinator"}};
        return true;
    }
}

size_t AggregatorLoaderManager::getTableColumnsDefinitionRetrievalTimes(const std::string& table_name) const {
    size_t retrieval_times = 0;
    std::lock_guard<std::mutex> g{dynamic_table_registration_mutex};

    auto search = table_definitions_retrieved_times.find(table_name);
    if (search != table_definitions_retrieved_times.end()) {
        retrieval_times = search->second;
    }

    return retrieval_times;
}

// No lock needed for this private method as its invocation is guarded already by the lock in method:
// getTableColumnsDefinition
void AggregatorLoaderManager::updateTableColumnsDefinitionRetrievalTimes(const std::string& table_name) const {
    size_t retrieval_times = 0;
    auto search = table_definitions_retrieved_times.find(table_name);
    if (search != table_definitions_retrieved_times.end()) {
        retrieval_times = search->second;
    }

    table_definitions_retrieved_times[table_name] = ++retrieval_times;
}

void AggregatorLoaderManager::shutdown() {
    bool stop_db_status_inspector = false;
    {
        std::lock_guard<std::mutex> g{server_status_inspector_mutex};

        if (server_status_inspector != nullptr) {
            stop_db_status_inspector = true;
        }
    }

    if (stop_db_status_inspector) {
        server_status_inspector->stop();
    }

    if (system_status_table_extractor != nullptr) {
        system_status_table_extractor->stop();
    }
}

void AggregatorLoaderManager::startCredentialRotationTimer() {
    if (!timer_running.load()) {
        LOG(INFO) << "DB credential rotation timer is stopped";
        return;
    }
    uint32_t wait = 30;
    LOG_ADMIN(4) << "Scheduling db credential rotation timer in " << wait << " secs";
    credential_rotation_timer.expires_after(boost::asio::chrono::seconds(wait));
    credential_rotation_timer.async_wait(std::bind(&AggregatorLoaderManager::db_credential_rotation, this));
}

void AggregatorLoaderManager::db_credential_rotation() {
    if (!timer_running.load()) {
        LOG(INFO) << "DB credential rotation timer is stopped";
        return;
    }
    if (SETTINGS_PARAM(config->databaseServer->dbauth->dbauth_enabled)) {
        if (!connection_pool->is_rotating()) {
            std::string new_user = SETTINGS_PARAM(processed->clickhouseUsername);
            boost::algorithm::trim(new_user);
            if (new_user.length() != 0 && new_user != connection_pool->get_active_user()) {
                // Rotate to new credential
                LOG(INFO) << "DB credential rotation: detect new username " << new_user
                          << ", start to rotate from current username " << connection_pool->get_active_user();
                connection_pool->rotate_credential(SETTINGS_PARAM(processed->clickhouseUsername),
                                                   SETTINGS_PARAM(processed->clickhousePassword));
            }
        } else {
            // Delete retired connections
            connection_pool->free_retired_conn_pool();
        }
    }
    startCredentialRotationTimer();
}

nlohmann::json AggregatorLoaderManager::to_json() const {
    nlohmann::json j;

    bool to_check_db_status = false;
    ServerStatusInspector::ServerStatus db_status{ServerStatusInspector::ServerStatus::UNKNOWN};
    {
        std::lock_guard<std::mutex> g{server_status_inspector_mutex};
        if (server_status_inspector != nullptr) {
            to_check_db_status = true;
        }
    }

    if (to_check_db_status) {
        db_status = server_status_inspector->reportStatus();
    }

    nlohmann::json status = {{"kind", "database"},
                             {"data", {{"state", nucolumnar::toServerStatusName(db_status)}, {"timestamp", now()}}}};

    j["Database_Status"] = status;

    nlohmann::json p;
    std::string active_user = connection_pool->get_active_user();
    bool is_rotating = connection_pool->is_rotating();
    // Don't show new active user until the rotation is finished
    if (is_rotating) {
        active_user += "-rotating";
    }
    p["Is_Rotating"] = is_rotating;
    p["Active_User"] = active_user;
    p["All_Conns_Free"] = connection_pool->is_all_conns_free();
    j["Connection_Pool"] = p;

    return j;
}

} // namespace nuclm
