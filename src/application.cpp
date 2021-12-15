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

#include "application.hpp"

#include "http_server/http_server.hpp"
#include "common/crashdump.hpp"
#include "common/settings_factory.hpp"
#include "common/log_cleaner.hpp"

#include "monitor/metrics_collector.hpp"

#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/KafkaConnectorManager.h>
#include <Aggregator/SSLEnabledApplication.h>
#include <Interpreters/Context.h>
#include <boost/thread/thread.hpp>
#include <boost/chrono.hpp>
#include <boost/filesystem.hpp>

#include <memory>
#include <string>
#include <thread>

namespace nuclm {

Application::Application() : m_http_server{HttpServer::create(m_ioc)}, m_log_cleaner(m_ioc) {}

struct ApplicationStatus {
    bool appStarted = false;
    bool dbInitialConnectionOk = false;
    bool kafkaInitialConnectorsOk = false;
    std::atomic<bool> trafficFreezed{false};
    std::atomic<bool> forcedInConsumeMode{false};

    /**
     * status: running, when everything is OK.
     *         offline, when something is wrong
     *         paused, when receving command to stop the traffic
     * @return
     */
    nlohmann::json to_json() {
        nlohmann::json j;
        if (appStarted) {
            if (!trafficFreezed) {
                j["status"] = "running";
            } else {
                j["status"] = "paused";
            }
        } else {
            j["status"] = "offline";
        }

        j["version"] = SETTINGS_FACTORY.get_version();

        nlohmann::json details;
        details["Application_Started"] = appStarted;
        details["DB_Connection_OK"] = dbInitialConnectionOk;
        details["Kafka_Connector_OK"] = kafkaInitialConnectorsOk;
        details["Incoming_Traffic_Freezed"] = trafficFreezed.load(std::memory_order_relaxed);
        details["Forced_in_Consume_Mode"] = forcedInConsumeMode.load(std::memory_order_relaxed);

        j["details"] = details;

        return j;
    }
};

static int64_t now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

class ApplicationImpl final : public Application {
    friend class Application;

  private:
    ApplicationImpl() :
            m_shared_context(DB::Context::createShared()),
            m_dbcontext{DB::Context::createGlobal(m_shared_context.get())} {
        m_main_thread = pthread_self();
        m_dbcontext->getSettings().background_schedule_pool_size =
            SETTINGS_PARAM(config->aggregatorLoader->flush_task_thread_pool_size);
        LOG(INFO) << "Flush task thread pool size has been changed to: "
                  << m_dbcontext->getSettingsRef().background_schedule_pool_size.value;
    }

  public:
    // ResourceManager* get_resource_manager() { return m_resource_mgr.get(); }
    // Metadata* get_metadata() { return m_metadata.get(); }

    void to_json(nlohmann::json& json_status) {
        json_status = application_status.to_json();
        if (m_kafka_connector_manager) {
            json_status["details"]["Kafka_Connector_Manager"] = m_kafka_connector_manager->to_json();
        }

        if (m_aggregator_loader_manager) {
            json_status["details"]["Aggregator_Loader_Manager"] = m_aggregator_loader_manager->to_json();
        }
    }

    void getDBStatus(nlohmann::json& status) { m_aggregator_loader_manager->reportDatabaseStatus(status); }

    void getKafkaStatus(nlohmann::json& status) {
        if (m_kafka_connector_manager) {
            m_kafka_connector_manager->reportKafkaStatus(status);
        }
    }

    bool syncDBStatus(nlohmann::json& status) { return m_aggregator_loader_manager->syncDatabaseStatus(status); }

    /**
     * Starts DB connections going, admin threads, etc
     */
    bool start(std::function<void()>&& notify_when_stopped) override {
        m_notify_when_stopped = notify_when_stopped;

        // Turn on the metrics monitoring
        http_server()
            .register_handler(
                "/metrics",
                RequestHandler(
                    [](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        monitor::NuDataMetricsFactory::instance().publish();
                        std::string body = monitor::NuDataMetricsFactory::instance().report();
                        HttpServer::send_string(std::move(body), HttpServer::text_content_type, cb, req);
                    },
                    3))
            .register_handler(
                "/api/v1/version",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->reportVersion(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    3))
            .register_handler(
                "/api/v1/threadDump",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        HttpServer::send_string(getThreadDump(), HttpServer::text_content_type, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/status",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->to_json(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    2))

            .register_handler(
                "/api/v1/getDBStatus",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->getDBStatus(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    2))
            .register_handler(
                "/api/v1/getKafkaStatus",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->getKafkaStatus(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    2))
            .register_handler(
                "/api/v1/syncStatus",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        if (this->syncDBStatus(status)) {
                            HttpServer::send_json(status, cb, req, http::status::ok);
                        } else {
                            HttpServer::send_json(status, cb, req, http::status::internal_server_error);
                        }
                    },
                    2))
            .register_handler(
                "/api/v1/getSettings",
                RequestHandler(
                    [](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        HttpServer::send_string(SETTINGS_FACTORY.get_current_settings(), HttpServer::json_content_type,
                                                cb, req);
                    },
                    2))
            .register_handler(
                "/api/v1/setServerMode",
                RequestHandler(
                    [](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        std::string mode;
                        for (auto& [n, v] : uri.getQueryParameters()) {
                            if (n == "mode") {
                                mode = v;
                                break;
                            }
                        }
                        std::string errstr;
                        bool success = false; // nucolumnar::ServiceStatusBean::instance().setServerMode(mode, errstr);
                        nlohmann::json status;
                        status["result"] = success ? "ok" : "fail";
                        if (!success) {
                            status["errmsg"] = errstr;
                        }
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/start",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->acceptStartCommand(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/restart",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->acceptRestartCommand(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/freeze",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->acceptFreezeCommand(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/getFreezeStatus",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->getFreezeStatus(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/getMetadataVersion",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->getMetadataVersion(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/setMetadataVersion",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        int version = -1;
                        for (auto& [n, v] : uri.getQueryParameters()) {
                            if (n == "version") {
                                version = std::stoi(v);
                                break;
                            }
                        }
                        bool success = this->setMetadataVersion(version);
                        nlohmann::json status;
                        status["success"] = success ? true : false;
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/setConsumeMode",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->forceInConsumeMode(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1))
            .register_handler(
                "/api/v1/resetConsumeMode",
                RequestHandler(
                    [this](const Poco::URI& uri, http_server::request_t&& req, http_server::completion_t&& cb) {
                        nlohmann::json status;
                        this->resetConsumeMode(status);
                        HttpServer::send_json(status, cb, req);
                    },
                    1));

        m_log_cleaner.start();
        LOG(INFO) << "log cleaner threads started";

        // Run the I/O service on the requested number of threads
        constexpr int nthreads = 3;
        m_asio_runners.reserve(nthreads);
        for (auto i = nthreads - 1; i >= 0; --i)
            m_asio_runners.emplace_back([this, i] {
                std::string name = "ASIO-worker-" + std::to_string(i);
#ifdef __APPLE__
                pthread_setname_np(name.c_str());
#else
                pthread_setname_np(pthread_self(), name.c_str());
#endif /* __APPLE__ */
                m_ioc.run();
            });
        SETTINGS_FACTORY.load_settings(
            [this](SETTINGS s, bool to_restart = false) {
                if (to_restart) // If configuration changes deem restart
                    signal_main_thread(SIGINT);
            },
            true);

        m_dbcontext->makeGlobalContext();
        m_dbcontext->setApplicationType(DB::Context::ApplicationType::SERVER);
        LOG(INFO) << "finish initialization of context.....";

        // Loading ssl related setting
        SETTINGS_FACTORY.with_settings([this](SETTINGS s) {
            if (s.config.databaseServer.tlsEnabled) {
                LOG(INFO) << "database server TLS is enabled and to load SSL related settings";
                std::string config_file_path = SETTINGS_FACTORY.get_local_config_file();
                boost::filesystem::path path(config_file_path);
                m_ssl_enabled_application = std::make_unique<SSLEnabledApplication>(path.parent_path().string());
                m_ssl_enabled_application->init();
                if (!m_ssl_enabled_application->isInitialized()) {
                    LOG(FATAL) << "cannot perform required SSL setting initialization";
                }
            } else {
                LOG(INFO) << "database server TLS is not enabled and thus no setting loading is required.....";
            }
        });

        m_aggregator_loader_manager = std::make_unique<AggregatorLoaderManager>(m_dbcontext, m_ioc);

        // To turn into a while loop, until backend connection is OK and table definitions are retrieved.
        bool initial_connection_to_loader_ok = false;
        std::string database_name = m_aggregator_loader_manager->getDatabase();
        while (!initial_connection_to_loader_ok) {
            initial_connection_to_loader_ok = m_aggregator_loader_manager->checkLoaderConnection();
            if (initial_connection_to_loader_ok) {
                LOG(INFO) << "finish initial connection to the backend database: " << database_name;
                std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
                loader_metrics->connection_to_db_metrics->labels({{"database", database_name}}).increment(1.0);
                application_status.dbInitialConnectionOk = true;
            } else {
                LOG(ERROR) << "failed to check initial connection to the backend database: " << database_name
                           << " continue to check...";
            }
        }

        bool table_definitions_retrieved = false;
        while (!table_definitions_retrieved) {
            table_definitions_retrieved = m_aggregator_loader_manager->initLoaderTableDefinitions();
            if (table_definitions_retrieved) {
                size_t number_of_table_definitions_retrieved =
                    m_aggregator_loader_manager->getDefinedTableNames().size();
                LOG(INFO) << "finish initialization of table definitions from the backend database: " << database_name
                          << " with number of tables retrieved: " << number_of_table_definitions_retrieved;
                std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
                loader_metrics->number_of_tables_retrieved_from_db_metrics->labels({{"database", database_name}})
                    .update(number_of_table_definitions_retrieved);
            } else {
                LOG(ERROR) << "failed to initialize table definitions from the backend database: " << database_name
                           << " continue to try to retrieve table definitions... ";
            }
        }

        m_aggregator_loader_manager->startDatabaseInspector();
        LOG(INFO) << "database status inspector started";

        m_aggregator_loader_manager->startSystemTablesExtractor();
        LOG(INFO) << "system tables extractor started";

        m_aggregator_loader_manager->startCredentialRotationTimer();
        LOG(INFO) << "credential rotation timer started";

        // Initialize the local locks and distributed locks to serialize block insertion to DB
        // This step is relatively more expensive compared to the above steps.
        m_aggregator_loader_manager->initLoaderLocks();
        LOG(INFO) << "initialized all loader locks for all loaded tables";

        m_kafka_connector_manager = std::make_unique<KafkaConnectorManager>(*m_aggregator_loader_manager, m_dbcontext);

        if (m_kafka_connector_manager->initKafkaEnvironment()) {
            LOG(INFO) << "finish initialize Kafka environment...";
            application_status.kafkaInitialConnectorsOk = true;
        } else {
            LOG(ERROR) << "failed to initialize Kafka environment .....";
            return false;
        }

        if (m_kafka_connector_manager->startKafkaConnectors()) {
            LOG(INFO) << "finished starting Kafka connectors";
        } else {
            LOG(ERROR) << "failed to start Kafka connectors";
            return false;
        }

        application_status.trafficFreezed = m_kafka_connector_manager->checkPersistedFreezeFlagExists();

        std::shared_ptr<ApplicationMetrics> app_metrics = MetricsCollector::instance().getApplicationMetrics();
        std::string version = SETTINGS_FACTORY.get_version();
        LOG(INFO) << "current application version is: " << version;
        app_metrics->appStarted->labels({{"version", version}}).increment(1.0);

        // Finally set application status to be started
        application_status.appStarted = true;

        return true;
    }

    void stop() override {
        if (!set_stop_in_progress()) {
            printf("another attempt to invoke Application::stop(), calling system exit(20)\n");
            ::exit(20);
        }

        CVLOG(VMODULE_APP, 1) << "Attempting graceful shutdown";

        CVLOG(VMODULE_APP, 2) << "Shutdown step 1: Starting shutdown watcher thread";
        boost::thread stop_watcher_id{ApplicationImpl::stop_watcher_thread};

        CVLOG(VMODULE_APP, 2) << "Shutdown step 2: shutting down log cleaner";
        m_log_cleaner.stop();

        CVLOG(VMODULE_APP, 2) << "Shutdown step 3: shutting down loader manager";
        if (m_aggregator_loader_manager) {
            m_aggregator_loader_manager->shutdown();
        }

        CVLOG(VMODULE_APP, 2) << "Shutdown step 4: shutting down settings file watcher";
        SETTINGS_FACTORY.stop_file_watcher();

        if (m_kafka_connector_manager) {
            CVLOG(VMODULE_APP, 2) << "Shutdown step 5: shutting down Kafka connectors";
            m_kafka_connector_manager->shutdownKafkaConnectors(); // to start the listener in the other thread.
        } else {
            CVLOG(VMODULE_APP, 2) << "Shutdown step 5: skip shut down Kafka connectors as it's not initialized";
        }

        CVLOG(VMODULE_APP, 2) << "Shutdown step 6: shutting down HttpServer";
        m_http_server->stop();

        CVLOG(VMODULE_APP, 2) << "Shutdown step 7: shutting down boost io_context";
        m_ioc.stop();

        CVLOG(VMODULE_APP, 2) << "Shutdown step 8: waiting for asio threads to exit";
        for (ssize_t i = m_asio_runners.size() - 1; i >= 0; --i) {
            m_asio_runners[i].join();
        }

        CVLOG(VMODULE_APP, 2) << "Shutdown step 9: shutting down DB Context";
        m_dbcontext->shutdown();

        LOG(INFO) << "Shutdown complete - Application::stop() exiting";
        m_notify_when_stopped();
    }

    // To pair with freeze command
    void acceptStartCommand(nlohmann::json& status) {
        LOG(INFO) << "accept start command from /api/v1/start";
        if (application_status.appStarted) {
            if (application_status.trafficFreezed.load(std::memory_order_relaxed)) {
                // to resume_traffic
                if (m_kafka_connector_manager) {
                    m_kafka_connector_manager->resumeTraffic();
                    status["success"] = true;
                    status["message"] = "to resume traffic";

                    application_status.trafficFreezed = false;
                } else {
                    // we are in the unexpected bad state.
                    status["success"] = true;
                    status["message"] = "to restart application by invoking stop()";

                    signal_main_thread(SIGINT);
                }
            } else {
                status["success"] = true;
                status["message"] = "application already started and no traffic resume is needed";
            }
        } else {
            LOG(INFO) << "to restart application by invoking stop()";
            signal_main_thread(SIGINT);
        }
    }

    // To pair with start command
    void acceptFreezeCommand(nlohmann::json& status) {
        LOG(INFO) << "accept freeze command from /api/v1/freeze";
        if (application_status.appStarted) {
            if (!application_status.trafficFreezed.load(std::memory_order_relaxed)) {
                // to resume_traffic
                if (m_kafka_connector_manager) {
                    m_kafka_connector_manager->freezeTraffic();
                    status["success"] = true;
                    status["message"] = "to freeze traffic";
                    application_status.trafficFreezed = true;
                } else {
                    LOG(ERROR) << "no KafkaConnector manager created to issue freeze command";
                    status["success"] = false;
                    status["message"] = "no KafkaConnector manager created to issue freeze command";
                }

            } else {
                LOG(INFO) << "application traffic has already been freezed";
                status["success"] = true;
                status["message"] = "application traffic has already been freezed";
            }
        } else {
            LOG(ERROR) << "freeze command not accepted as application has not been started";
            status["success"] = false;
            status["message"] = "freeze command not accepted as application has not been started";
        }
    }

    // Simply restart the process. If there is a persistent freeze flag, after the process gets restarted, the
    // traffic will be still in the "freeze" mode.
    void acceptRestartCommand(nlohmann::json& status) {
        LOG(INFO) << "accept stop command from /api/v1/restart. application is to be stopped and restarted now";
        status["message"] = "restart command accepted and to perform restart now";
        signal_main_thread(SIGINT);
    }

    void forceInConsumeMode(nlohmann::json& status) {
        LOG(INFO) << "accept set-consume-mode command from /api/v1/setConsumeMode";
        if (application_status.appStarted) {
            if (!application_status.trafficFreezed.load(std::memory_order_relaxed)) {
                // to resume_traffic
                if (m_kafka_connector_manager) {
                    m_kafka_connector_manager->forceConsumeMode(true);
                    status["success"] = true;
                    status["message"] = "to set-consume mode";
                    application_status.forcedInConsumeMode = true;
                } else {
                    LOG(ERROR) << "no KafkaConnector manager created to issue set-consume-mode command";
                    status["success"] = false;
                    status["message"] = "no KafkaConnector manager created to issue set-consume-mode command";
                }

            } else {
                LOG(INFO) << "application traffic has already been freezed. no consume mode to be forced";
                status["success"] = true;
                status["message"] = "application traffic has already been freezed. no consume mode to be forced";
            }
        } else {
            LOG(ERROR) << "set-consume-mode command not accepted as application has not been started";
            status["success"] = false;
            status["message"] = "set-consume-mode command not accepted as application has not been started";
        }
    }

    void resetConsumeMode(nlohmann::json& status) {
        LOG(INFO) << "accept reset-consume-mode command from /api/v1/resetConsumeMode";
        if (application_status.appStarted) {
            if (!application_status.trafficFreezed.load(std::memory_order_relaxed)) {
                // to resume_traffic
                if (m_kafka_connector_manager) {
                    m_kafka_connector_manager->forceConsumeMode(false);
                    status["success"] = true;
                    status["message"] = "to reset-consume mode";
                    application_status.forcedInConsumeMode = false;
                } else {
                    LOG(ERROR) << "no KafkaConnector manager created to issue reset-consume-mode command";
                    status["success"] = false;
                    status["message"] = "no KafkaConnector manager created to issue reset-consume-mode command";
                }

            } else {
                LOG(INFO) << "application traffic has already been freezed. no reset-consume-mode to be forced";
                status["success"] = true;
                status["message"] = "application traffic has already been freezed. no reset-consume-mode to be forced";
            }
        } else {
            LOG(ERROR) << "reset-consume-mode not accepted as application has not been started";
            status["success"] = false;
            status["message"] = "reset-consume-mode not accepted as application has not been started";
        }
    }

    void getFreezeStatus(nlohmann::json& status) {
        LOG(INFO) << "getting traffic freeze status ";
        m_kafka_connector_manager->getKafkaConnectorFreezeStatus(status);
    }

    void reportVersion(nlohmann::json& status) {
        LOG(INFO) << "accept version command from /api/v1/version";
        std::string version = SETTINGS_FACTORY.get_version();
        status["version"] = version;
    }

    bool setMetadataVersion(int version) {
        LOG(INFO) << "setting metadata version to " << std::to_string(version);
        return m_kafka_connector_manager->setMetadataVersion(version);
    }

    void getMetadataVersion(nlohmann::json& status) {
        LOG(INFO) << "getting metadata version for /api/v1/getMetadataVersion";
        status["metadata_version"] = std::to_string(m_kafka_connector_manager->getMetadataVersion());
    }

    void signal_main_thread(int sig) {
#ifdef _POSIX_THREADS
        pthread_kill(m_main_thread, sig);
#else
        LOG_ASSERT(false) << "_POSIX_THREADS is not defined";
#endif
    }

  private:
    bool is_stop_in_progress() const { return m_stop_in_progress.load(std::memory_order_acquire); }
    // attempt to set the flag to true and return bool to indicate whether that succeeded or not
    bool set_stop_in_progress() {
        bool current = false;
        return m_stop_in_progress.compare_exchange_strong(current, true);
    }
    static void stop_watcher_thread(void) {
        uint32_t stop_wait_time_sec = SETTINGS_PARAM(config.shutdown_wait_time_sec);
#ifdef __APPLE__
        pthread_setname_np("shutdown_watcher_thread");
#else
        pthread_setname_np(pthread_self(), "shutdown_watcher_thread");
#endif /* __APPLE__ */

        try {
            boost::this_thread::sleep_for(boost::chrono::seconds(stop_wait_time_sec));
        } catch (boost::thread_interrupted& e) {
            // If we are interrupted, exit normally.
            return;
        }

        std::cout << "ApplicationImpl::stop() has not completed in " << stop_wait_time_sec
                  << " seconds, force shutdown system exit(20)" << std::endl;
        ::exit(20);
    }

    DB::SharedContextHolder m_shared_context;
    DB::ContextMutablePtr m_dbcontext;
    std::unique_ptr<SSLEnabledApplication> m_ssl_enabled_application;
    std::unique_ptr<KafkaConnectorManager> m_kafka_connector_manager;
    std::unique_ptr<AggregatorLoaderManager> m_aggregator_loader_manager;

    pthread_t m_main_thread;
    std::function<void()> m_notify_when_stopped;
    std::vector<std::thread> m_asio_runners;
    std::atomic_bool m_stop_in_progress = false;

    ApplicationStatus application_status;
};

Application& Application::instance() {
    static ApplicationImpl instance;
    return instance;
}

} // namespace nuclm
