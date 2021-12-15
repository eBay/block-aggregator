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
#include "monitor/metrics_collector.hpp"
#include <common/logging.hpp>

#include <chrono>
#include <sstream>

#ifdef _PRERELEASE
#include <flip/flip.hpp>
#endif

#include <Aggregator/BlockSupportedBufferFlushTask.h>
#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/DistributedLoaderLock.h>

namespace DB {
namespace ErrorCodes {
extern const int UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE;
extern const int TABLE_IS_READ_ONLY;
extern const int TIMEOUT_EXCEEDED;
extern const int KEEPER_EXCEPTION;
} // namespace ErrorCodes
} // namespace DB

namespace nuclm {

std::atomic<unsigned long> BlockSupportedBufferFlushTask::task_id{0};

std::string BlockSupportedBufferFlushTask::formulateInsertQuery() {
    // the first time loading, need to formulate the insert query.
    std::stringstream fields_section;

    for (auto elem = columns_definition.begin(); elem != columns_definition.end(); ++elem) {
        if (std::distance(elem, columns_definition.end()) == 1) {
            fields_section << (*elem).name; // column name
        } else {
            fields_section << (*elem).name << ",";
        }
    }

    std::string insert_query = "INSERT INTO " + table + "( " + fields_section.str() + " ) VALUES";
    return insert_query;
}

void BlockSupportedBufferFlushTask::reloadBuffer() {
    // to-reschedule the task back to the task pool.
    if (!task_handle->schedule()) {
        LOG(ERROR) << "FlushTask " << assigned_task_id << " unable to reschedule, fail now";
        loading_done = true;
        send_loading_done.set_value();
    } else {
        LOG_AGGRPROC(4) << "FlushTask " << assigned_task_id << " rescheduled now";
    }
}

void BlockSupportedBufferFlushTask::reloadBufferAfter(size_t delay_ms) {
    // to-reschedule the task back to the task pool.
    if (!task_handle->scheduleAfter(delay_ms)) {
        LOG(ERROR) << "FlushTask " << assigned_task_id << " unable to reschedule, fail now";
        loading_done = true;
        send_loading_done.set_value();
    } else {
        LOG_AGGRPROC(4) << "FlushTask " << assigned_task_id << " rescheduled with " << delay_ms << " ms delay";
    }
}

bool BlockSupportedBufferFlushTask::heartbeat() {
    LOG_AGGRPROC(2) << "Executing zookeeper heartbeat ...";
    DB::Block result;
    if (loader->executeTableSelectQuery("system.zookeeper", "select * from system.zookeeper where path = '/'",
                                        result)) {
        LOG_AGGRPROC(2) << "Zookeeper heartbeat succeeded";
        return true;
    } else {
        LOG(ERROR) << "Zookeeper heartbeat failed";
        return false;
    }
}

bool BlockSupportedBufferFlushTask::checkQuorumStatus() {
    // Example: /clickhouse/tables/176596/LSA/quorum/status.
    std::string shard_id = with_settings([this](SETTINGS s) { return s.config.identity.shardId; });
    std::string zk_path = "/clickhouse/tables/" + shard_id + "/" + table + "/quorum";
    LOG_AGGRPROC(3) << "Checking previous quorum status: " << zk_path << "/status";
    DB::Block result;
    int error_code = 0;

    // Use "select * from system.zookeeper where path = '" + zk_path +"' and name = 'status'" to avoid non-node exist
    // zookeeper exception caused by select * from system.zookeeper where path = zk_path + '/status'
    if (loader->executeTableSelectQuery(
            "system.zookeeper", "select * from system.zookeeper where path = '" + zk_path + "' and name = 'status'",
            result, error_code)) {
        if (result.rows() > 0) {
            LOG_AGGRPROC(2) << "Found pending quorum: " << zk_path << "/status, retry later";
            return false;
        } else {
            LOG_AGGRPROC(2) << "No pending quorum: " << zk_path << "/status, retry insert";
            return true;
        }
    } else {
        if (error_code != 0) {
            LOG(ERROR) << "Failed to check quorum status: " << zk_path << ", code: " << error_code << ", retry insert";
        }
        // Avoid unknown error block the insert, insert anyway.
        return true;
    }
}

void BlockSupportedBufferFlushTask::handleError(int error_code, int& max_retry_times) {
    // TODO: Re-enable quorum status check later after Loader code is stable

    if (error_code == DB::ErrorCodes::UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE) {
        max_retry_times = MAX_NUMBER_OF_LOADING_RETRIES_QUORUM;
        // load_predicate = boost::bind(&BlockSupportedBufferFlushTask::checkQuorumStatus, this);
    } else if (error_code == DB::ErrorCodes::TABLE_IS_READ_ONLY || error_code == DB::ErrorCodes::TIMEOUT_EXCEEDED) {
        max_retry_times = MAX_NUMBER_OF_LOADING_RETRIES_READONLY;
        // load_predicate = boost::bind(&BlockSupportedBufferFlushTask::heartbeat, this);
    }

    // Increase error counter for the error passed back from Clickhouse
    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
    std::string error_code_str = std::to_string(error_code);
    loader_metrics->errors_passed_from_clickhouse_total->labels({{"table", table}, {"error_code", error_code_str}})
        .increment(1);

    load_predicate = nullptr;
}

void BlockSupportedBufferFlushTask::doBlockInsertion(int& max_retry_times) {
    try {
        // Create and release a connection each round.
        loader = std::make_unique<AggregatorLoader>(context, loader_manager.getConnectionPool(),
                                                    loader_manager.getConnectionParameters());
        bool loader_connection_initialized = loader->init();
        LOG_AGGRPROC(3) << "FlushTask's BlockInsertion " << assigned_task_id
                        << " initialized DB connection: " << (loader_connection_initialized ? "success" : "fail");

        if (loader_connection_initialized) {
            // this needs to check different status, for example, incorrect schema or due to communication failure or
            // due to replication at the backend.
            LOG_AGGRPROC(3) << "FlushTask's BlockInsertion " << assigned_task_id
                            << " start to load buffer for table: " << table;

            bool predicate_ok = true;
            if (load_predicate != nullptr) {
                if (load_predicate()) {
                    LOG_AGGRPROC(3) << "FlushTask's BlockInsertion Load predicate ok for task (id): "
                                    << assigned_task_id;
                } else {
                    LOG(ERROR) << "Load predicate failed for task (id): " << assigned_task_id;
                    predicate_ok = false;
                }
            }
            if (predicate_ok) {
                int error_code = 0;

                // NOTE: how can we cancel buffer loading if kafka connector is shutdown already?
                // the buffer loading with quorum = 2 can have long wait time and the main thread may start to terminate
                // itself.
                loading_succeeded = loader->load_buffer(table, table_insert_query, block_to_load, error_code);
#ifdef _PRERELEASE
                if (flip::Flip::instance().test_flip("[load-buffer-long-time]")) {
                    LOG(INFO) << "[load-buffer-long-time]: FlushTask's BlockInsertion simulates aggregator to "
                                 "clickhouse use too long time bigger than max_poll_interval_ms";
                    auto max_poll_interval_ms = SETTINGS_PARAM(config.kafka.consumerConf.max_poll_interval_ms);
                    std::this_thread::sleep_for(std::chrono::milliseconds(max_poll_interval_ms * 2));
                }
#endif
                if (!loading_succeeded) {
                    handleError(error_code, max_retry_times);
                }
            } else {
                loading_succeeded = false;
                LOG_AGGRPROC(3) << "Predicate failed, skip loading for task " << assigned_task_id;
            }
        } else {
            LOG(ERROR) << "FlushTask " << assigned_task_id
                       << "  can not load buffer due to loader can not be initialized";
            loading_succeeded = false;
        }
    } catch (std::exception& err) {
        LOG(ERROR) << "FlushTask " << assigned_task_id << " caught exception: " << err.what();
    }

    // Release connection
    loader = nullptr;
}

void BlockSupportedBufferFlushTask::loadBuffer() {
    // If kafka connector shutdown happens, immediately exit.
    if (kafka_connector->isRunning()) {
        auto block_insertion_mode =
            with_settings([this](SETTINGS s) { return s.config.blockLoadingToDB.useDistributedLocking; });
        // need to set a metrics on the setting.
        if (block_insertion_mode == 1) {
            loadBufferWithPreventiveLocking();
        } else {
            loadBufferWithoutPreventiveLocking();
        }
    } else {
        LOG(INFO) << " FlushTask with task (id): " << assigned_task_id
                  << " exists with loading-done=true now due to kafka-connector shutdown";
        loading_done = true;
        send_loading_done.set_value();
    }
}

void BlockSupportedBufferFlushTask::loadBufferWithoutPreventiveLocking() {
    executed_times++;

    LOG_AGGRPROC(3) << "FlushTask " << assigned_task_id << " loading block"
                    << " as " << executed_times << " attempt(s)"
                    << " without preventive locking";

    // we can have the re-load here, with: task_handle->scheduleAfter(...), again.
    if (table_insert_query.empty()) {
        table_insert_query = formulateInsertQuery();
        LOG_AGGRPROC(4) << "FlushTask formulated the insert query: " << table_insert_query
                        << " for task (id): " << assigned_task_id << " without preventive locking";
    }

    int max_retry_times = MAX_NUMBER_OF_LOADING_RETRIES;
    size_t retry_after_ms = std::min(executed_times * LOADING_RETRY_DELAY_MS, LOADING_RETRY_MAX_DELAY_MS);

    std::chrono::time_point<std::chrono::high_resolution_clock> load_buffer_start =
        std::chrono::high_resolution_clock::now();

    doBlockInsertion(max_retry_times);

    std::chrono::time_point<std::chrono::high_resolution_clock> load_buffer_end =
        std::chrono::high_resolution_clock::now();
    uint64_t time_diff =
        std::chrono::duration_cast<std::chrono::microseconds>(load_buffer_end.time_since_epoch()).count() -
        std::chrono::duration_cast<std::chrono::microseconds>(load_buffer_start.time_since_epoch()).count();

    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
    loader_metrics->block_loading_time_metrics->labels({{"table", table}}).observe(time_diff);

    if (loading_succeeded) {
        if (executed_times > 1) {
            LOG_AGGRPROC(2) << "FlushTask " << assigned_task_id << " successfully loaded block after " << executed_times
                            << " attempts"
                            << " without preventive locking";
        } else {
            LOG_AGGRPROC(2) << "FlushTask " << assigned_task_id
                            << " successfully loaded block without preventive locking";
        }
        loader_metrics->block_loading_number_of_retries_metrics->labels({{"table", table}}).observe(executed_times - 1);
        // notify the possible waiting thread.
        loading_done = true;
        send_loading_done.set_value();
        load_predicate = nullptr;

        // update lag time only when loading succeeds
        updateLagTimeOnLoadedBlock();
    } else {
        // Adjust to avoid retry storm while recovering at same time point.
        retry_after_ms +=
            (std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                 .count() %
             LOADING_RETRY_DELAY_MS);

        LOG(WARNING) << "FlushTask " << assigned_task_id << " failed to load block after " << executed_times
                     << " attempts"
                     << " without preventive locking";
        // Need to check the failure result, and then issue retry, if kafka connector is still running.
        if ((executed_times.load(std::memory_order_relaxed) < max_retry_times) && kafka_connector->isRunning()) {
            LOG(WARNING) << "FlushTask " << assigned_task_id << " failed and retry in " << retry_after_ms << " ms"
                         << " without preventive locking";
            reloadBufferAfter(retry_after_ms);
        } else {
            if (!kafka_connector->isRunning()) {
                LOG(ERROR) << "FlushTask with id: " << assigned_task_id << " finally failed to load block after "
                           << executed_times << " attempts due to kafka connector is not running "
                           << " in non-preventive locking mode";
            } else {
                LOG(ERROR) << "FlushTask " << assigned_task_id << " finally failed to load block after "
                           << executed_times << " attempts"
                           << " in non-preventive locking mode";
            }

            loader_metrics->blocks_failed_to_be_persisted_total->labels({{"table", table}}).increment();
            // notify the possible waiting thread
            loading_done = true;
            send_loading_done.set_value();
        }
    }
}

void BlockSupportedBufferFlushTask::loadBufferWithPreventiveLocking() {
    executed_times++;

    LOG_AGGRPROC(3) << "FlushTask " << assigned_task_id << " loading block with preventive locking"
                    << " with " << executed_times << " attempt(s)";

    // We can have the re-load here, with: task_handle->scheduleAfter(...), again.
    if (table_insert_query.empty()) {
        table_insert_query = formulateInsertQuery();
        LOG_AGGRPROC(4) << "FlushTask formulated the insert query: " << table_insert_query
                        << " for task (id): " << assigned_task_id << " with preventive locking";
    }

    int max_retry_times = MAX_NUMBER_OF_LOADING_RETRIES;
    size_t retry_after_ms = std::min(executed_times * LOADING_RETRY_DELAY_MS, LOADING_RETRY_MAX_DELAY_MS);

    std::chrono::time_point<std::chrono::high_resolution_clock> load_buffer_start =
        std::chrono::high_resolution_clock::now();

    size_t max_retries_on_locking = 30;
    size_t current_retries_on_locking = 0;

    bool lock_acquired = false;

    std::shared_ptr<DistributedLockingMetrics> distributed_locking_metrics =
        MetricsCollector::instance().getDistributedLockingMetrics();

    // Ensure that the per-table local lock is created
    LocalLoaderLockManager::getInstance().ensureLockExists(table);
    LocalLoaderLock::LocalLoaderLockPtr local_lock_ptr = LocalLoaderLockManager::getInstance().getLock(table);
    CHECK(local_lock_ptr != nullptr) << "local lock manager should return an non-empty local lock object";

    // Ensure that the per-table distributed lock is created at the "table" level. The concrete lock has the table name
    // as the pre-fix
    DistributedLoaderLockManager::getInstance().ensureLockExists(table);
    DistributedLoaderLock::DistributedLoaderLockPtr distributed_lock_ptr =
        DistributedLoaderLockManager::getInstance().getLock(table);
    CHECK(distributed_lock_ptr != nullptr)
        << "distributed lock manager should return an non-empty distributed lock object";
    std::string lock_path = DistributedLoaderLockManager::getInstance().getLockPath(table);

    //(1) local locking across multiple kafka-connectors, first
    std::lock_guard<std::mutex> lck(local_lock_ptr->loader_lock);

    //(2) distributed locking, second
    std::chrono::time_point<std::chrono::high_resolution_clock> distributed_locking_start_time =
        std::chrono::high_resolution_clock::now();
    while (!lock_acquired && (current_retries_on_locking < max_retries_on_locking) && kafka_connector->isRunning()) {
        // Only one of the two can be true at the end due to Zookeeper exception raised in try-lock or un-lock
        bool locking_issue_experienced = true;
        try {
            // (1) get lock
            auto lock =
                createSimpleZooKeeperLock(distributed_lock_ptr->tryGetZooKeeper(), distributed_lock_ptr->getLockPath(),
                                          distributed_lock_ptr->getLockName(), "");
            if (lock->tryLock()) {
                LOG_AGGRPROC(4) << "In distributed locking, successfully acquired distributed lock on lock path: "
                                << distributed_lock_ptr->getLockPath()
                                << " and lock name: " << distributed_lock_ptr->getLockName();
                lock_acquired = true;
                std::chrono::time_point<std::chrono::high_resolution_clock> distributed_locking_end_time =
                    std::chrono::high_resolution_clock::now();
                uint64_t distributed_locking_time_diff = std::chrono::duration_cast<std::chrono::microseconds>(
                                                             distributed_locking_end_time.time_since_epoch())
                                                             .count() -
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        distributed_locking_start_time.time_since_epoch())
                        .count();
                distributed_locking_metrics->distributed_locking_lock_distribution_time
                    ->labels({{"table", table}, {"status", "succeeded"}})
                    .observe(distributed_locking_time_diff);
                // (2) perform actual block insertion to the local db server. All of the related exceptions are
                // captured.
                //     This only happen when kafka-connector is still running
                if (kafka_connector->isRunning()) {
                    doBlockInsertion(max_retry_times);
                }
            }

            locking_issue_experienced = false;

            // (3) release lock
            lock->unlock();
            LOG_AGGRPROC(4) << "In distributed locking, successfully acquired distributed lock";
        } catch (const Coordination::Exception& e) {
            if (locking_issue_experienced) {
                LOG(ERROR) << "In distributed locking, experienced issue at locking session"; // need to have metrics
            } else {
                LOG(ERROR) << "In distributed locking, experienced issue at unlocking session"; // need to have metrics
            }

            if (Coordination::isHardwareError(e.code)) {
                LOG(ERROR) << "In distributed locking, to restart ZooKeeper session after: "
                           << DB::getCurrentExceptionMessage(false);

                distributed_locking_metrics->distributed_locking_zookeeper_exception_total
                    ->labels({{"table", table}, {"category", "hardware"}})
                    .increment(1);
                std::chrono::time_point<std::chrono::high_resolution_clock> failure_handling_start_time =
                    std::chrono::high_resolution_clock::now();
                while (true) {
                    if (kafka_connector->isRunning()) {
                        try {
                            distributed_lock_ptr->getAndSetZooKeeper();
                            LOG(INFO) << "ZooKeeper session restarted for table : " << table;
                            break;
                        } catch (...) {
                            LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                            using namespace std::chrono_literals;
                            if (kafka_connector->isRunning()) {
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                            }
                        }
                    } else {
                        break;
                    }
                }

                std::chrono::time_point<std::chrono::high_resolution_clock> failure_handling_end_time =
                    std::chrono::high_resolution_clock::now();
                uint64_t time_diff =
                    std::chrono::duration_cast<std::chrono::microseconds>(failure_handling_end_time.time_since_epoch())
                        .count() -
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        failure_handling_start_time.time_since_epoch())
                        .count();

                distributed_locking_metrics->distributed_locking_zookeeper_exception_handling_time
                    ->labels({{"table", table}, {"category", "hardware"}})
                    .observe(time_diff);
            } else if (e.code == Coordination::Error::ZNONODE || e.code == Coordination::Error::ZNODEEXISTS) {
                // That is OK, for example, due to manual deletion of the lock node, thus to ignore it
                LOG(ERROR) << "In distributed locking, experienced ZooKeeper error with error: "
                           << Coordination::errorMessage(e.code) << DB::getCurrentExceptionMessage(true);
                distributed_locking_metrics->distributed_locking_zookeeper_exception_total
                    ->labels({{"table", table}, {"category", "znode"}})
                    .increment(1);
            } else {
                // Other zookeeper related failure, let's reset the session
                LOG(ERROR) << "In distributed locking, Unexpected ZooKeeper error with error: "
                           << Coordination::errorMessage(e.code) << DB::getCurrentExceptionMessage(true)
                           << ". Reset the session .";
                distributed_locking_metrics->distributed_locking_zookeeper_exception_total
                    ->labels({{"table", table}, {"category", "other"}})
                    .increment(1);
                std::chrono::time_point<std::chrono::high_resolution_clock> failure_handling_start_time =
                    std::chrono::high_resolution_clock::now();

                while (true) {
                    if (kafka_connector->isRunning()) {
                        try {
                            distributed_lock_ptr->getAndSetZooKeeper();
                            LOG(INFO) << "In distributed locking, ZooKeeper session restarted for table : " << table;
                            break;
                        } catch (...) {
                            LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                            using namespace std::chrono_literals;
                            if (kafka_connector->isRunning()) {
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                            }
                        }
                    } else {
                        break;
                    }
                }

                std::chrono::time_point<std::chrono::high_resolution_clock> failure_handling_end_time =
                    std::chrono::high_resolution_clock::now();
                uint64_t time_diff =
                    std::chrono::duration_cast<std::chrono::microseconds>(failure_handling_end_time.time_since_epoch())
                        .count() -
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        failure_handling_start_time.time_since_epoch())
                        .count();

                distributed_locking_metrics->distributed_locking_zookeeper_exception_handling_time
                    ->labels({{"table", table}, {"category", "other"}})
                    .observe(time_diff);
            }
        } catch (...) {
            // Other non-zookeeper expected failure, which should not happen.
            LOG(ERROR) << "In distributed locking, unexpected error not due to ZooKeeper related exception.."
                       << DB::getCurrentExceptionMessage(true) << ". Continue on distributed locking loop. ";
        }

        if (!lock_acquired) {
            current_retries_on_locking++;
            // This gives us 500 * max_retries_on_locking = 500*30 = 15 seconds
            if (current_retries_on_locking > 0 && current_retries_on_locking % 5 == 0) {
                LOG_AGGRPROC(2) << "In distributed locking, current distributed locking retry times is: "
                                << current_retries_on_locking;
            }

            if (kafka_connector->isRunning()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
    }

    // ToDo: on metrics on lock not acquired after the above retry loop.
    if (lock_acquired) {
        distributed_locking_metrics->distributed_locking_successful_total->labels({{"table", table}}).increment(1);
        // Successful distributed locking time has been captured earlier right after lock_acquired to be true.
    } else {
        // Failed distributed locking time
        std::chrono::time_point<std::chrono::high_resolution_clock> distributed_locking_end_time =
            std::chrono::high_resolution_clock::now();
        uint64_t distributed_locking_time_diff =
            std::chrono::duration_cast<std::chrono::microseconds>(distributed_locking_end_time.time_since_epoch())
                .count() -
            std::chrono::duration_cast<std::chrono::microseconds>(distributed_locking_start_time.time_since_epoch())
                .count();
        distributed_locking_metrics->distributed_locking_lock_distribution_time
            ->labels({{"table", table}, {"status", "failed"}})
            .observe(distributed_locking_time_diff);
        distributed_locking_metrics->distributed_locking_failed_total->labels({{"table", table}}).increment(1);
    }

    distributed_locking_metrics->distributed_locking_retried_total->labels({{"table", table}})
        .increment(current_retries_on_locking);

    std::chrono::time_point<std::chrono::high_resolution_clock> load_buffer_end =
        std::chrono::high_resolution_clock::now();
    uint64_t time_diff =
        std::chrono::duration_cast<std::chrono::microseconds>(load_buffer_end.time_since_epoch()).count() -
        std::chrono::duration_cast<std::chrono::microseconds>(load_buffer_start.time_since_epoch()).count();

    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
    loader_metrics->block_loading_time_metrics->labels({{"table", table}}).observe(time_diff);

    if (loading_succeeded) {
        if (executed_times > 1) {
            LOG_AGGRPROC(2) << "FlushTask " << assigned_task_id << " successfully loaded block after " << executed_times
                            << " attempts"
                            << " with preventive locking";
        } else {
            LOG_AGGRPROC(2) << "FlushTask " << assigned_task_id << " successfully loaded block with preventive locking";
        }
        loader_metrics->block_loading_number_of_retries_metrics->labels({{"table", table}}).observe(executed_times - 1);
        // notify the possible waiting thread.
        loading_done = true;
        send_loading_done.set_value();
        load_predicate = nullptr;
        // update lag time only when loading succeeds
        updateLagTimeOnLoadedBlock();

    } else {
        // The retry herein includes the situation that after the distributed locking retry loop, the lock is still not
        // acquired, as actual loading can only happen after the lock is acquired.
        // Furthermore, to adjust retry delay time to avoid retry storm while recovering at same time point.
        retry_after_ms +=
            (std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                 .count() %
             LOADING_RETRY_DELAY_MS);

        LOG(WARNING) << "FlushTask " << assigned_task_id << " failed to load block after " << executed_times
                     << " attempts"
                     << " with preventive locking";
        // need to check the failure result, and then issue retry;
        if ((executed_times.load(std::memory_order_relaxed) < max_retry_times) && kafka_connector->isRunning()) {
            LOG(WARNING) << "FlushTask " << assigned_task_id << " failed and retry in " << retry_after_ms << " ms"
                         << " with preventive locking";
            reloadBufferAfter(retry_after_ms);
        } else {
            if (!kafka_connector->isRunning()) {
                LOG(ERROR) << "FlushTask " << assigned_task_id << " finally failed to load block after "
                           << executed_times << " attempts due to kafka connector is not running "
                           << " with preventive locking";
            } else {
                LOG(ERROR) << "FlushTask " << assigned_task_id << " finally failed to load block after "
                           << executed_times << " attempts"
                           << " with preventive locking";
            }

            loader_metrics->blocks_failed_to_be_persisted_total->labels({{"table", table}}).increment();
            // Notify the possible waiting thread
            loading_done = true;
            send_loading_done.set_value();
        }
    }
}

void BlockSupportedBufferFlushTask::start() {
    if (loading_started.load()) {
        LOG(FATAL) << "FlushTask already started and should not restart multiple times, ignore";
        return;
    }
    LOG_AGGRPROC(2) << "FlushTask " << assigned_task_id << " starting to insert into table " << table << " with "
                    << block_to_load.rows() << " rows, " << block_to_load.allocatedBytes() << " bytes";
    // increase the number of the blocks to be flushed.
    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
    loader_metrics->bytes_in_blocks_from_batched_kafka_messages_stored_metrics->labels({{"table", table}})
        .observe(block_to_load.allocatedBytes());
    loader_metrics->rows_in_blocks_from_batched_kafka_messages_stored_metrics->labels({{"table", table}})
        .observe(block_to_load.rows());
    loader_metrics->total_blocks_from_batched_kafka_messages_stored_metrics->labels({{"table", table}}).increment(1);

    task_handle =
        pool.createTask("BlockSupportedBufferFlushTask", boost::bind(&BlockSupportedBufferFlushTask::loadBuffer, this));
    if (!task_handle->activateAndSchedule()) {
        LOG(ERROR) << "Failed to activate FlushTask " << assigned_task_id;
        loading_done = true;
        send_loading_done.set_value();
        return;
    }
    loading_started.store(true);
    LOG_AGGRPROC(3) << "FlushTask " << assigned_task_id << " started";
}

bool BlockSupportedBufferFlushTask::isDone() { return !loading_started.load() || loading_done.load(); }

// NOTE: should we have the status to be return on whether successfully flushed or not.
bool BlockSupportedBufferFlushTask::blockWait() {
    if (!loading_started.load()) {
        LOG(FATAL) << "FlushTask " << assigned_task_id << " not started yet before blockWait";
        return false;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> task_wait_start =
        std::chrono::high_resolution_clock::now();
    auto [var_zone, var_topic] = with_settings([this](SETTINGS s) {
        size_t idx = kafka_connector->getId();
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });

    if (!loading_done.load()) {
        LOG_AGGRPROC(3) << "FlushTask " << assigned_task_id << " waiting ...";
        send_loading_future.wait();
    }

    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();

    if (loading_succeeded) {
        LOG_AGGRPROC(2) << "FlushTask " << assigned_task_id << " finished as SUCCESS";
        loader_metrics->total_blocks_from_batched_kafka_messages_persisted_metrics->labels({{"table", table}})
            .increment();
    } else {
        LOG(ERROR) << "FlushTask " << assigned_task_id << " finished as FAILED";
        loader_metrics->bytes_of_blocks_failed_to_be_persisted_total->labels({{"table", table}})
            .increment(block_to_load.allocatedBytes());
        return false;
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> task_wait_end =
        std::chrono::high_resolution_clock::now();
    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();
    uint64_t time_diff =
        std::chrono::duration_cast<std::chrono::microseconds>(task_wait_end.time_since_epoch()).count() -
        std::chrono::duration_cast<std::chrono::microseconds>(task_wait_start.time_since_epoch()).count();
    kafkaconnector_metrics->task_completion_waittime_by_kafka_connectors
        ->labels({{"on_topic", var_topic}, {"on_zone", var_zone}})
        .observe(time_diff);

    return true;
}

void BlockSupportedBufferFlushTask::moveBlock(DB::Block& other_block) {
    block_to_load = SerializationHelper::getBlockDefinition(columns_definition);

    DB::MutableColumns columns = other_block.cloneEmptyColumns();
    DB::MutableColumns other_columns = other_block.mutateColumns();

    size_t number_of_columns = columns.size();
    for (size_t column_index = 0; column_index < number_of_columns; column_index++) {
        columns[column_index]->insertRangeFrom(*other_columns[column_index], 0, other_columns[column_index]->size());
    }

    // construct the current block
    block_to_load.setColumns(std::move(columns));
    // TODO: Try with swap, block_to_load.swap(other_block);
}

// Only when block loading succeeded, we will update lag time. If loading un-successful, the lag time will be recorded
// in the future loader that can be from a different replica.
void BlockSupportedBufferFlushTask::updateLagTimeOnLoadedBlock() {
    // now () - minimum time stamp for all of the rows accumulated in the block
    int64_t lag_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count() -
        minmax_msg_timestamp.first;
    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
    loader_metrics->end_to_end_lag_time_at_loader->labels({{"table", table}}).observe(lag_time);
}

BlockSupportedBufferFlushTask::~BlockSupportedBufferFlushTask() {
    LOG_AGGRPROC(4) << "FlushTask is shutdown in destructor "
                    << " for task (id): " << assigned_task_id;
    if (loading_started.load() && !loading_done.load()) {
        LOG_AGGRPROC(3) << "FlushTask " << assigned_task_id << " waiting to finish in destructor";
        send_loading_future.wait();
        LOG_AGGRPROC(3) << "FlushTask " << assigned_task_id << " finished";
    }
}

} // namespace nuclm
