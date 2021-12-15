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
#include "monitor/metrics_collector.hpp"

#include <Aggregator/DistributedLoaderLock.h>

#include <boost/algorithm/string.hpp>

#include <random>
#include <functional>
#include <algorithm>
#include <vector>
#include <memory>

namespace nuclm {

std::shared_ptr<zkutil::ZooKeeper> LoaderZooKeeperSession::getZooKeeper() {
    std::lock_guard lock(zookeeper_session_mutex);

    if (!zookeeper) {
        // retrieve from configuration the zookeeper host list
        auto zookeeper_hosts = with_settings([this](SETTINGS s) { return s.config.zookeeper.endpoints; });

        // shuffle the zookeeper host lists for load balancing, to mimic what is done when loading from the
        // XML configuration file
        std::vector<std::string> zookeeper_host_addresses;
        boost::split(zookeeper_host_addresses, zookeeper_hosts, boost::is_any_of(","));

        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(zookeeper_host_addresses.begin(), zookeeper_host_addresses.end(), g);

        std::stringstream zookeeper_hosts_description;

        for (auto host = zookeeper_host_addresses.begin(); host != zookeeper_host_addresses.end(); ++host) {
            if (std::distance(host, zookeeper_host_addresses.end()) == 1) {
                zookeeper_hosts_description << *host;
            } else {
                zookeeper_hosts_description << *host << ",";
            }
        }

        std::string shuffled_zookeeper_hosts = zookeeper_hosts_description.str();
        LOG_AGGRPROC(1) << "shuffled zookeeper hosts are: " << shuffled_zookeeper_hosts;
        // Use the default implementation "zookeeper" in zkutil::ZooKeeper's constructor
        zookeeper = std::make_shared<zkutil::ZooKeeper>(shuffled_zookeeper_hosts);
        LOG_AGGRPROC(1) << "finished creation of the zookeeper instance";
    } else if (zookeeper->expired()) {
        zookeeper = zookeeper->startNewSession();
        LOG_AGGRPROC(1) << "finished starting a new zookeeper session after previous one expired";
    }

    return zookeeper;
}

LocalLoaderLock::LocalLoaderLockPtr LocalLoaderLockManager::getOrCreateLock(const std::string& table_name) {
    std::lock_guard lock(loader_lock_mutex);
    auto search_found = loader_locks.find(table_name);
    if (search_found == loader_locks.end()) {
        LocalLoaderLock::LocalLoaderLockPtr lock_ptr = std::make_shared<LocalLoaderLock>(table_name);
        loader_locks[table_name] = lock_ptr;
        return lock_ptr;
    } else {
        return search_found->second;
    }
}

bool LocalLoaderLockManager::ensureLockExists(const std::string& table_name) {
    LocalLoaderLock::LocalLoaderLockPtr lock_ptr = LocalLoaderLockManager::getInstance().getOrCreateLock(table_name);
    return (lock_ptr != nullptr);
}

LocalLoaderLock::LocalLoaderLockPtr LocalLoaderLockManager::getLock(const std::string& table_name) {
    std::lock_guard lock(loader_lock_mutex);
    auto search_found = loader_locks.find(table_name);
    if (search_found == loader_locks.end()) {
        return nullptr;
    } else {
        return search_found->second;
    }
}

DistributedLoaderLock::DistributedLoaderLockPtr
DistributedLoaderLockManager::getOrCreateLock(const std::string& table_name, const std::string& lock_path,
                                              const std::string& lock_path_prefix) {
    std::lock_guard lock(loader_lock_mutex);
    auto search_found = loader_locks.find(table_name);
    if (search_found == loader_locks.end()) {
        DistributedLoaderLock::DistributedLoaderLockPtr lock_ptr =
            std::make_shared<DistributedLoaderLock>(lock_path, lock_path_prefix, "lock");
        loader_locks[table_name] = lock_ptr;
        return lock_ptr;
    } else {
        return search_found->second;
    }
}

DistributedLoaderLock::DistributedLoaderLockPtr DistributedLoaderLockManager::getLock(const std::string& table_name) {
    std::lock_guard lock(loader_lock_mutex);
    auto search_found = loader_locks.find(table_name);
    if (search_found == loader_locks.end()) {
        return nullptr;
    } else {
        return search_found->second;
    }
}

// The lock name called "lock" need to combine with lock_path to form the full ZK path of: lock_path/lock.
std::string DistributedLoaderLockManager::getLockPath(const std::string& table_name) {
    auto shard_id = with_settings([this](SETTINGS s) { return s.config.identity.shardId; });
    std::string lock_path = "/clickhouse_aggregator/tables/" + shard_id + "/" + table_name;
    return lock_path;
}

std::string DistributedLoaderLockManager::getLockPathPrefix(const std::string& table_name) {
    auto shard_id = with_settings([this](SETTINGS s) { return s.config.identity.shardId; });
    std::string lock_path_prefix = "/clickhouse_aggregator/tables/" + shard_id + "/";
    return lock_path_prefix;
}

bool DistributedLoaderLockManager::ensureLockExists(const std::string& table_name) {
    std::string lock_path = DistributedLoaderLockManager::getInstance().getLockPath(table_name);
    std::string lock_path_prefix = DistributedLoaderLockManager::getInstance().getLockPathPrefix(table_name);
    DistributedLoaderLock::DistributedLoaderLockPtr lock_ptr =
        DistributedLoaderLockManager::getInstance().getOrCreateLock(table_name, lock_path, lock_path_prefix);
    if (!lock_ptr->isLockInitialized()) {
        lock_ptr->initLock();
        LOG_AGGRPROC(1) << "distributed lock for table: " << table_name << " initialized with lock path: " << lock_path;
    }
    return true;
}

// In the case of two kafka connectors to come to initialize the per-table lock when a table is created when the
// process is running, then both kafka connectors will get the same table lock and come to initialize it, they will
// still have the same lock being initialized at the end, as each step is idempotent.
void DistributedLoaderLock::initLock() {
    std::shared_ptr<DistributedLockingMetrics> distributed_locking_metrics =
        MetricsCollector::instance().getDistributedLockingMetrics();
    if (!lock_initialized.load()) {
        do {
            try {
                auto zookeeper = getAndSetZooKeeper();
                LOG_AGGRPROC(1) << "finished getting or creating a zookeeper instance";
                // Create a persistent node. Do nothing if the node already exists.
                zookeeper->createAncestors(zookeeper_lock_path_prefix);
                LOG_AGGRPROC(1) << "finished creating ancestors for lock prefix: " << zookeeper_lock_path_prefix;
                zookeeper->createIfNotExists(zookeeper_lock_path, "");
                lock_initialized = true;
                LOG_AGGRPROC(1) << "initialized zookeeper lock at lock path: " << zookeeper_lock_path;
            } catch (const Coordination::Exception& e) {
                LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                LOG_AGGRPROC(1) << "encountered zookeeper related-exception at initializing lock at lock path: "
                                << zookeeper_lock_path << " and lock prefix: " << zookeeper_lock_path_prefix
                                << " with exception: " << Coordination::errorMessage(e.code);
                // When initialization being trapped and in continuous exception capturing and retry, the counter will
                // keep increasing and allow us to receive the alert.
                distributed_locking_metrics->distributed_locking_trapped_at_initialization_total
                    ->labels({{"lock_name", zookeeper_lock_name}})
                    .increment();

            } catch (...) {
                LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                LOG_AGGRPROC(1) << "encountered non-zookeeper related-exception at initializing lock at lock path: "
                                << zookeeper_lock_path << " and lock prefix: " << zookeeper_lock_path_prefix;
                // When initialization being trapped and in continuous exception capturing and retry, the counter will
                // keep increasing and allow us to receive the alert.
                distributed_locking_metrics->distributed_locking_trapped_at_initialization_total
                    ->labels({{"lock_name", zookeeper_lock_name}})
                    .increment();
            }

            /// Avoid busy loop when ZooKeeper is not available.
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        } while (!lock_initialized.load());
    }
}

DistributedLoaderLock::ZooKeeperPtr DistributedLoaderLock::tryGetZooKeeper() {
    std::lock_guard lock(zookeeper_mutex);
    return current_zookeeper;
}

DistributedLoaderLock::ZooKeeperPtr DistributedLoaderLock::getAndSetZooKeeper() {
    std::lock_guard lock(zookeeper_mutex);

    if (!current_zookeeper || current_zookeeper->expired())
        current_zookeeper = LoaderZooKeeperSession::getInstance().getZooKeeper();

    return current_zookeeper;
}

// The Lock's constructor only constructs the ZK node up to the lock_prefix path.
std::unique_ptr<ZooKeeperLock> createSimpleZooKeeperLock(const std::shared_ptr<zkutil::ZooKeeper>& zookeeper,
                                                         const String& lock_prefix, const String& lock_name,
                                                         const String& lock_message) {
    auto zookeeper_holder = std::make_shared<ZooKeeperHolder>();
    zookeeper_holder->initFromInstance(zookeeper);
    return std::make_unique<ZooKeeperLock>(std::move(zookeeper_holder), lock_prefix, lock_name, lock_message);
}

} // namespace nuclm
