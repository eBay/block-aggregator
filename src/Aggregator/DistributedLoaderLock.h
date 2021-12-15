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

#include "Aggregator/ZooKeeperLock.h"

#include <memory>
#include <mutex>

namespace nuclm {

class LoaderZooKeeperSession {
  public:
    static LoaderZooKeeperSession& getInstance() {
        static LoaderZooKeeperSession instance;
        return instance;
    }

    std::shared_ptr<zkutil::ZooKeeper> getZooKeeper();

  private:
    LoaderZooKeeperSession() : zookeeper{nullptr} {}

    ~LoaderZooKeeperSession() = default;

    LoaderZooKeeperSession(const LoaderZooKeeperSession&) = delete;

    LoaderZooKeeperSession& operator=(const LoaderZooKeeperSession&) = delete;

    std::shared_ptr<zkutil::ZooKeeper> zookeeper;
    std::mutex zookeeper_session_mutex;
};

/**
 * cross-thread locking in a single process to allow one single thread to load a block into the DB server. The locking
 * is per table.
 */
class LocalLoaderLock {
  public:
    using LocalLoaderLockPtr = std::shared_ptr<LocalLoaderLock>;

  public:
    std::mutex loader_lock;
    std::string table_name;

    LocalLoaderLock(const std::string& table_name_) : table_name(table_name_) {}
};

/**
 * The Lock Manager that is responsible for creation and retrieval of the locks across multiple threads in a single
 * process.
 */
class LocalLoaderLockManager {
  public:
    static LocalLoaderLockManager& getInstance() {
        static LocalLoaderLockManager instance;
        return instance;
    }

    LocalLoaderLock::LocalLoaderLockPtr getOrCreateLock(const std::string& table_name);
    LocalLoaderLock::LocalLoaderLockPtr getLock(const std::string& table_name);
    bool ensureLockExists(const std::string& table_name);

  private:
    LocalLoaderLockManager() {}

    ~LocalLoaderLockManager() = default;

    LocalLoaderLockManager(const LocalLoaderLockManager&) = delete;

    LocalLoaderLockManager& operator=(const LocalLoaderLockManager&) = delete;

    std::mutex loader_lock_mutex;
    std::unordered_map<std::string, LocalLoaderLock::LocalLoaderLockPtr> loader_locks;
};

/**
 *  cross-process locking to allow one single process to load a block into the DB server.
 */
class DistributedLoaderLock {
  public:
    using DistributedLoaderLockPtr = std::shared_ptr<DistributedLoaderLock>;
    using ZooKeeperPtr = std::shared_ptr<zkutil::ZooKeeper>;

  public:
    DistributedLoaderLock(const std::string& zookeeper_lock_path_, const std::string& zookeeper_lock_path_prefix_,
                          const std::string& zookeeper_lock_name_) :
            current_zookeeper{nullptr},
            zookeeper_lock_path(zookeeper_lock_path_),
            zookeeper_lock_path_prefix(zookeeper_lock_path_prefix_),
            zookeeper_lock_name(zookeeper_lock_name_),
            lock_initialized{false} {}

    void initLock();

    // cache zookeeper shared pointer locally to avoid every time to check session expired. Only when we
    // encounter session expiration, that is the time that we need to get back to LoaderZooKeeperSession to
    // get a new session
    // Returns cached ZooKeeper session (which may be already expired)
    ZooKeeperPtr tryGetZooKeeper();

    // If necessary, create a new session to replace the expired one and cache it. This method need to be invoked first
    // to allow caching during initialization.
    ZooKeeperPtr getAndSetZooKeeper();

    bool isLockInitialized() { return lock_initialized.load(); }

    std::string getLockName() const { return zookeeper_lock_name; }

    std::string getLockPath() const { return zookeeper_lock_path; }

    std::string getLockPathPrefix() const { return zookeeper_lock_path_prefix; };

  private:
    // to handle sharing by multiple loader threads
    mutable std::mutex zookeeper_mutex;
    ZooKeeperPtr current_zookeeper;

    std::string zookeeper_lock_path;
    std::string zookeeper_lock_path_prefix;
    std::string zookeeper_lock_name;

    std::atomic<bool> lock_initialized;
};

class DistributedLoaderLockManager {
  public:
    static DistributedLoaderLockManager& getInstance() {
        static DistributedLoaderLockManager instance;
        return instance;
    }

    DistributedLoaderLock::DistributedLoaderLockPtr
    getOrCreateLock(const std::string& table_name, const std::string& lock_path, const std::string& lock_path_prefix);
    DistributedLoaderLock::DistributedLoaderLockPtr getLock(const std::string& table_name);
    std::string getLockPath(const std::string& table_name);
    std::string getLockPathPrefix(const std::string& table_name);
    bool ensureLockExists(const std::string& table_name);

  private:
    DistributedLoaderLockManager() {}

    ~DistributedLoaderLockManager() = default;

    DistributedLoaderLockManager(const DistributedLoaderLockManager&) = delete;

    DistributedLoaderLockManager& operator=(const DistributedLoaderLockManager&) = delete;

    std::mutex loader_lock_mutex;
    std::unordered_map<std::string, DistributedLoaderLock::DistributedLoaderLockPtr> loader_locks;
};

// Directly expose the Lock to the caller
std::unique_ptr<ZooKeeperLock> createSimpleZooKeeperLock(const std::shared_ptr<zkutil::ZooKeeper>& zookeeper,
                                                         const String& lock_prefix, const String& lock_name,
                                                         const String& lock_message);

} // namespace nuclm
