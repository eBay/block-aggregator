/************************************************************************
Modifications Copyright 2021, eBay, Inc.

Original Copyright:
See URL: https://github.com/ClickHouse/ClickHouse

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

#include "common/logging.hpp"

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/Exception.h>

#include <boost/noncopyable.hpp>

#include <mutex>

namespace nuclm {

class ZooKeeperLock;

class ZooKeeperHolder : public boost::noncopyable {
    friend class ZooKeeperLock;

  protected:
    class UnstorableZookeeperHandler;

  public:
    ZooKeeperHolder() = default;

    template <typename... Args> void init(Args&&... args);

    bool isInitialized() const { return ptr != nullptr; }

    void initFromInstance(const zkutil::ZooKeeper::Ptr& zookeeper_ptr);

    UnstorableZookeeperHandler getZooKeeper();
    bool replaceZooKeeperSessionToNewOne();

    bool isSessionExpired() const;

  protected:
    class UnstorableZookeeperHandler {
      public:
        UnstorableZookeeperHandler(zkutil::ZooKeeper::Ptr zk_ptr_);

        explicit operator bool() const { return bool(zk_ptr); }
        bool operator==(std::nullptr_t) const { return zk_ptr == nullptr; }
        bool operator!=(std::nullptr_t) const { return !(*this == nullptr); }

        zkutil::ZooKeeper* operator->();
        const zkutil::ZooKeeper* operator->() const;
        zkutil::ZooKeeper& operator*();
        const zkutil::ZooKeeper& operator*() const;

      private:
        zkutil::ZooKeeper::Ptr zk_ptr;
    };

  private:
    mutable std::mutex mutex;
    zkutil::ZooKeeper::Ptr ptr;

    static std::string nullptr_exception_message;
};

template <typename... Args> void ZooKeeperHolder::init(Args&&... args) {
    ptr = std::make_shared<zkutil::ZooKeeper>(std::forward<Args>(args)...);
}

using ZooKeeperHolderPtr = std::shared_ptr<ZooKeeperHolder>;

/**
 * Note this class is taken from the ZooKeeper implementation in src/Interpreters/DDLWorker.cpp, in which this class
 * is privately used in the DDLWorker's implementation and not exported externally.  We use the ZooKeeper
 * lock to prevent the contention on concurrent loading of the same table from multiple replicas into the ClickHouse
 * shards. At the ClickHouse side, the configuration parameter of "insert_quorum_parallel" is set to be false, so that
 * at any given time, for a given table, only one replica can perform loading to the shard and thus to not to introduce
 * high pressure to the ZooKeeper cluster. The locking provided by this ZooKeeperLock  class is to preventatively
 * minimize such contention on table loading at the client side. Even if the locking fails due to some failure
 * conditions, the ClickHouse side still has the enforcement on only a single replica can perform table loading at any
 * given time. Thus what we really need from this zookeeper lock is best-effort preventative locking.
 */
class ZooKeeperLock {
  public:
    ZooKeeperLock(ZooKeeperHolderPtr zookeeper_holder_, const std::string& lock_prefix_, const std::string& lock_name_,
                  const std::string& lock_message_ = "", bool create_parent_path_ = false) :
            zookeeper_holder(zookeeper_holder_),
            lock_path(lock_prefix_ + "/" + lock_name_),
            lock_message(lock_message_) {
        auto zookeeper = zookeeper_holder->getZooKeeper();
        if (create_parent_path_)
            zookeeper->createAncestors(lock_prefix_);

        zookeeper->createIfNotExists(lock_prefix_, "");
    }

    ZooKeeperLock(const ZooKeeperLock&) = delete;
    ZooKeeperLock(ZooKeeperLock&& lock) = default;
    ZooKeeperLock& operator=(const ZooKeeperLock&) = delete;

    ~ZooKeeperLock() {
        try {
            unlock();
        } catch (...) {
            // DB::tryLogCurrentException(__PRETTY_FUNCTION__);
            std::string err_msg = "Zookeeper unlock fails on lock_path: " + lock_path +
                " with lock_message: " + lock_message + " " + DB::getCurrentExceptionMessage(true);
            LOG(ERROR) << err_msg;
        }
    }

    enum Status {
        UNLOCKED,
        LOCKED_BY_ME,
        LOCKED_BY_OTHER,
    };

    Status tryCheck() const;

    void unlock();
    void unlockAssumeLockNodeRemovedManually();

    bool tryLock();

    /// путь к ноде блокировки в zookeeper
    const std::string& getPath() { return lock_path; }

  private:
    ZooKeeperHolderPtr zookeeper_holder;
    using ZooKeeperHandler = ZooKeeperHolder::UnstorableZookeeperHandler;
    std::unique_ptr<ZooKeeperHandler> locked;

    std::string lock_path;
    std::string lock_message;
};

} // namespace nuclm
