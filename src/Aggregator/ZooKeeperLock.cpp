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

#include <Aggregator/ZooKeeperLock.h>

namespace nuclm {

namespace ErrorCodes {
extern const int LOGICAL_ERROR;
}

ZooKeeperHolder::UnstorableZookeeperHandler ZooKeeperHolder::getZooKeeper() {
    std::unique_lock lock(mutex);
    return UnstorableZookeeperHandler(ptr);
}

void ZooKeeperHolder::initFromInstance(const zkutil::ZooKeeper::Ptr& zookeeper_ptr) { ptr = zookeeper_ptr; }

bool ZooKeeperHolder::replaceZooKeeperSessionToNewOne() {
    std::unique_lock lock(mutex);

    if (ptr.unique()) {
        ptr = ptr->startNewSession();
        return true;
    } else {
        // LOG_ERROR(log, "replaceZooKeeperSessionToNewOne(): Fail to replace zookeeper session to new one because
        // handlers for old zookeeper session still exists.");
        LOG(ERROR) << "replaceZooKeeperSessionToNewOne(): Fail to replace zookeeper session to new one because "
                      "handlers for old zookeeper session still exists.";
        return false;
    }
}

bool ZooKeeperHolder::isSessionExpired() const { return ptr ? ptr->expired() : false; }

std::string ZooKeeperHolder::nullptr_exception_message =
    "UnstorableZookeeperHandler::zk_ptr is nullptr. "
    "ZooKeeperHolder should be initialized before sending any request to ZooKeeper";

ZooKeeperHolder::UnstorableZookeeperHandler::UnstorableZookeeperHandler(zkutil::ZooKeeper::Ptr zk_ptr_) :
        zk_ptr(zk_ptr_) {}

zkutil::ZooKeeper* ZooKeeperHolder::UnstorableZookeeperHandler::operator->() {
    if (zk_ptr == nullptr)
        throw DB::Exception(nullptr_exception_message, ErrorCodes::LOGICAL_ERROR);

    return zk_ptr.get();
}

const zkutil::ZooKeeper* ZooKeeperHolder::UnstorableZookeeperHandler::operator->() const {
    if (zk_ptr == nullptr)
        throw DB::Exception(nullptr_exception_message, ErrorCodes::LOGICAL_ERROR);
    return zk_ptr.get();
}

zkutil::ZooKeeper& ZooKeeperHolder::UnstorableZookeeperHandler::operator*() {
    if (zk_ptr == nullptr)
        throw DB::Exception(nullptr_exception_message, ErrorCodes::LOGICAL_ERROR);
    return *zk_ptr;
}

const zkutil::ZooKeeper& ZooKeeperHolder::UnstorableZookeeperHandler::operator*() const {
    if (zk_ptr == nullptr)
        throw DB::Exception(nullptr_exception_message, ErrorCodes::LOGICAL_ERROR);
    return *zk_ptr;
}

bool ZooKeeperLock::tryLock() {
    auto zookeeper = zookeeper_holder->getZooKeeper();
    if (locked) {

        if (tryCheck() != Status::LOCKED_BY_ME)
            locked.reset();
    } else {
        std::string dummy;
        Coordination::Error code = zookeeper->tryCreate(lock_path, lock_message, zkutil::CreateMode::Ephemeral, dummy);

        if (code == Coordination::Error::ZNODEEXISTS) {
            locked.reset();
        } else if (code == Coordination::Error::ZOK) {
            locked = std::make_unique<ZooKeeperHandler>(zookeeper);
        } else {
            throw Coordination::Exception(code);
        }
    }
    return bool(locked);
}

void ZooKeeperLock::unlock() {
    if (locked) {
        auto zookeeper = zookeeper_holder->getZooKeeper();
        if (tryCheck() == Status::LOCKED_BY_ME)
            zookeeper->remove(lock_path, -1);
        locked.reset();
    }
}

ZooKeeperLock::Status ZooKeeperLock::tryCheck() const {
    auto zookeeper = zookeeper_holder->getZooKeeper();

    Status lock_status;
    Coordination::Stat stat;
    std::string dummy;
    bool result = zookeeper->tryGet(lock_path, dummy, &stat);
    if (!result)
        lock_status = UNLOCKED;
    else {
        if (stat.ephemeralOwner == zookeeper->getClientID())
            lock_status = LOCKED_BY_ME;
        else
            lock_status = LOCKED_BY_OTHER;
    }

    if (locked && lock_status != LOCKED_BY_ME) {
        // LOG_WARNING(log, "Lock is lost. It is normal if session was expired. Path: {}/{}", lock_path, lock_message);
        std::string err_msg = "Lock is lost. It is normal if session was expired, with lock_path: " + lock_path +
            " and lock_message: " + lock_message;
        LOG(WARNING) << err_msg;
    }

    return lock_status;
}

void ZooKeeperLock::unlockAssumeLockNodeRemovedManually() { locked.reset(); }

} // namespace nuclm
