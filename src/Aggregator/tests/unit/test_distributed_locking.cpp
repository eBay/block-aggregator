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

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/DistributedLoaderLock.h>
#include <Core/Defines.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/ColumnsDescription.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>

#include <nucolumnar/aggregator/v1/nucolumnaraggregator.pb.h>
#include <nucolumnar/datatypes/v1/columnartypes.pb.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

// for pthread id retrieval
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

const std::string TEST_CONFIG_FILE_PATH_ENV_VAR = "TEST_CONFIG_FILE_PATH";

static std::string getConfigFilePath(const std::string& config_file) {
    const char* env_p = std::getenv(TEST_CONFIG_FILE_PATH_ENV_VAR.c_str());
    if (env_p == nullptr) {
        LOG(ERROR) << "cannot find  TEST_CONFIG_FILE_PATH environment variable....exit test execution...";
        exit(-1);
    }

    std::string path(env_p);
    path.append("/").append(config_file);

    return path;
}

class ContextWrapper {
  public:
    ContextWrapper() :
            shared_context_holder(DB::Context::createShared()),
            context{DB::Context::createGlobal(shared_context_holder.get())} {
        context->makeGlobalContext();
    }

    DB::ContextMutablePtr getContext() { return context; }

    boost::asio::io_context& getIOContext() { return ioc; }

    ~ContextWrapper() { LOG(INFO) << "Global context wrapper is now deleted"; }

  private:
    DB::SharedContextHolder shared_context_holder;
    DB::ContextMutablePtr context;
    boost::asio::io_context ioc{1};
};

/**
 * The related tests will need to be performed in multiple processes
 */
class DistributedLockingRelatedTest : public ::testing::Test {
  protected:
    // Per-test-suite set-up
    // Called before the first test in this test suite
    // Can be omitted if not needed
    // NOTE: this method is not called SetUpTestSuite, as what is described in:
    // https://github.com/google/googletest/blob/master/googletest/docs/advanced.md
    /// Fxied from: https://stackoverflow.com/questions/54468799/google-test-using-setuptestsuite-doesnt-seem-to-work
    static void SetUpTestCase() {
        LOG(INFO) << "SetUpTestCase invoked..." << std::endl;
        shared_context = new ContextWrapper();
    }

    // Per-test-suite tear-down
    // Called after the last test in this test suite.
    // Can be omitted if not needed
    static void TearDownTestCase() {
        LOG(INFO) << "TearDownTestCase invoked..." << std::endl;
        delete shared_context;
        shared_context = nullptr;
    }

    // Define per-test set-up logic as usual
    virtual void SetUp() {
        //...
    }

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    static ContextWrapper* shared_context;
};

ContextWrapper* DistributedLockingRelatedTest::shared_context = nullptr;

// For testing purpose when use a different ZK cluster (not shared by the ClickHouse cluster) to test distributed
// locking.
std::unique_ptr<nuclm::ZooKeeperLock>
createSimpleZooKeeperLockWithAncestorsCreated(const std::shared_ptr<zkutil::ZooKeeper>& zookeeper,
                                              const String& lock_prefix, const String& lock_name,
                                              const String& lock_message) {
    auto zookeeper_holder = std::make_shared<nuclm::ZooKeeperHolder>();
    zookeeper_holder->initFromInstance(zookeeper);
    return std::make_unique<nuclm::ZooKeeperLock>(std::move(zookeeper_holder), lock_prefix, lock_name, lock_message,
                                                  true);
}

TEST_F(DistributedLockingRelatedTest, testObtainZookeeperInstance) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::shared_ptr<zkutil::ZooKeeper> zookeeper_instance = nuclm::LoaderZooKeeperSession::getInstance().getZooKeeper();
    ASSERT_TRUE(zookeeper_instance != nullptr);
}

TEST_F(DistributedLockingRelatedTest, testCreateAncestorsPath) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    // retrieve from configuration the zookeeper host list
    auto zookeeper_hosts = with_settings([this](SETTINGS s) { return s.config.zookeeper.endpoints; });

    const String prefix = "/clickhouse_test/zkutil/watch_get_children_with_chroot";
    LOG(INFO) << "to test create-ancestors for prefix: " << prefix;
    bool result = true;
    try {
        auto zookeeper = std::make_unique<zkutil::ZooKeeper>(zookeeper_hosts);
        zookeeper->createAncestors(prefix + "/");
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        result = false;
    }

    ASSERT_TRUE(result);
}

TEST_F(DistributedLockingRelatedTest, testCreateAncestorsPathWthShuffleHosts) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    // retrieve from configuration the zookeeper host list
    auto zookeeper_hosts = with_settings([this](SETTINGS s) { return s.config.zookeeper.endpoints; });

    const String prefix = "/clickhouse_test-7/zkutil/watch_get_children_with_chroot";
    LOG(INFO) << "to test create-ancestors for prefix: " << prefix;
    bool result = true;
    try {
        std::shared_ptr<zkutil::ZooKeeper> zookeeper_instance =
            nuclm::LoaderZooKeeperSession::getInstance().getZooKeeper();
        zookeeper_instance->createAncestors(prefix + "/");
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        result = false;
    }

    ASSERT_TRUE(result);
}

TEST_F(DistributedLockingRelatedTest, testDistributedLockManagerEnsureDistributedLockExists) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_15";
    bool result = nuclm::DistributedLoaderLockManager::getInstance().ensureLockExists(table_name);

    ASSERT_TRUE(result);
}

TEST_F(DistributedLockingRelatedTest, testDistributedLockManagerEnsureLocalLockExists) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_15";
    // ensure that the per-table locl lock is crated
    nuclm::LocalLoaderLockManager::getInstance().ensureLockExists(table_name);
    nuclm::LocalLoaderLock::LocalLoaderLockPtr local_lock_ptr =
        nuclm::LocalLoaderLockManager::getInstance().getLock(table_name);
    ASSERT_TRUE(local_lock_ptr != nullptr);
}

/**
 * Test the method used to create the final distributed lock
 */
TEST_F(DistributedLockingRelatedTest, testSimpleZooKeeperLockCreation) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_15";
    bool result = true;
    try {
        std::shared_ptr<zkutil::ZooKeeper> zookeeper_instance =
            nuclm::LoaderZooKeeperSession::getInstance().getZooKeeper();
        // the table level zk path
        result = nuclm::DistributedLoaderLockManager::getInstance().ensureLockExists(table_name);
        nuclm::DistributedLoaderLock::DistributedLoaderLockPtr lock_ptr =
            nuclm::DistributedLoaderLockManager::getInstance().getLock(table_name);
        auto lock_path = lock_ptr->getLockPath();
        auto lock_name = lock_ptr->getLockName();

        ASSERT_EQ(lock_name, "lock");

        auto lock = nuclm::createSimpleZooKeeperLock(zookeeper_instance, lock_path, lock_name, "");

        ASSERT_TRUE(lock != nullptr);

        LOG(INFO) << " distributed lock is to be acquired";
        bool get_the_lock = lock->tryLock();
        ASSERT_TRUE(get_the_lock);

        LOG(INFO) << "distributed lock obtained is to be unlocked";
        lock->unlock();
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        result = false;
    }

    ASSERT_TRUE(result);
}

// Note this is to test out in the pre-production environment, the three-node ZK cluster when one node goes down,
// the Aggregator fails on the following loop.
// For manual intensive testing, we can increase max_zk_node_creation_count = 100 or even 1000;
TEST_F(DistributedLockingRelatedTest, testContinousSimpleZooKeeperLockCreation) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_80";
    std::shared_ptr<zkutil::ZooKeeper> zookeeper_instance = nuclm::LoaderZooKeeperSession::getInstance().getZooKeeper();

    int count = 0;
    int max_zk_node_creation_count = 10;
    int shard_id_base = 98000;

    std::string zk_namespace = "zk_cluster_testing"; // so that we can remove the whole path after the testing easily.

    while (count < max_zk_node_creation_count) {
        std::string shard_id = std::to_string(shard_id_base + count);
        std::string lock_path_prefix = "/clickhouse_aggregator/tables/" + zk_namespace + "/" + shard_id + "/";
        std::string lock_path = "/clickhouse_aggregator/tables/" + zk_namespace + "/" + shard_id + "/" + table_name;
        nuclm::DistributedLoaderLock::DistributedLoaderLockPtr lock_ptr =
            nuclm::DistributedLoaderLockManager::getInstance().getOrCreateLock(table_name, lock_path, lock_path_prefix);
        std::atomic<bool> lock_initialized{false};

        std::chrono::time_point<std::chrono::high_resolution_clock> lock_creation_start =
            std::chrono::high_resolution_clock::now();
        if (!lock_initialized.load()) {
            do {
                try {
                    auto zookeeper = lock_ptr->getAndSetZooKeeper();
                    // NOTE: need to check? Create a persistent node. Do nothing is the node already exists.
                    zookeeper->createAncestors(lock_path_prefix);
                    zookeeper->createIfNotExists(lock_path, "");
                    lock_initialized = true;
                    LOG_AGGRPROC(1) << "initialized zookeeper lock at lock path: " << lock_path;
                } catch (const Coordination::Exception& e) {
                    LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                    LOG(INFO) << "encountered zookeeper related-exception at initializing lock at lock path: "
                              << lock_path << " and lock prefix: " << lock_path_prefix
                              << " with exception: " << Coordination::errorMessage(e.code);

                } catch (...) {
                    LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                    LOG(INFO) << "encountered non-zookeeper related-exception at initializing lock at lock path: "
                              << lock_path << " and lock prefix: " << lock_path_prefix;
                }

                /// Avoid busy loop when ZooKeeper is not available.
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            } while (!lock_initialized.load());
        }

        std::chrono::time_point<std::chrono::high_resolution_clock> lock_creation_end =
            std::chrono::high_resolution_clock::now();
        uint64_t time_diff =
            std::chrono::duration_cast<std::chrono::milliseconds>(lock_creation_end.time_since_epoch()).count() -
            std::chrono::duration_cast<std::chrono::milliseconds>(lock_creation_start.time_since_epoch()).count();
        LOG(INFO) << "finish creation lock path for lock: " << lock_path
                  << " and it take time (ms): " << std::to_string(time_diff);

        count++;
    }
}

void SimulatedDoBlockInsertion(size_t sleeptime_on_ms) {
    // need to use scope guard to exit the application.
    LOG(INFO) << "simulate block insertion running ";
    std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
}

void SimulatedProcessIdling(size_t sleeptime_on_ms) {
    // need to use scope guard to exit the application.
    LOG(INFO) << "simulate process is working without lock ";
    std::this_thread::sleep_for(std::chrono::milliseconds(sleeptime_on_ms));
}

/**
 * The code used BlockSupportedBufferFlushTask.cpp
 */
TEST_F(DistributedLockingRelatedTest, testSimpleZooKeeperLockOperationInOneProcess) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    size_t max_retries_on_locking = 20;
    size_t current_retries_on_locking = 0;

    bool lock_acquired = false;
    std::string table = "simple_event_16";
    bool result = true;
    try {
        // Ensure that the per-table distributed lock is created at the "table" level. The concrete lock is under table
        // name.
        nuclm::DistributedLoaderLockManager::getInstance().ensureLockExists(table);
        nuclm::DistributedLoaderLock::DistributedLoaderLockPtr distributed_lock_ptr =
            nuclm::DistributedLoaderLockManager::getInstance().getLock(table);
        CHECK(distributed_lock_ptr != nullptr)
            << "distributed lock manager should return an non-empty distributed lock object";
        std::string lock_path = nuclm::DistributedLoaderLockManager::getInstance().getLockPath(table);

        // ensure that the per-table local lock is crated
        nuclm::LocalLoaderLockManager::getInstance().ensureLockExists(table);
        nuclm::LocalLoaderLock::LocalLoaderLockPtr local_lock_ptr =
            nuclm::LocalLoaderLockManager::getInstance().getLock(table);
        CHECK(local_lock_ptr != nullptr) << "local lock manager should return an non-empty local lock object";

        //(1) local locking across multiple kafka-connector first
        std::lock_guard<std::mutex> lck(local_lock_ptr->loader_lock);
        pid_t process_id = getpid();
        //(2) distributed locking second
        while (!lock_acquired && (current_retries_on_locking < max_retries_on_locking)) {

            // Only one of the two can be true at the end due to Zookeeper exception raised in try-lock or un-lock
            bool locking_issue_experienced = true;
            try {
                // (1) get lock
                auto lock = nuclm::createSimpleZooKeeperLock(distributed_lock_ptr->tryGetZooKeeper(),
                                                             distributed_lock_ptr->getLockPath(),
                                                             distributed_lock_ptr->getLockName(), "");
                if (lock->tryLock()) {
                    lock_acquired = true;
                    LOG(INFO) << "distributed lock is acquired by process: " << std::to_string(process_id);
                    // (2)perform actual block insertion to the local db server. All of the related exceptions have been
                    // captured.
                    SimulatedDoBlockInsertion(2000);
                }

                locking_issue_experienced = false;

                // (3) release lock
                lock->unlock();
                LOG(INFO) << "distributed lock is released by process: " << std::to_string(process_id);
            } catch (const Coordination::Exception& e) {
                if (locking_issue_experienced) {
                    LOG(ERROR)
                        << "In distributed locking, experienced issue at locking session"; // need to have metrics
                } else {
                    LOG(ERROR)
                        << "In distributed locking, experienced issue at unlocking session"; // need to have metrics
                }

                if (Coordination::isHardwareError(e.code)) {
                    LOG(ERROR) << "In distributed locking, to restart ZooKeeper session after: "
                               << DB::getCurrentExceptionMessage(false);
                    while (true) {
                        try {
                            distributed_lock_ptr->getAndSetZooKeeper();
                            LOG(INFO) << "ZooKeeper session restarted for table : " << table;
                            break;
                        } catch (...) {
                            LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                        }
                    }
                } else if (e.code == Coordination::Error::ZNONODE || e.code == Coordination::Error::ZNODEEXISTS) {
                    // that is OK, we can ignore it
                    LOG(ERROR) << "In distributed locking, experienced ZooKeeper error with error: "
                               << Coordination::errorMessage(e.code) << DB::getCurrentExceptionMessage(true);
                } else {
                    // other zookeeper related failure, let's reset the session
                    LOG(ERROR) << "In distributed locking, Unexpected ZooKeeper error with error: "
                               << Coordination::errorMessage(e.code) << DB::getCurrentExceptionMessage(true)
                               << ". Reset the session .";

                    while (true) {
                        try {
                            distributed_lock_ptr->getAndSetZooKeeper();
                            LOG(INFO) << "In distributed locking, ZooKeeper session restarted for table : " << table;
                            break;
                        } catch (...) {
                            LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                            using namespace std::chrono_literals;
                            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                        }
                    }
                }
            } catch (...) {
                // other non-zookeeper expected failure, which should not happen. Let's break the locking loop
                LOG(ERROR) << "In distributed locking, unexpected error other than ZooKeeper.."
                           << DB::getCurrentExceptionMessage(true) << ". Exit distributed locking loop. ";
            }

            if (!lock_acquired) {
                current_retries_on_locking++;
                // this gives us 200 * max_retries_on_locking = 200*100 = 20 seconds
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        result = false;
    }

    ASSERT_TRUE(result);
}

/**
 * The code used BlockSupportedBufferFlushTask.cpp. We can find that the lock node's creation time keeps changing.
 *
        [zk: localhost(CONNECTED) 19] get /clickhouse_aggregator_8/tables/9890/simple_event_16/lock

        cZxid = 0x10000027d
        ctime = Mon Sep 14 11:10:36 PDT 2020
        mZxid = 0x10000027d
        mtime = Mon Sep 14 11:10:36 PDT 2020
        pZxid = 0x10000027d
        cversion = 0
        dataVersion = 0
        aclVersion = 0
        ephemeralOwner = 0x200002195260008
        dataLength = 0
        numChildren = 0


        [zk: localhost(CONNECTED) 20] get /clickhouse_aggregator_8/tables/9890/simple_event_16/lock

        cZxid = 0x10000028f
        ctime = Mon Sep 14 11:10:43 PDT 2020
        mZxid = 0x10000028f
        mtime = Mon Sep 14 11:10:43 PDT 2020
        pZxid = 0x10000028f
        cversion = 0
        dataVersion = 0
        aclVersion = 0
        ephemeralOwner = 0x200002195260008
        dataLength = 0
        numChildren = 0
 *
 * Test: manually check the ephemeral node: we can run 2 test instances and to check how the ephemeralOwner id keeps
 switching between the two process owners.
 *
 * Test: manually delete the emphermal node with 2 test instances running, by manually issuing ZK shell command:
 *    delete /clickhouse_aggregator_8/tables/9890/simple_event_16/lock
 *
 * The application will show the following transient failure but the application continues to run:
 *  E0914 18:29:46.473749 26456 test_distributed_locking.cpp:532] In distributed locking, experienced ZooKeeper error
 with error: No nodeCode: 999,
 *  e.displayText() = Coordination::Exception: No node,
 *  path: /clickhouse_aggregator_8/tables/9890/simple_event_16/lock, Stack trace (when copying this message, always
 include the lines below):

   Overall, this test shows that we can have a seperate monitor to monitor the lifetime of the ephemeral nodes, in case
 the lifetime exceeds the 2* Block-Insertion time-out at the Clickhouse side, then we should kill the node.

 *
 * Test: communication link failure to all of the ZK servers, when we use iptables command to simualte the communication
 link failure from the test application (hosted on one Linux workstation) to all of the ZK nodes, we are seeing the
 following exception:
 *
 * E0915 01:00:18.737665 11159 test_distributed_locking.cpp:531] Code: 999, e.displayText() = Coordination::Exception:
 All connection tries failed while connecting to ZooKeeper.
 nodes: 10.148.179.109:2181, 10.148.179.6:2181, 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 0,
 e.displayText() = Timeout: connect timed out: 10.148.179.109:2181 (version 20.6.3.28), 10.148.179.109:2181
    Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Timeout: connect timed out: 10.148.179.6:2181
 (version 20.6.3.28), 10.148.179.6:2181 Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Timeout: connect
 timed out: 10.148.179.150:2181 (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 0,
 e.displayText() = Timeout: connect timed out: 10.148.179.109:2181 (version 20.6.3.28), 10.148.179.109:2181
    Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Timeout: connect timed out: 10.148.179.6:2181
 (version 20.6.3.28), 10.148.179.6:2181 Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Timeout: connect
 timed out: 10.148.179.150:2181 (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 0,
 e.displayText() = Timeout: connect timed out: 10.148.179.109:2181 (version 20.6.3.28), 10.148.179.109:2181
    Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Timeout: connect timed out: 10.148.179.6:2181
 (version 20.6.3.28), 10.148.179.6:2181 Poco::Exception. Code: 1000, e.code() = 0, e.displayText() = Timeout: connect
 timed out: 10.148.179.150:2181 (version 20.6.3.28), 10.148.179.150:2181 (Connection loss), Stack trace (when copying
 this message, always include the lines below):

    0. StackTrace::StackTrace() @ 0x4b10547 in
 /media/junli5/data2/NuColumnarAggr-CH206Stable/NuColumnarAggr/run-tests/deployed/test_distributed_locking
    1. DB::Exception::Exception(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > const&,
 int) @ 0x4af08b3 in
 /media/junli5/data2/NuColumnarAggr-CH206Stable/NuColumnarAggr/run-tests/deployed/test_distributed_locking
    2. Coordination::Exception::Exception(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>
 > const&, Coordination::Error, int) @ 0x4b837e4 in
 /media/junli5/data2/NuColumnarAggr-CH206Stable/NuColumnarAggr/run-tests/deployed/test_distributed_locking
    3. Coordination::Exception::Exception(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>
 > const&, Coordination::Error) @ 0x4b84284 in
 /media/junli5/data2/NuColumnarAggr-CH206Stable/NuColumnarAggr/run-tests/deployed/test_distributed_locking
    4. Coordination::ZooKeeper::connect(std::vector<Coordination::ZooKeeper::Node,
 std::allocator<Coordination::ZooKeeper::Node> > const&, Poco::Timespan) [clone .cold] @ 0xa89fd8 in
 /media/junli5/data2/NuColumnarAggr-CH206Stable/NuColumnarAggr/run-tests/deployed/test_distributed_locking
 *
 *  After we turn back the iptables, the test routine goes back to normal from the infinite loop of:
 *                    while (true) {
                            try {
                                distributed_lock_ptr->getAndSetZooKeeper();
                                LOG(INFO) << "ZooKeeper session restarted for table : " << table;
                                break;
                            }
                            catch (...) {
                                LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                                using namespace std::chrono_literals;
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                            }
                        }
 *
 * TEST: to kill one ZK servers manually. If we kill one of the three ZooKeeper servers 10.148.179.109, then we see the
 following session to be restarted, and then immediately
 * get restarted.
 *
 *  E0915 01:13:18.413949 11182 test_distributed_locking.cpp:514] In distributed locking, experienced issue at locking
 session E0915 01:13:18.413975 11182 test_distributed_locking.cpp:522] In distributed locking, to restart ZooKeeper
 session after: Code: 999, e.displayText() = Coordination::Exception: Session expired (Session expired)
 (version 20.6.3.28) E0915 01:13:18.814956 11182 test_distributed_locking.cpp:531] Code: 999, e.displayText() =
 Coordination::Exception: All connection tries failed while connecting to ZooKeeper.
 nodes: 10.148.179.109:2181, 10.148.179.6:2181, 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 111,
 e.displayText() = Connection refused (version 20.6.3.28), 10.148.179.109:2181 Code: 33, e.displayText() =
 DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper
 (version 20.6.3.28), 10.148.179.6:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all data. Bytes read: 0.
 Bytes expected: 4.: while receiving handshake from ZooKeeper (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception.
 Code: 1000, e.code() = 111, e.displayText() = Connection refused (version 20.6.3.28), 10.148.179.109:2181 Code: 33,
 e.displayText() = DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.: while receiving handshake
 from ZooKeeper (version 20.6.3.28), 10.148.179.6:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all data.
 Bytes read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper (version 20.6.3.28), 10.148.179.150:2181
    Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused
 (version 20.6.3.28), 10.148.179.109:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all data. Bytes read:
 0. Bytes expected: 4.: while receiving handshake from ZooKeeper (version 20.6.3.28), 10.148.179.6:2181 Code: 33,
 e.displayText() = DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.: while receiving handshake
 from ZooKeeper (version 20.6.3.28), 10.148.179.150:2181 (Connection loss), Stack trace (when copying this message,
 always include the lines below):

    I0915 01:13:19.901535 11182 test_distributed_locking.cpp:527] ZooKeeper session restarted for table :
 simple_event_16 I0915 01:13:20.162186 11182 test_distributed_locking.cpp:501] distributed lock is acquired by process:
 11182 I0915 01:13:20.162220 11182 test_distributed_locking.cpp:266] simulate block insertion running I0915
 01:13:20.418566 11182 test_distributed_locking.cpp:510] distributed lock is released by process: 11182

    Note: if we start the ZK cluster with the two remaining two nodes at t1, and then add back the node being killed,
 nothing abnormal shows up in the log. If we now remove the node that is just added a moment ago, nothing happenes. If
 we then kill the other node that was up at t1 (10.148.179.6),

    E0915 01:26:56.166679 11533 test_distributed_locking.cpp:514] In distributed locking, experienced issue at locking
 session E0915 01:26:56.166715 11533 test_distributed_locking.cpp:522] In distributed locking, to restart ZooKeeper
 session after: Code: 999, e.displayText() = Coordination::Exception: Session expired (Session expired)
 (version 20.6.3.28) E0915 01:26:56.559294 11533 test_distributed_locking.cpp:531] Code: 999, e.displayText() =
 Coordination::Exception: All connection tries failed while connecting to ZooKeeper.
 nodes: 10.148.179.150:2181, 10.148.179.6:2181, 10.148.179.109:2181 Code: 33, e.displayText() = DB::Exception: Cannot
 read all data. Bytes read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper
 (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection
 refused (version 20.6.3.28), 10.148.179.6:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all data. Bytes
 read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper (version 20.6.3.28), 10.148.179.109:2181 Code:
 33, e.displayText() = DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.: while receiving handshake
 from ZooKeeper (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 111, e.displayText() =
 Connection refused (version 20.6.3.28), 10.148.179.6:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all
 data. Bytes read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper
 (version 20.6.3.28), 10.148.179.109:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all data. Bytes read:
 0. Bytes expected: 4.: while receiving handshake from ZooKeeper (version 20.6.3.28), 10.148.179.150:2181
    Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused
 (version 20.6.3.28), 10.148.179.6:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all data. Bytes read: 0.
 Bytes expected: 4.: while receiving handshake from ZooKeeper (version 20.6.3.28), 10.148.179.109:2181 (Connection
 loss), Stack trace (when copying this message, always include the lines below):
     ...

     I0915 01:26:57.614862 11533 test_distributed_locking.cpp:527] ZooKeeper session restarted for table :
 simple_event_16



    Test: to kill two ZK servers manually: if I killed two out of the three ZK servers, then the failure keeps showing
 as:

    E0915 01:43:07.308949 11533 test_distributed_locking.cpp:531] Code: 999, e.displayText() = Coordination::Exception:
 All connection tries failed while connecting to ZooKeeper. nodes: 10.148.179.150:2181, 10.148\
    .179.6:2181, 10.148.179.109:2181
    Code: 33, e.displayText() = DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.: while receiving
 handshake from ZooKeeper (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 111,
 e.displayText() = Connection refused (version 20.6.3.28), 10.148.179.6:2181 Poco::Exception. Code: 1000, e.code() =
 111, e.displayText() = Connection refused (version 20.6.3.28), 10.148.179.109:2181 Code: 33, e.displayText() =
 DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper
 (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection
 refused (version 20.6.3.28), 10.148.179.6:2181 Poco::Exception. Code: 1000, e.code() = 111, e.displayText() =
 Connection refused (version 20.6.3.28), 10.148.179.109:2181 Code: 33, e.displayText() = DB::Exception: Cannot read all
 data. Bytes read: 0. Bytes expected: 4.: while receiving handshake from ZooKeeper
 (version 20.6.3.28), 10.148.179.150:2181 Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection
 refused (version 20.6.3.28), 10.148.179.6:2181 Poco::Exception. Code: 1000, e.code() = 111, e.displayText() =
 Connection refused (version 20.6.3.28), 10.148.179.109:2181 (Connection loss), Stack trace (when copying this message,
 always include the lines below):

   After I restarted one server, the ZooKeeper session then can be re-created and the application can then exit the
 following infinite loop:

                      while (true) {
                            try {
                                distributed_lock_ptr->getAndSetZooKeeper();
                                LOG(INFO) << "ZooKeeper session restarted for table : " << table;
                                break;
                            }
                            catch (...) {
                                LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                                using namespace std::chrono_literals;
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                            }
                        }
    Adding back one more ZK server does not introduce state change visible in the application.

 */
TEST_F(DistributedLockingRelatedTest, testSimpleZooKeeperLockOperationInOneProcessInALoop) {
    std::string path = getConfigFilePath("example_aggregator_config_for_distributed_locking.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    size_t max_retries_on_locking = 20;
    std::string table = "simple_event_16";

    size_t max_counter = 20;
    size_t counter = max_counter;

    while (counter > 0) {
        counter--;
        LOG(INFO) << "Current counter is: " << counter << " with max counter: " << max_counter;
        SimulatedProcessIdling(500);

        bool lock_acquired = false;
        size_t current_retries_on_locking = 0;
        bool result = true;

        try {
            // Ensure that the per-table distributed lock is created at the "table" level. The concrete lock is under
            // table name.
            nuclm::DistributedLoaderLockManager::getInstance().ensureLockExists(table);
            nuclm::DistributedLoaderLock::DistributedLoaderLockPtr distributed_lock_ptr =
                nuclm::DistributedLoaderLockManager::getInstance().getLock(table);
            CHECK(distributed_lock_ptr != nullptr)
                << "distributed lock manager should return an non-empty distributed lock object";
            std::string lock_path = nuclm::DistributedLoaderLockManager::getInstance().getLockPath(table);

            // ensure that the per-table local lock is crated
            nuclm::LocalLoaderLockManager::getInstance().ensureLockExists(table);
            nuclm::LocalLoaderLock::LocalLoaderLockPtr local_lock_ptr =
                nuclm::LocalLoaderLockManager::getInstance().getLock(table);
            CHECK(local_lock_ptr != nullptr) << "local lock manager should return an non-empty local lock object";

            //(1) local locking across multiple kafka-connector first
            std::lock_guard<std::mutex> lck(local_lock_ptr->loader_lock);
            pid_t process_id = getpid();
            //(2) distributed locking second
            while (!lock_acquired && (current_retries_on_locking < max_retries_on_locking)) {

                // Only one of the two can be true at the end due to Zookeeper exception raised in try-lock or un-lock
                bool locking_issue_experienced = true;
                try {
                    // (1) get lock
                    auto lock = nuclm::createSimpleZooKeeperLock(distributed_lock_ptr->tryGetZooKeeper(),
                                                                 distributed_lock_ptr->getLockPath(),
                                                                 distributed_lock_ptr->getLockName(), "");
                    if (lock->tryLock()) {
                        lock_acquired = true;
                        LOG(INFO) << "distributed lock is acquired by process: " << std::to_string(process_id);
                        // (2)perform actual block insertion to the local db server. All of the related exceptions have
                        // been captured.
                        SimulatedDoBlockInsertion(200);
                    }

                    locking_issue_experienced = false;

                    // (3) release lock
                    lock->unlock();
                    LOG(INFO) << "distributed lock is released by process: " << std::to_string(process_id);
                } catch (const Coordination::Exception& e) {
                    if (locking_issue_experienced) {
                        LOG(ERROR)
                            << "In distributed locking, experienced issue at locking session"; // need to have metrics
                    } else {
                        LOG(ERROR)
                            << "In distributed locking, experienced issue at unlocking session"; // need to have metrics
                    }

                    if (Coordination::isHardwareError(e.code)) {
                        LOG(ERROR) << "In distributed locking, to restart ZooKeeper session after: "
                                   << DB::getCurrentExceptionMessage(false);
                        while (true) {
                            try {
                                distributed_lock_ptr->getAndSetZooKeeper();
                                LOG(INFO) << "ZooKeeper session restarted for table : " << table;
                                break;
                            } catch (...) {
                                LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                                using namespace std::chrono_literals;
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                            }
                        }
                    } else if (e.code == Coordination::Error::ZNONODE || e.code == Coordination::Error::ZNODEEXISTS) {
                        // that is OK, we can ignore it
                        LOG(ERROR) << "In distributed locking, experienced ZooKeeper error with error: "
                                   << Coordination::errorMessage(e.code) << DB::getCurrentExceptionMessage(true);
                    } else {
                        // other zookeeper related failure, let's reset the session
                        LOG(ERROR) << "In distributed locking, Unexpected ZooKeeper error with error: "
                                   << Coordination::errorMessage(e.code) << DB::getCurrentExceptionMessage(true)
                                   << ". Reset the session .";

                        while (true) {
                            try {
                                distributed_lock_ptr->getAndSetZooKeeper();
                                LOG(INFO)
                                    << "In distributed locking, ZooKeeper session restarted for table : " << table;
                                break;
                            } catch (...) {
                                LOG(ERROR) << DB::getCurrentExceptionMessage(true);

                                using namespace std::chrono_literals;
                                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                            }
                        }
                    }
                } catch (...) {
                    // other non-zookeeper expected failure, which should not happen. Let's break the locking loop
                    LOG(ERROR) << "In distributed locking, unexpected error other than ZooKeeper.."
                               << DB::getCurrentExceptionMessage(true) << ". Exit distributed locking loop. ";
                }

                if (!lock_acquired) {
                    current_retries_on_locking++;
                    // this gives us 200 * max_retries_on_locking = 200*100 = 20 seconds
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                }
            }
        } catch (...) {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            result = false;
        }

        ASSERT_TRUE(result);
    }
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
