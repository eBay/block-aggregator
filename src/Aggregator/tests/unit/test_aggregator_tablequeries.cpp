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

#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <memory>
#include <thread>
#include <cstdlib>
#include <unistd.h>
#include <limits.h>

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

static bool removeTableContent(DB::ContextMutablePtr context, boost::asio::io_context& ioc,
                               const std::string& table_name) {
    bool query_result = false;
    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();

        std::string query = "ALTER TABLE " + table_name + " DELETE WHERE 1=1;";

        query_result = loader.executeTableCreation(table_name, query);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();
        LOG(ERROR) << "with exception return code: " << code;
        query_result = false;
    }

    return query_result;
}

static bool populateRowsAfterCleanupRows(DB::ContextMutablePtr context, boost::asio::io_context& ioc,
                                         const std::string& table_name) {

    bool status = false;
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    if (!removed) {
        return false;
    }

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    srand(time(NULL)); // to guarantee that the random number is different each time when the test case runs.
    size_t rows_to_insert = 10;

    if (table_name == "simple_event_5") {
        for (size_t i = 0; i < rows_to_insert; i++) {
            // this is smaller than the condition in one of the test case: > 19214455
            int counter_val_defined = rand() % 10000000;

            std::string query = "insert into " + table_name +
                " (`Host`, `Colo`, `Count`)"
                " VALUES ('graphdb-1', 'LVS', " +
                std::to_string(counter_val_defined) + ") ";
            LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
            loader.init();
            status = loader.load_buffer(table_name, query);
        }
    } else {
        LOG(ERROR) << "populate table content does not support implementation for table: " << table_name;
    };

    return status;
}

static bool forceToRemoveZooKeeperTablePath(const std::string& table_name, const std::string& shard_id) {
    std::string table_path = "/clickhouse/tables/" + shard_id + "/" + table_name;
    bool result = true;
    try {
        std::shared_ptr<zkutil::ZooKeeper> zookeeper_instance =
            nuclm::LoaderZooKeeperSession::getInstance().getZooKeeper();
        zookeeper_instance->removeRecursive(table_path);
    } catch (Coordination::Exception& ex) {
        if (ex.code == Coordination::Error::ZNONODE) {
            LOG(WARNING) << "try to remove node: " << table_path << " that does not exist any more";
        } else {
            LOG(ERROR) << DB::getCurrentExceptionMessage(true);
            result = false;
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        result = false;
    }
    return result;
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

class AggregatorLoaderTableQueryRelatedTest : public ::testing::Test {
  protected:
    static std::string GetShardId() {
        return std::string("01"); // this is from the configuration file on shard id.
    }

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

    // Define per-test set-up logic as usual, to ensure that we have table?
    virtual void SetUp() {}

    // Define per-test tear-down logic as usual
    virtual void TearDown() {
        //....
    }

    static ContextWrapper* shared_context;
};

ContextWrapper* AggregatorLoaderTableQueryRelatedTest::shared_context = nullptr;

/**
 *
    I0208 15:18:05.722509 25925 AggregatorLoader.cpp:352] in AggregatorLoader, OnData identified that block received is
 not empty I0208 15:18:05.722561 25925 AggregatorLoader.cpp:354] In AggregatorLoader, OnData shows structure dumped:
 count() UInt64 UInt64(size = 0) I0208 15:18:05.722573 25925 AggregatorLoader.cpp:359] in AggregatorLoader, OnData
 identified that block received has number of rows: 0 I0208 15:18:05.722738 25925 AggregatorLoader.cpp:371]  In
 AggregatorLoader exit OnData event processing.... I0208 15:18:05.722788 25925 AggregatorLoader.cpp:346]  In
 AggregatorLoader enter OnData event processing.... I0208 15:18:05.722800 25925 AggregatorLoader.cpp:352] in
 AggregatorLoader, OnData identified that block received is not empty I0208 15:18:05.722812 25925
 AggregatorLoader.cpp:354] In AggregatorLoader, OnData shows structure dumped: count() UInt64 UInt64(size = 1) I0208
 15:18:05.722821 25925 AggregatorLoader.cpp:359] in AggregatorLoader, OnData identified that block received has number
 of rows: 1 2320 I0208 15:18:05.722868 25925 AggregatorLoader.cpp:371]  In AggregatorLoader exit OnData event
 processing.... I0208 15:18:05.722919 25925 AggregatorLoader.cpp:346]  In AggregatorLoader enter OnData event
 processing.... E0208 15:18:05.722931 25925 AggregatorLoader.cpp:348] in AggregatorLoader, OnData identified that block
 received is empty, exist OnData event processing
 */

TEST_F(AggregatorLoaderTableQueryRelatedTest, testTableCountQuery) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << "JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;

    try {
        DB::ContextMutablePtr context = AggregatorLoaderTableQueryRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderTableQueryRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        with_settings([=, &context, &ioc](SETTINGS s) {
            nuclm::AggregatorLoaderManager manager(context, ioc);
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            auto& databases = s.config.schema.databases;
            int databaseIndex = 0;
            for (const auto& database : databases) {
                ASSERT_EQ(database.databaseName, "default");

                if (databaseIndex == 0) {
                    databaseIndex++;

                    size_t table_index = 0;
                    auto& tables = database.tables;

                    LOG(INFO) << "total number of the tables defined in the specification is: " << tables.size();

                    for (const auto& table : tables) {
                        std::string table_name = table.tableName;
                        table_index++;
                        // simple_event_5 is 3-th table.
                        if (table_index == 3) {
                            // Populate rows first after removing the whole table content.
                            bool refreshed = populateRowsAfterCleanupRows(context, ioc, table_name);
                            LOG(INFO) << "remove table content for table: " << table_name
                                      << " returned status: " << refreshed;
                            ASSERT_TRUE(refreshed);

                            std::string query = "select count (*) from " + table_name;

                            LOG(INFO) << "table index: " << table_index << " chosen table: " << table_name
                                      << " with query: " << query;
                            DB::Block query_result;

                            bool status = loader.executeTableSelectQuery(table_name, query, query_result);
                            ASSERT_TRUE(status);
                            // the block header has the definition of:
                            // name String String(size = 3), type String String(size = 3), default_type String
                            // String(size = 3),
                            //       default_expression String String(size = 3), comment String String(size = 3),
                            //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
                            if (status) {

                                std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine =
                                    loader.getLoaderStateMachine();

                                std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                                    std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
                                // header definition for the table definition block:
                                DB::Block sample_block;
                                sm->loadSampleHeader(sample_block);

                                // header definition for the table definition block:
                                const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                                    sample_block.getColumnsWithTypeAndName();
                                int column_index = 0;
                                for (auto& p : columns_with_type_and_name) {
                                    LOG(INFO)
                                        << "column index: " << column_index++ << " column type: " << p.type->getName()
                                        << " column name: " << p.name << " number of rows: " << p.column->size();
                                }

                                DB::MutableColumns columns = query_result.mutateColumns();
                                // only one column, with type of UInt64.
                                auto& column_uint64_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]);
                                DB::UInt64 count_query_result = column_uint64_0.getData()[0];

                                LOG(INFO) << " count query result is: " << count_query_result;

                                ASSERT_TRUE(count_query_result > 0);
                            }
                        }
                    }
                }
            }
        });
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorLoaderTableQueryRelatedTest, testTableCountQueryWithCondition) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderTableQueryRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderTableQueryRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        with_settings([=, &context, &ioc](SETTINGS s) {
            nuclm::AggregatorLoaderManager manager(context, ioc);
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            auto& databases = s.config.schema.databases;
            int databaseIndex = 0;
            for (const auto& database : databases) {
                ASSERT_EQ(database.databaseName, "default");

                if (databaseIndex == 0) {
                    databaseIndex++;

                    size_t table_index = 0;
                    auto& tables = database.tables;

                    LOG(INFO) << "total number of the tables defined in the specification is: " << tables.size();

                    for (const auto& table : tables) {
                        std::string table_name = table.tableName;
                        table_index++;
                        if (table_index == 3) {
                            bool refreshed = populateRowsAfterCleanupRows(context, ioc, table_name);
                            LOG(INFO) << "remove table content for table: " << table_name
                                      << " returned status: " << refreshed;
                            ASSERT_TRUE(refreshed);

                            std::string query =
                                "select count (*) from " + table.tableName + " where  Count > 19214455 ";

                            LOG(INFO) << "table index: " << table_index << " chosen table: " << table.tableName
                                      << " with query: " << query;
                            DB::Block query_result;

                            bool status = loader.executeTableSelectQuery(table.tableName, query, query_result);
                            ASSERT_TRUE(status);

                            // the block header has the definition of:
                            // name String String(size = 3), type String String(size = 3), default_type String
                            // String(size = 3),
                            //       default_expression String String(size = 3), comment String String(size = 3),
                            //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
                            if (status) {

                                std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine =
                                    loader.getLoaderStateMachine();

                                std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                                    std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
                                // header definition for the table definition block:
                                DB::Block sample_block;
                                sm->loadSampleHeader(sample_block);

                                // header definition for the table definition block:
                                const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                                    sample_block.getColumnsWithTypeAndName();
                                int column_index = 0;
                                for (auto& p : columns_with_type_and_name) {
                                    LOG(INFO)
                                        << "column index: " << column_index++ << " column type: " << p.type->getName()
                                        << " column name: " << p.name << " number of rows: " << p.column->size();
                                }

                                DB::MutableColumns columns = query_result.mutateColumns();
                                // only one column, with type of UInt64.
                                auto& column_uint64_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]);
                                DB::UInt64 count_query_result = column_uint64_0.getData()[0];

                                LOG(INFO) << " count query result is: " << count_query_result;

                                ASSERT_TRUE(count_query_result == 0);
                            }
                        }
                    }
                }
            }
        });
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorLoaderTableQueryRelatedTest, testTableCountSelectQuery) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderTableQueryRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderTableQueryRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

        with_settings([=, &context, &ioc](SETTINGS s) {
            nuclm::AggregatorLoaderManager manager(context, ioc);
            nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
            loader.init();

            auto& databases = s.config.schema.databases;
            int databaseIndex = 0;
            for (const auto& database : databases) {
                ASSERT_EQ(database.databaseName, "default");

                if (databaseIndex == 0) {
                    databaseIndex++;

                    size_t table_index = 0;
                    auto& tables = database.tables;

                    LOG(INFO) << "total number of the tables defined in the specification is: " << tables.size();

                    for (const auto& table : tables) {
                        std::string table_name = table.tableName;
                        table_index++;
                        if (table_index == 3) {
                            bool refreshed = populateRowsAfterCleanupRows(context, ioc, table_name);
                            LOG(INFO) << "remove table content for table: " << table_name
                                      << " returned status: " << refreshed;
                            ASSERT_TRUE(refreshed);

                            std::string query = "select * from " + table.tableName;

                            LOG(INFO) << "table index: " << table_index << " chosen table: " << table.tableName
                                      << " with query: " << query;
                            DB::Block query_result;

                            bool status = loader.executeTableSelectQuery(table.tableName, query, query_result);
                            ASSERT_TRUE(status);
                            // the block header has the definition of:
                            // name String String(size = 3), type String String(size = 3), default_type String
                            // String(size = 3),
                            //       default_expression String String(size = 3), comment String String(size = 3),
                            //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
                            if (status) {
                                std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine =
                                    loader.getLoaderStateMachine();

                                std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                                    std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
                                // header definition for the table definition block:
                                DB::Block sample_block;
                                sm->loadSampleHeader(sample_block);
                                const DB::ColumnsWithTypeAndName& columns_with_type_and_name =
                                    sample_block.getColumnsWithTypeAndName();
                                int column_index = 0;

                                for (auto& p : columns_with_type_and_name) {
                                    LOG(INFO)
                                        << "column index: " << column_index++ << " column type: " << p.type->getName()
                                        << " column name: " << p.name << " number of rows: " << p.column->size();
                                }

                                DB::MutableColumns columns = query_result.mutateColumns();
                                // only one column, with type of UInt64.
                                auto& column_uint64_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]);
                                auto& column_string_1 = assert_cast<DB::ColumnString&>(*columns[1]);
                                auto& column_string_2 = assert_cast<DB::ColumnString&>(*columns[2]);

                                size_t total_row_count = column_uint64_0.size();

                                for (size_t i = 0; i < total_row_count; i++) {
                                    DB::UInt64 column_0 = column_uint64_0.getData()[i];
                                    std::string column_1 = column_string_1.getDataAt(i).toString();
                                    std::string column_2 = column_string_2.getDataAt(i).toString();

                                    LOG(INFO)
                                        << " row: " << i << ": " << column_0 << " " << column_1 << " " << column_2;
                                }

                                LOG(INFO) << " total number of rows retrieved:  " << total_row_count;

                                ASSERT_TRUE(total_row_count > 0);
                            }
                        }
                    }
                }
            }
        });
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * for table insertion, if it is successfully, only the end-of-stream returns. no actual data is acknowledged.
 *
    I0208 16:50:11.918706 30876 AggregatorLoader.cpp:531] before sending empty query for table column definitions.  in
 thread: 140162626827456 I0208 16:50:11.920173 30876 AggregatorLoader.cpp:536] after sending empty query for table
 column definitions.  in thread: 140162626827456 I0208 16:50:11.920197 30876 AggregatorLoader.cpp:120] In Aggregator
 Loader: receiveQueryResult entering... I0208 16:50:11.969141 30876 AggregatorLoader.cpp:434]  In AggregatorLoader enter
 OnEndOfStream event processing.... I0208 16:50:11.969168 30876 AggregatorLoader.cpp:443]  In AggregatorLoader exit
 OnEndOfStream event processing.... I0208 16:50:11.969177 30876 AggregatorLoader.cpp:183] Query: create table
 simple_event_2 ( Host String, Colo String, EventName String, Count UInt64, Duration Float32)

    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_2', '{replica}') ORDER BY(Host, Colo,
 EventName) SETTINGS index_granularity=8192;  has actual processing result: of 0 I0208 16:50:11.969189 30876
 AggregatorLoader.cpp:189] In Aggregator Loader: receiveQueryResult exiting...

    if  re-run the query again, we will have the following exception;

    0208 16:53:22.498585 30904 AggregatorLoader.cpp:120] In Aggregator Loader: receiveQueryResult entering...
    E0208 16:53:22.517721 30904 AggregatorLoader.cpp:115] Received exception Code: 57. DB::Exception: Received
 from 10.169.98.238:9000. DB::Exception: Table default.simple_event_2 already exists..  in thread: 140245570335936 I0208
 16:53:22.517750 30904 AggregatorLoader.cpp:183] Query: create table simple_event_2 ( Host String, Colo String,
         EventName String,
         Count UInt64,
         Duration Float32)

    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_2', '{replica}') ORDER BY(Host, Colo,
 EventName) SETTINGS index_granularity=8192;  has actual processing result: of 0 I0208 16:53:22.517763 30904
 AggregatorLoader.cpp:189] In Aggregator Loader: receiveQueryResult exiting...
 */

TEST_F(AggregatorLoaderTableQueryRelatedTest, testTableCreation) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderTableQueryRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderTableQueryRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
        nuclm::AggregatorLoaderManager manager(context, ioc);
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();
        {
            // NOTE: this is to cut and paste from the query string that is from the editor and works with the
            // clickhouse-client query interface. the "\n" gets automatically added by the clion editor.
            std::string table_name = "simple_event_45";
            std::string zk_path = "/clickhouse/tables/{shard}/" + table_name;
            std::string query = "create table " + table_name +
                " (\n"
                "     Host String,\n"
                "     Colo String,\n"
                "     EventName String,\n"
                "     Count UInt64,\n"
                "     Duration Float32)\n"
                "\n"
                "ENGINE = ReplicatedMergeTree('" +
                zk_path +
                "', "
                "'{replica}') ORDER BY(Host, Colo, EventName) SETTINGS index_granularity=8192; ";

            bool query_result = loader.executeTableCreation(table_name, query);
            ASSERT_TRUE(query_result);
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * for table drop, if it is successfully, only the end-of-stream returns. no actual data is acknowledged.

    DROP TABLE simple_event_2

    I0208 17:00:21.810241 31129 AggregatorLoader.cpp:531] before sending empty query for table column definitions.  in
 thread: 140006305965248 I0208 17:00:21.811748 31129 AggregatorLoader.cpp:536] after sending empty query for table
 column definitions.  in thread: 140006305965248 I0208 17:00:21.811772 31129 AggregatorLoader.cpp:120] In Aggregator
 Loader: receiveQueryResult entering... I0208 17:00:21.842996 31129 AggregatorLoader.cpp:434]  In AggregatorLoader enter
 OnEndOfStream event processing.... I0208 17:00:21.843022 31129 AggregatorLoader.cpp:443]  In AggregatorLoader exit
 OnEndOfStream event processing.... I0208 17:00:21.843030 31129 AggregatorLoader.cpp:183] Query: drop table
 simple_event_2; has actual processing result: of 0 I0208 17:00:21.843039 31129 AggregatorLoader.cpp:189] In Aggregator
 Loader: receiveQueryResult exiting... I0208 17:00:21.843047 31129 AggregatorLoader.cpp:540] receive table query result
 with status:  0 in thread: 1

    If re-run the test again, then we will encounter the following exception:

    E0208 17:02:04.639351  5696 AggregatorLoader.cpp:115] Received exception Code: 60. DB::Exception: Received
 from 10.169.98.238:9000. DB::Exception: Table default.simple_event_2 doesn't exist..  in thread: 139776790029504

 */
TEST_F(AggregatorLoaderTableQueryRelatedTest, testTableDrop) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());
    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderTableQueryRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderTableQueryRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
        nuclm::AggregatorLoaderManager manager(context, ioc);
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();

        {
            // NOTE: this is to cut and paste from the query string that is from the editor and works with the
            // clickhouse-client query interface. the "\n" gets automatically added by the clion editor.
            std::string table_name = "simple_event_45";

            bool query_result = loader.executeTableDeletion(table_name);
            ASSERT_TRUE(query_result);

            // In ClickHoue 21.8, after dropping the table command, the corresponding table sub-tree in zookeeper still
            // exists, thus we need to perform the clean-up of the zookeeper sub-tree also.
            std::string shard_id = AggregatorLoaderTableQueryRelatedTest::GetShardId();
            bool deleteZKTreeResult = forceToRemoveZooKeeperTablePath(table_name, shard_id);
            LOG(INFO) << "to delete zookeeper table path for table: " << table_name << "with result (successful=1)"
                      << deleteZKTreeResult;
            ASSERT_EQ(deleteZKTreeResult, 1);
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

TEST_F(AggregatorLoaderTableQueryRelatedTest, testTableCleanUp) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    ASSERT_TRUE(!path.empty());

    bool failed = false;
    try {
        DB::ContextMutablePtr context = AggregatorLoaderTableQueryRelatedTest::shared_context->getContext();
        boost::asio::io_context& ioc = AggregatorLoaderTableQueryRelatedTest::shared_context->getIOContext();
        SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
        nuclm::AggregatorLoaderManager manager(context, ioc);
        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
        loader.init();

        std::string table_name = "ontime";
        std::string query = "ALTER TABLE " + table_name + " DELETE WHERE 1=1;";

        bool status = loader.executeTableCreation(table_name, query);
        ASSERT_TRUE(status);

        // After that, to check that no rows exist.
        query = "select count (*) from " + table_name;
        LOG(INFO) << "chosen table: " << table_name << " with query: " << query;
        DB::Block query_result;

        status = loader.executeTableSelectQuery(table_name, query, query_result);
        ASSERT_TRUE(status);
        // the block header has the definition of:
        // name String String(size = 3), type String String(size = 3), default_type String String(size = 3),
        //       default_expression String String(size = 3), comment String String(size = 3),
        //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
        if (status) {
            std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

            std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
            // header definition for the table definition block:
            DB::Block sample_block;
            sm->loadSampleHeader(sample_block);
            const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
            int column_index = 0;

            for (auto& p : columns_with_type_and_name) {
                LOG(INFO) << "column index: " << column_index++ << " column type: " << p.type->getName()
                          << " column name: " << p.name << " number of rows: " << p.column->size();
            }

            DB::MutableColumns columns = query_result.mutateColumns();
            // only one column, with type of UInt64.
            auto& column_uint64_0 = assert_cast<DB::ColumnUInt64&>(*columns[0]);
            DB::UInt64 total_count = column_uint64_0.getData()[0];
            LOG(INFO) << " count query result is: " << total_count;

            ASSERT_TRUE(total_count == 0);
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();
        LOG(ERROR) << "with exception return code: " << code;
        failed = true;
    }
    ASSERT_FALSE(failed);
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
