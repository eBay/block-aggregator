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

#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Storages/ColumnsDescription.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>

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

using DetailedRowInspector = std::function<void(DB::Block& /*query result*/, DB::Block& /*block header*/)>;

static bool inspectRowBasedContent(const std::string& table_name, const std::string& query_on_table,
                                   nuclm::AggregatorLoader& loader, const DetailedRowInspector& rowInspector) {
    // to retrieve array counter with 5 elements, and to retrieve default array(array(nullable(string))).
    bool query_status = false;
    try {
        // Retrieve the array value back.
        DB::Block query_result;

        bool status = loader.executeTableSelectQuery(table_name, query_on_table, query_result);
        LOG(INFO) << " status on querying table: " + table_name + " with query: " + query_on_table;
        if (status) {
            std::shared_ptr<nuclm::AggregatorLoaderStateMachine> state_machine = loader.getLoaderStateMachine();

            std::shared_ptr<nuclm::SelectQueryStateMachine> sm =
                std::static_pointer_cast<nuclm::SelectQueryStateMachine>(state_machine);
            // header definition for the table definition block:
            DB::Block sample_block;
            sm->loadSampleHeader(sample_block);
            rowInspector(query_result, sample_block);
            query_status = true;
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code << " when retrieving rows for table: " << table_name;
    }

    return query_status;
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

class AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context = nullptr;

TEST_F(AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest, testGetDefaultsFromTableDefinition) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    bool failed = false;
    try {
        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        for (const auto& table_name : table_names) {
            if (table_name == "simulated_simple_ads_pl0") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;
                DB::ColumnDefaults column_defaults = table_definition.getColumnDefaults();
                LOG(INFO) << " ****retrieved column-defaults has size: " << column_defaults.size();

                size_t default_column_count = 0;
                for (std::pair<std::string, DB::ColumnDefault> column_default : column_defaults) {
                    LOG(INFO) << "column name is: " << column_default.first;
                    if (column_default.second.kind == DB::ColumnDefaultKind::Default) {
                        LOG(INFO) << " column default kind is: "
                                  << " default column";
                    } else if (column_default.second.kind == DB::ColumnDefaultKind::Materialized) {
                        LOG(INFO) << " column default kind is: "
                                  << " materialized column";
                    } else if (column_default.second.kind == DB::ColumnDefaultKind::Alias) {
                        LOG(INFO) << " column default kind is: "
                                  << " alias column";
                    }

                    {
                        DB::ASTPtr expression_ptr = column_default.second.expression;
                        DB::WriteBufferFromOwnString buf;
                        DB::formatAST(*expression_ptr, buf, false, true);
                        LOG(INFO) << "default column's default expression is: " << buf.str();
                    }

                    default_column_count++;
                }

                ASSERT_EQ(default_columns_count, (size_t)1);
            }
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
 * Purposely retrieve the table that has less number of columns to simulate the missing of columns, and have the
aggregator
 * to perform fixing of the missing columns against the retrieved (up-to-date) server-side table schema. Note that the
columns at the client are the
 * sub-set of the columns at the server.
 *
 * That is, we insert data + schema based on the table of:
 *
 * CREATE TABLE simulated_simple_ads_pl0_missing_columns (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64,
`itemId` Int64)
 *
 * but the aggregator and the server side are with the table of:
 * CREATE TABLE simulated_simple_ads_pl0 (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64, `itemId` Int64,
`selleerKwId` String DEFAULT 'normal', `adgroupId` Nullable(String), `compaignType` LowCardinality(String))

 * and with the insertion of (only 6 columns):
 *
 *  structure dumped in block holder that is constructed from protobuf-reader that takes into account the server-side
schema already: eventts DateTime UInt32(size = 1), sellerId Int64 Int64(size = 1), compaignId Int64 Int64(size = 1),
itemId Int64 Int64(size = 1), selleerKwId String String(size = 1), adgroupId Nullable(String) Nullable(size = 1,
String(size = 1), UInt8(size = 1)), compaignType LowCardinality(String) ColumnLowCardinality(size = 1, UInt8(size = 1),
ColumnUnique(size = 1, String(size = 1)))

*   INSERT INTO simulated_simple_ads_pl0 (eventts, sellerId, compaignId, itemId, selleerKwId, adgroupId, compaignType)
VALUES

 *  And the query result of:
 *
    ┌─────────────eventts─┬──sellerId─┬─compaignId─┬────itemId─┬─selleerKwId─┬─adgroupId─┬─compaignType─┐
    │ 2020-11-24 02:16:37 │ 599123456 │  600123456 │ 601123456 │ normal      │ ᴺᵁᴸᴸ      │              │
    └─────────────────────┴───────────┴────────────┴───────────┴─────────────┴───────────┴──────────────┘
 *

 */
TEST_F(AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest,
       InsertARowWithColumnsMissingAndFixingDoneByAggregatorSide) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string actual_table_name = "simulated_simple_ads_pl0";
    bool removed = removeTableContent(context, ioc, actual_table_name);
    LOG(INFO) << "remove table content for table: " << actual_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    bool failed = false;
    size_t rows = 1;

    long time_stamp = 0;
    long seller_id = 0;
    long compaign_id = 0;
    long item_id = 0;

    srand(time(NULL));
    // to enforce the ordering that is based on: ORDER BY (sellerId, compaignId, compaignType)
    long init_rand_val = rand() % 100000000;

    try {

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string missing_columns_table_name = "simulated_simple_ads_pl0_missing_columns";
        bool found_missing_columns_table_name = false;
        bool found_actual_table_name = false;
        for (const auto& table_name : table_names) {
            if (table_name == missing_columns_table_name) {
                found_missing_columns_table_name = true;
            }
            if (table_name == actual_table_name) {
                found_actual_table_name = true;
            }
        }
        ASSERT_TRUE(found_missing_columns_table_name);
        ASSERT_TRUE(found_actual_table_name);

        LOG(INFO) << "retrieved real table definition with missing columns: " << actual_table_name;
        const nuclm::TableColumnsDescription& actual_table_definition =
            manager.getTableColumnsDefinition(actual_table_name);
        LOG(INFO) << " with definition: " << actual_table_definition.str();

        size_t actual_default_columns_count = actual_table_definition.getSizeOfDefaultColumns();
        LOG(INFO) << " size of default columns count is: " << actual_default_columns_count;

        ASSERT_TRUE(actual_default_columns_count == 1);
        size_t actual_ordinary_columns_count = actual_table_definition.getSizeOfOrdinaryColumns();
        LOG(INFO) << " size of ordinary columns count is: " << actual_ordinary_columns_count;

        ASSERT_TRUE(actual_ordinary_columns_count == 6);

        DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
            actual_table_definition.getFullColumnTypesAndNamesDefinition());

        std::string serializedSqlBatchRequestInString;

        {
            LOG(INFO) << "to construct and serialized a message";
            std::string table = actual_table_name;
            // To simulate that the client has out-of-date schema.
            std::string sql =
                "insert into " + actual_table_name + " (eventts, sellerId, compaignId, itemId) values (?, ?, ?, ?)";
            std::string shard = "nudata.monstor.cdc.dev.marketing.1";

            nucolumnar::aggregator::v1::DataBindingList bindingList;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
            sqlBatchRequest.set_shard(shard);
            sqlBatchRequest.set_table(table);

            nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                sqlBatchRequest.mutable_nucolumnarencoding();
            sqlWithBatchBindings->set_sql(sql);
            sqlWithBatchBindings->mutable_batch_bindings();

            for (size_t r = 0; r < rows; r++) {
                nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                    sqlWithBatchBindings->add_batch_bindings();
                // value 1: eventts, DateTime
                nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                auto ts = new nucolumnar::datatypes::v1::TimestampP();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch());
                ts->set_milliseconds(ms.count());
                val1->set_allocated_timestamp(ts);
                time_stamp = ms.count();

                // value 2: sellerId, int64
                nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                seller_id = init_rand_val++;
                val2->set_long_value(seller_id);

                // value 3: compaignId, int64
                nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                compaign_id = init_rand_val++;
                val3->set_long_value(compaign_id);

                // value4: itemId, int64
                nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                item_id = init_rand_val++;
                val4->set_long_value(item_id);

                nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                pval1->CopyFrom(*val1);

                nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                pval2->CopyFrom(*val2);

                nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                pval3->CopyFrom(*val3);

                nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                pval4->CopyFrom(*val4);
            }

            serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
        }

        // to create a dummy schema_tracker.
        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(actual_table_name, actual_table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        nucolumnar::aggregator::v1::SQLBatchRequest deserialized_batch_request;
        deserialized_batch_request.ParseFromString(serializedSqlBatchRequestInString);

        bool serialization_status = batchReader.read(actual_table_definition, deserialized_batch_request);
        ASSERT_TRUE(serialization_status);

        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

        // to insert to actual table: simulated_simple_ads_pl0, but with the knowledge of the columns that are with the
        // missing columns one. The actual values field do not matter, as it will be truncated by the Aggregator Loader.
        std::string query = "insert into " + actual_table_name +
            " (eventts, sellerId, compaignId, itemId, selleerKwId, adgroupId, compaignType) VALUES (9098, 9097);";
        LOG(INFO) << "chosen table: " << actual_table_name << "with insert query: " << query;
        loader.init();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        size_t total_number_of_columns = block_holder.columns();
        LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names_holder;

        ASSERT_EQ(total_number_of_rows_holder, rows);
        ASSERT_EQ(total_number_of_columns, (size_t)7);

        // We should see totally 7 columns in simulated_simple_ads_pl0
        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder that is constructed from protobuf-reader that takes into "
                     "account the server-side schema already: "
                  << structure;
        bool result = loader.load_buffer(actual_table_name, query, block_holder);

        ASSERT_TRUE(result);

        std::string table_being_queried = "default." + actual_table_name;
        std::string query_on_table = "select eventts , \n"   /* 0. DateTime*/
                                     "     sellerId,\n"      /* 1. Int64 */
                                     "     compaignId,\n"    /* 2. Int64*/
                                     "     itemId, \n"       /* 3. Int64 */
                                     "     selleerKwId , \n" /* 4. String, with default: normal  */
                                     "     adgroupId , \n"   /* 5. Nullable (String) */
                                     "     compaignType \n"  /* 6. LowCardinality(String)*/
                                     "from " +
            table_being_queried +
            "\n"
            "order by (sellerId, compaignId, compaignType)";

        auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
            const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
            int column_index = 0;

            for (auto& p : columns_with_type_and_name) {
                LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                          << " column type: " << p.type->getName() << " column name: " << p.name
                          << " number of rows: " << p.column->size();
            }

            DB::MutableColumns columns = query_result.mutateColumns();

            size_t number_of_columns = columns.size();
            ASSERT_EQ(number_of_columns, 7U);

            auto& column_0 = assert_cast<DB::ColumnUInt32&>(*columns[0]);         // eventts, DateTime
            auto& column_1 = assert_cast<DB::ColumnInt64&>(*columns[1]);          // seller-id;
            auto& column_2 = assert_cast<DB::ColumnInt64&>(*columns[2]);          // compaign-id;
            auto& column_3 = assert_cast<DB::ColumnInt64&>(*columns[3]);          // item-id;
            auto& column_4 = assert_cast<DB::ColumnString&>(*columns[4]);         // selleerKwid
            auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);       //  ad-groupId
            auto& column_6 = assert_cast<DB::ColumnLowCardinality&>(*columns[6]); // compaign-type

            {
                // eventts value retrieval
                uint32_t datetime_val = column_0.getData()[0];
                LOG(INFO) << "retrieved event-time (datetime): " << datetime_val
                          << " with expected value: " << time_stamp;
                ASSERT_EQ(datetime_val, (uint32_t)(time_stamp / 1000));

                //  sellerid retrieval
                int64_t sellerid_val = column_1.getData()[0];
                LOG(INFO) << "retrieved seller-id: " << sellerid_val << " with expected value: " << seller_id;
                ASSERT_EQ(sellerid_val, seller_id);

                //  compaign-id retrieval
                int64_t compaign_id_val = column_2.getData()[0];
                LOG(INFO) << "retrieved compaign-id: " << compaign_id_val << " with expected value: " << compaign_id;
                ASSERT_EQ(compaign_id_val, compaign_id);

                //  item-id retrieval
                int64_t item_id_val = column_3.getData()[0];
                LOG(INFO) << "retrieved item-id: " << item_id_val << " with expected value: " << item_id;
                ASSERT_EQ(item_id_val, item_id);

                // selleerKwid retrieval
                auto column_4_string = column_4.getDataAt(0);
                std::string column_4_real_string(column_4_string.data, column_4_string.size);
                LOG(INFO) << "retrieved selleerKwId: " << column_4_real_string;
                ASSERT_EQ(column_4_real_string, "normal"); // default value

                // ad-group id retrieval
                auto column_5_nullable_nested_column = column_5.getNestedColumnPtr().get();
                const DB::ColumnString& resulted_column_5_nullable_nested_column =
                    assert_cast<const DB::ColumnString&>(*column_5_nullable_nested_column);

                ASSERT_TRUE(column_5.isNullAt(0));
                if (column_5.isNullAt(0)) {
                    LOG(INFO) << "retrieved ad-group-id is a NULL value";
                } else {
                    auto column_5_real_value = resulted_column_5_nullable_nested_column.getDataAt(0);
                    std::string column_5_real_string_value(column_5_real_value.data, column_5_real_value.size);
                    LOG(INFO) << "retrieved ad-group-id is: " << column_5_real_string_value;
                }

                // compaign-type retrieval
                auto column_6_string = column_6.getDataAt(0);
                std::string column_6_real_string(column_6_string.data, column_6_string.size);
                LOG(INFO) << "retrieved low cardinality (string ) compaign-type: " << column_6_real_string;
                ASSERT_EQ(column_6_real_string, "");
            }
        };

        bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
        ASSERT_TRUE(query_status);

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * Similar to the above test of: InsertARowWithColumnsMissingAndFixingDoneByAggregatorSide. But here the exception fails
 * because the block constructed following the server-side table definition, but the insert query statement only
 * contains the client-side schema that has less columns than the populated blocks, and thus encounters exception.
 *
 * This test case is similar to the one in test_defaults_serialization_loader.cpp,
 *         InsertARowWithDefaultRowMissingAndInsertQueryOnlyWithExplicitColumnNamesAndEncounterException
 *
 * The block structure constructd:
 *
 * test_missing_columns_with_defaults.cpp:516] structure dumped in block holder that is constructed from protobuf-reader
 * that takes into account the server-side schema already: eventts DateTime UInt32(size = 1), sellerId Int64 Int64(size
 * = 1), compaignId Int64 Int64(size = 1), itemId Int64 Int64(size = 1), selleerKwId String String(size = 1), adgroupId
 * Nullable(String) Nullable(size = 1, String(size = 1), UInt8(size = 1)), compaignType LowCardinality(String)
 * ColumnLowCardinality(size = 1, UInt8(size = 1), ColumnUnique(size = 1, String(size = 1)))
 *
 * The insert query statement:
 *
 * insert query query expression is: INSERT INTO simulated_simple_ads_pl0 (eventts, sellerId, compaignId, itemId) VALUES
 *
 * And the exception message:
 *
 *   Received exception Code: 10. DB::Exception: Received from 10.194.224.19:9000. DB::Exception: Not found column
 * selleerKwId in block. There are only columns: eventts, sellerId, compaignId, itemId.
 *
 */
TEST_F(AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest,
       InsertARowWithColumnsMissingAndFixingDoneByAggregatorSideWithException) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string actual_table_name = "simulated_simple_ads_pl0";
    bool removed = removeTableContent(context, ioc, actual_table_name);
    LOG(INFO) << "remove table content for table: " << actual_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    bool failed = false;
    try {

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string missing_columns_table_name = "simulated_simple_ads_pl0_missing_columns";
        bool found_missing_columns_table_name = false;
        bool found_actual_table_name = false;
        for (const auto& table_name : table_names) {
            if (table_name == missing_columns_table_name) {
                found_missing_columns_table_name = true;
            }
            if (table_name == actual_table_name) {
                found_actual_table_name = true;
            }
        }
        ASSERT_TRUE(found_missing_columns_table_name);
        ASSERT_TRUE(found_actual_table_name);

        LOG(INFO) << "retrieved real table definition with missing columns: " << actual_table_name;
        const nuclm::TableColumnsDescription& actual_table_definition =
            manager.getTableColumnsDefinition(actual_table_name);
        LOG(INFO) << " with definition: " << actual_table_definition.str();

        size_t actual_default_columns_count = actual_table_definition.getSizeOfDefaultColumns();
        LOG(INFO) << " size of default columns count is: " << actual_default_columns_count;

        ASSERT_TRUE(actual_default_columns_count == 1);
        size_t actual_ordinary_columns_count = actual_table_definition.getSizeOfOrdinaryColumns();
        LOG(INFO) << " size of ordinary columns count is: " << actual_ordinary_columns_count;

        ASSERT_TRUE(actual_ordinary_columns_count == 6);

        DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
            actual_table_definition.getFullColumnTypesAndNamesDefinition());

        std::string serializedSqlBatchRequestInString;

        size_t rows = 1;
        {
            LOG(INFO) << "to construct and serialized a message";
            std::string table = actual_table_name;
            // To simulate that the client has out-of-date schema.
            std::string sql =
                "insert into " + actual_table_name + " (eventts, sellerId, compaignId, itemId) values (?, ?, ?, ?)";
            std::string shard = "nudata.monstor.cdc.dev.marketing.1";

            nucolumnar::aggregator::v1::DataBindingList bindingList;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
            sqlBatchRequest.set_shard(shard);
            sqlBatchRequest.set_table(table);

            nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                sqlBatchRequest.mutable_nucolumnarencoding();
            sqlWithBatchBindings->set_sql(sql);
            sqlWithBatchBindings->mutable_batch_bindings();

            for (size_t r = 0; r < rows; r++) {
                nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                    sqlWithBatchBindings->add_batch_bindings();
                // value 1: eventts, DateTime
                nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                auto ts = new nucolumnar::datatypes::v1::TimestampP();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch());
                ts->set_milliseconds(ms.count());
                val1->set_allocated_timestamp(ts);

                // value 2: sellerId, int64
                nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                val2->set_long_value(599123456 + r);

                // value 3: compaignId, int64
                nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                val3->set_long_value(600123456 + r);

                // value4: itemId, int64
                nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                val4->set_long_value(601123456 + r);

                nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                pval1->CopyFrom(*val1);

                nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                pval2->CopyFrom(*val2);

                nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                pval3->CopyFrom(*val3);

                nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                pval4->CopyFrom(*val4);
            }

            serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
        }

        // to create a dummy schema_tracker.
        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(actual_table_name, actual_table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        nucolumnar::aggregator::v1::SQLBatchRequest deserialized_batch_request;
        deserialized_batch_request.ParseFromString(serializedSqlBatchRequestInString);

        bool serialization_status = batchReader.read(actual_table_definition, deserialized_batch_request);
        ASSERT_TRUE(serialization_status);

        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

        // Purposely to use only the client-side schema that only contains 4 columns.
        std::string query =
            "insert into " + actual_table_name + " (eventts, sellerId, compaignId, itemId) VALUES (9098, 9097);";
        LOG(INFO) << "chosen table: " << actual_table_name << "with insert query: " << query;
        loader.init();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        size_t total_number_of_columns = block_holder.columns();
        LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names_holder;

        ASSERT_EQ(total_number_of_rows_holder, rows);
        ASSERT_EQ(total_number_of_columns, (size_t)7);

        // We should see totally 7 columns in simulated_simple_ads_pl0
        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder that is constructed from protobuf-reader that takes into "
                     "account the server-side schema already: "
                  << structure;
        bool result = loader.load_buffer(actual_table_name, query, block_holder);

        LOG(WARNING) << "Note: We ARE AWARE OF the test case will lead to exception.";
        ASSERT_FALSE(result);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        LOG(WARNING) << "Note: we ARE AWARE OF the test case will lead to exception.";
        failed = false;
    }

    ASSERT_FALSE(failed);
}

/**
 * Purposely retrieve the table that has less number of columns to simulate the missing of columns, for both the client
 side and the
 * aggregator side. We rely on the server-side to perform fixing of the missing columns against the server-side table
 schema.
 * Note that the columns at the client and at the aggregator side are the sub-set of the columns at the server.
 *
 * That is, we insert data + schema based on the table of:
 *
 * CREATE TABLE simulated_simple_ads_pl0_missing_columns (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64,
 `itemId` Int64)
 *
 * but the server side is with the table of:
 * CREATE TABLE simulated_simple_ads_pl0 (`eventts` DateTime, `sellerId` Int64, `compaignId` Int64, `itemId` Int64,
 `selleerKwId` String DEFAULT 'normal', `adgroupId` Nullable(String), `compaignType` LowCardinality(String))

 * and with the insertion of:
 *
 *  structure dumped in block holder that is constructed from protobuf-reader that takes into account the server-side
 schema already: eventts DateTime UInt32(size = 1), sellerId Int64 Int64(size = 1), compaignId Int64 Int64(size = 1),
 itemId Int64 Int64(size = 1)
 *
 *  insert query query expression is: INSERT INTO simulated_simple_ads_pl1 (eventts, sellerId, compaignId, itemId)
 VALUES
 *
 * And the query result of:
 *
    ┌─────────────eventts─┬──sellerId─┬─compaignId─┬────itemId─┬─selleerKwId─┬─adgroupId─┬─compaignType─┐
    │ 2020-11-24 01:57:43 │ 799123456 │  800123456 │ 801123456 │ normal      │ ᴺᵁᴸᴸ      │              │
    └─────────────────────┴───────────┴────────────┴───────────┴─────────────┴───────────┴──────────────┘
 *
 */
TEST_F(AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest,
       InsertARowWithColumnsMissingAndFixingDoneByClickHouseServerSide) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc =
        AggregatorLoaderMissingColumnsDefaultsHandlingRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string actual_table_name = "simulated_simple_ads_pl1";
    bool removed = removeTableContent(context, ioc, actual_table_name);
    LOG(INFO) << "remove table content for table: " << actual_table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    bool failed = false;
    size_t rows = 5;

    std::vector<long> time_stamp_array;
    std::vector<long> seller_id_array;
    std::vector<long> compaign_id_array;
    std::vector<long> item_id_array;

    srand(time(NULL));
    // to enforce the ordering that is based on: ORDER BY (sellerId, compaignId, compaignType)
    long init_rand_val = rand() % 100000000;

    try {

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 1);

        std::string missing_columns_table_name = "simulated_simple_ads_pl1_missing_columns";
        bool found_missing_columns_table_name = false;
        bool found_actual_table_name = false;
        for (const auto& table_name : table_names) {
            if (table_name == missing_columns_table_name) {
                found_missing_columns_table_name = true;
            }
            if (table_name == actual_table_name) {
                found_actual_table_name = true;
            }
        }
        ASSERT_TRUE(found_missing_columns_table_name);
        ASSERT_TRUE(found_actual_table_name);

        LOG(INFO) << "retrieved table definition with missing columns: " << missing_columns_table_name;
        const nuclm::TableColumnsDescription& missing_columns_table_definition =
            manager.getTableColumnsDefinition(missing_columns_table_name);
        LOG(INFO) << " with definition: " << missing_columns_table_definition.str();

        size_t default_columns_count = missing_columns_table_definition.getSizeOfDefaultColumns();
        LOG(INFO) << " size of default columns count is: " << default_columns_count;

        ASSERT_TRUE(default_columns_count == 0);
        size_t ordinary_columns_count = missing_columns_table_definition.getSizeOfOrdinaryColumns();
        LOG(INFO) << " size of ordinary columns count is: " << ordinary_columns_count;

        ASSERT_TRUE(ordinary_columns_count == 4);

        // Purposely using the out-of-date schema.
        DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
            missing_columns_table_definition.getFullColumnTypesAndNamesDefinition());

        LOG(INFO) << "retrieved real table definition with missing columns: " << actual_table_name;
        const nuclm::TableColumnsDescription& actual_table_definition =
            manager.getTableColumnsDefinition(actual_table_name);
        LOG(INFO) << " with definition: " << actual_table_definition.str();

        size_t actual_default_columns_count = actual_table_definition.getSizeOfDefaultColumns();
        LOG(INFO) << " size of default columns count is: " << actual_default_columns_count;

        ASSERT_TRUE(actual_default_columns_count == 1);
        size_t actual_ordinary_columns_count = actual_table_definition.getSizeOfOrdinaryColumns();
        LOG(INFO) << " size of ordinary columns count is: " << actual_ordinary_columns_count;

        ASSERT_TRUE(actual_ordinary_columns_count == 6);

        std::string serializedSqlBatchRequestInString;

        {
            LOG(INFO) << "to construct and serialized a message";
            std::string table = actual_table_name;
            // To simulate that we only insert 4 columns (defined in simulated_simple_ads_pl1_missing_columns)
            // to the actual table of simulated_simple_ads_pl1.
            std::string sql =
                "insert into " + actual_table_name + " (eventts, sellerId, compaignId, itemId) values (?, ?, ?, ?)";
            std::string shard = "nudata.monstor.cdc.dev.marketing.1";

            nucolumnar::aggregator::v1::DataBindingList bindingList;
            nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
            sqlBatchRequest.set_shard(shard);
            sqlBatchRequest.set_table(table);

            nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
                sqlBatchRequest.mutable_nucolumnarencoding();
            sqlWithBatchBindings->set_sql(sql);
            sqlWithBatchBindings->mutable_batch_bindings();

            for (size_t r = 0; r < rows; r++) {
                nucolumnar::aggregator::v1::DataBindingList* dataBindingList =
                    sqlWithBatchBindings->add_batch_bindings();
                // value 1: eventts, DateTime
                nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                auto ts = new nucolumnar::datatypes::v1::TimestampP();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch());
                ts->set_milliseconds(ms.count());
                val1->set_allocated_timestamp(ts);
                time_stamp_array.push_back(ms.count());

                // value 2: sellerId, int64
                nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                long seller_id = init_rand_val++;
                val2->set_long_value(seller_id);
                seller_id_array.push_back(seller_id);

                // value 3: compaignId, int64
                nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
                long compaign_id = init_rand_val++;
                val3->set_long_value(compaign_id);
                compaign_id_array.push_back(compaign_id);

                // value4: itemId, int64
                nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
                long item_id = init_rand_val++;
                val4->set_long_value(item_id);
                item_id_array.push_back(item_id);

                nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                pval1->CopyFrom(*val1);

                nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                pval2->CopyFrom(*val2);

                nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
                pval3->CopyFrom(*val3);

                nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
                pval4->CopyFrom(*val4);
            }

            serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
        }

        // to create a dummy schema_tracker.
        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(actual_table_name, actual_table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        nucolumnar::aggregator::v1::SQLBatchRequest deserialized_batch_request;
        deserialized_batch_request.ParseFromString(serializedSqlBatchRequestInString);

        // Purposely using the out-of-date table definition, and as a result, the block constructed will follow
        // the schema of the table: missing_columns_table_name
        bool serialization_status = batchReader.read(missing_columns_table_definition, deserialized_batch_request);
        ASSERT_TRUE(serialization_status);

        nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

        // Purposely to insert to actual table: simulated_simple_ads_pl0, but with the knowledge of the columns that are
        // with the missing columns one. The actual values field do not matter, as it will be truncated by the
        // Aggregator Loader.
        std::string query =
            "insert into " + actual_table_name + " (eventts, sellerId, compaignId, itemId) VALUES (9098, 9097);";
        LOG(INFO) << "chosen table: " << actual_table_name << "with insert query: " << query;
        loader.init();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        size_t total_number_of_columns = block_holder.columns();
        LOG(INFO) << "total number of columns in block holder: " << total_number_of_columns;

        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names_holder;

        ASSERT_EQ(total_number_of_rows_holder, rows);
        ASSERT_EQ(total_number_of_columns, (size_t)4);

        // We should only see 4 columns as we are using the out-of-date schema.
        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder that is constructed from protobuf-reader that uses the "
                     "out-of-date table definition: "
                  << structure;
        bool result = loader.load_buffer(actual_table_name, query, block_holder);

        ASSERT_TRUE(result);

        std::string table_being_queried = "default." + actual_table_name;
        std::string query_on_table = "select eventts , \n"   /* 0. DateTime*/
                                     "     sellerId,\n"      /* 1. Int64 */
                                     "     compaignId,\n"    /* 2. Int64*/
                                     "     itemId, \n"       /* 3. Int64 */
                                     "     selleerKwId , \n" /* 4. String, with default: normal  */
                                     "     adgroupId , \n"   /* 5. Nullable (String) */
                                     "     compaignType \n"  /* 6. LowCardinality(String)*/
                                     "from " +
            table_being_queried +
            "\n"
            "order by (sellerId, compaignId, compaignType)";

        auto row_based_inspector = [&](DB::Block& query_result, DB::Block& sample_block) {
            const DB::ColumnsWithTypeAndName& columns_with_type_and_name = sample_block.getColumnsWithTypeAndName();
            int column_index = 0;

            for (auto& p : columns_with_type_and_name) {
                LOG(INFO) << "table: " << table_being_queried << " with  column index: " << column_index++
                          << " column type: " << p.type->getName() << " column name: " << p.name
                          << " number of rows: " << p.column->size();
            }

            DB::MutableColumns columns = query_result.mutateColumns();

            size_t number_of_columns = columns.size();
            ASSERT_EQ(number_of_columns, 7U);

            auto& column_0 = assert_cast<DB::ColumnUInt32&>(*columns[0]);         // eventts, DateTime
            auto& column_1 = assert_cast<DB::ColumnInt64&>(*columns[1]);          // seller-id;
            auto& column_2 = assert_cast<DB::ColumnInt64&>(*columns[2]);          // compaign-id;
            auto& column_3 = assert_cast<DB::ColumnInt64&>(*columns[3]);          // item-id;
            auto& column_4 = assert_cast<DB::ColumnString&>(*columns[4]);         // selleerKwid
            auto& column_5 = assert_cast<DB::ColumnNullable&>(*columns[5]);       //  ad-groupId
            auto& column_6 = assert_cast<DB::ColumnLowCardinality&>(*columns[6]); // compaign-type

            for (size_t i = 0; i < rows; i++) {
                // eventts value retrieval
                uint32_t datetime_val = column_0.getData()[i];
                LOG(INFO) << "retrieved event-time (datetime): " << datetime_val
                          << " with expected value: " << time_stamp_array[i];
                ASSERT_EQ(datetime_val, (uint32_t)(time_stamp_array[i] / 1000));

                //  sellerid retrieval
                int64_t sellerid_val = column_1.getData()[i];
                LOG(INFO) << "retrieved seller-id: " << sellerid_val << " with expected value: " << seller_id_array[i];
                ASSERT_EQ(sellerid_val, seller_id_array[i]);

                //  compaign-id retrieval
                int64_t compaign_id_val = column_2.getData()[i];
                LOG(INFO) << "retrieved compaign-id: " << compaign_id_val
                          << " with expected value: " << compaign_id_array[i];
                ASSERT_EQ(compaign_id_val, compaign_id_array[i]);

                //  item-id retrieval
                int64_t item_id_val = column_3.getData()[i];
                LOG(INFO) << "retrieved item-id: " << item_id_val << " with expected value: " << item_id_array[i];
                ASSERT_EQ(item_id_val, item_id_array[i]);

                // selleerKwid retrieval
                auto column_4_string = column_4.getDataAt(i);
                std::string column_4_real_string(column_4_string.data, column_4_string.size);
                LOG(INFO) << "retrieved selleerKwId: " << column_4_real_string;
                ASSERT_EQ(column_4_real_string, "normal"); // default value

                // ad-group id retrieval
                auto column_5_nullable_nested_column = column_5.getNestedColumnPtr().get();
                const DB::ColumnString& resulted_column_5_nullable_nested_column =
                    assert_cast<const DB::ColumnString&>(*column_5_nullable_nested_column);

                ASSERT_TRUE(column_5.isNullAt(i));
                if (column_5.isNullAt(i)) {
                    LOG(INFO) << "retrieved ad-group-id is a NULL value";
                } else {
                    auto column_5_real_value = resulted_column_5_nullable_nested_column.getDataAt(0);
                    std::string column_5_real_string_value(column_5_real_value.data, column_5_real_value.size);
                    LOG(INFO) << "retrieved ad-group-id is: " << column_5_real_string_value;
                }

                // compaign-type retrieval
                auto column_6_string = column_6.getDataAt(i);
                std::string column_6_real_string(column_6_string.data, column_6_string.size);
                LOG(INFO) << "retrieved low cardinality (string ) compaign-type: " << column_6_real_string;
                ASSERT_EQ(column_6_real_string, "");
            }
        };

        bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
        ASSERT_TRUE(query_status);

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * Use xdr_test to test out missing columns
 */

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
