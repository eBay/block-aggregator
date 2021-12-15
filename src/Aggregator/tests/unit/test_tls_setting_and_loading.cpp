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

#include <Aggregator/SSLEnabledApplication.h>
#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/AggregatorLoaderManager.h>

#include <Common/Exception.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/ColumnsDescription.h>
#include <DataStreams/AddingDefaultBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>

#include <nucolumnar/aggregator/v1/nucolumnaraggregator.pb.h>
#include <nucolumnar/datatypes/v1/columnartypes.pb.h>

#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
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

class AggregatorLoaderTLSRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderTLSRelatedTest::shared_context = nullptr;

TEST_F(AggregatorLoaderTLSRelatedTest, testLoadingConfigurationFile) {
    std::string the_path = getConfigFilePath("example_aggregator_config_with_tls.json");
    LOG(INFO) << " JSON configuration file path is: " << the_path;

    try {
        SETTINGS_FACTORY.load(the_path);
        std::string config_file_path = SETTINGS_FACTORY.get_local_config_file();
        boost::filesystem::path path(config_file_path);
        std::unique_ptr<nuclm::SSLEnabledApplication> application_ptr =
            std::make_unique<nuclm::SSLEnabledApplication>(path.parent_path().string());
        application_ptr->init();
        bool initialized_result = application_ptr->isInitialized();

        ASSERT_EQ(initialized_result, true);

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
    }
}

TEST_F(AggregatorLoaderTLSRelatedTest, testLoadinOneBlock) {
    std::string the_path = getConfigFilePath("example_aggregator_config_with_tls.json");
    LOG(INFO) << " JSON configuration file path is: " << the_path;

    DB::ContextMutablePtr context = AggregatorLoaderTLSRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderTLSRelatedTest::shared_context->getIOContext();

    SETTINGS_FACTORY.load(the_path); // force to load the configuration setting as the global instance.
    try {
        std::string config_file_path = SETTINGS_FACTORY.get_local_config_file();
        boost::filesystem::path path(config_file_path);
        std::unique_ptr<nuclm::SSLEnabledApplication> application_ptr =
            std::make_unique<nuclm::SSLEnabledApplication>(path.parent_path().string());
        application_ptr->init();
        bool initialized_result = application_ptr->isInitialized();

        ASSERT_EQ(initialized_result, true);

        nuclm::AggregatorLoaderManager manager(context, ioc);
        manager.initLoaderTableDefinitions();
        std::vector<std::string> table_names = manager.getDefinedTableNames();
        ASSERT_TRUE(table_names.size() > 0);

        for (const auto& table_name : table_names) {
            if (table_name == "xdr_tst2") {
                LOG(INFO) << "retrieved table name: " << table_name;
                const nuclm::TableColumnsDescription& table_definition = manager.getTableColumnsDefinition(table_name);
                LOG(INFO) << " with definition: " << table_definition.str();

                size_t default_columns_count = table_definition.getSizeOfDefaultColumns();
                LOG(INFO) << " size of default columns count is: " << default_columns_count;

                ASSERT_TRUE(default_columns_count == 2);
                size_t ordinary_columns_count = table_definition.getSizeOfOrdinaryColumns();
                LOG(INFO) << " size of ordinary columns count is: " << default_columns_count;

                ASSERT_TRUE(ordinary_columns_count == 2);
                nuclm::ColumnTypesAndNamesTableDefinition full_columns_definition =
                    table_definition.getFullColumnTypesAndNamesDefinition();

                DB::Block block_holder = nuclm::SerializationHelper::getBlockDefinition(
                    table_definition.getFullColumnTypesAndNamesDefinition());
                std::string serializedSqlBatchRequestInString;

                size_t rows = 1;
                {
                    LOG(INFO) << "to construct and serialized a message";
                    std::string table = table_name;
                    std::string sql = "insert into xdr_tst2 (end_date, start_date) VALUES (?, ?);";
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
                        // value end_date,
                        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
                        val1->set_long_value(9923456 + r);
                        // value 2
                        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
                        val2->set_long_value(9923456 + r);

                        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
                        pval1->CopyFrom(*val1);

                        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
                        pval2->CopyFrom(*val2);
                    }

                    serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
                }

                nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
                    std::make_shared<nuclm::TableSchemaUpdateTracker>(table_name, table_definition, manager);
                nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr,
                                                       block_holder, context);

                bool serialization_status = batchReader.read();
                ASSERT_TRUE(serialization_status);

                nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

                // for table: xdr_tst2, here we need to specify all of the four columns, as the missing columns should
                // have been populated.
                std::string query = "insert into xdr_tst2 (`date`, `answered`, `end_date`, `start_date` ) VALUES "
                                    "('02-15-2020', 8, 9098, 9097);";
                LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
                loader.init();

                size_t total_number_of_rows_holder = block_holder.rows();
                LOG(INFO) << "fINAL total number of rows in block holder: " << total_number_of_rows_holder;
                std::string names_holder = block_holder.dumpNames();
                LOG(INFO) << "final column names dumped in block holder : " << names_holder;

                ASSERT_EQ(total_number_of_rows_holder, rows);

                std::string structure = block_holder.dumpStructure();
                LOG(INFO) << "final structure dumped in block holder: " << structure;
                bool result = loader.load_buffer(table_name, query, block_holder);

                ASSERT_TRUE(result);
            }
        }

    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
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
