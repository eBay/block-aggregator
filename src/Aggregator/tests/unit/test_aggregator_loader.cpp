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

/**
 * Take the block definition from Interpreters/InternalTextLogsQueue, but not include the definition in this
 * test class in order to avoid the compilation error.
 */
class SimplifiedInternalTextLogsQueue {
  public:
    static DB::Block getSampleBlock() {
        return DB::Block{{std::make_shared<DB::DataTypeDateTime>(), "event_time"},
                         {std::make_shared<DB::DataTypeUInt32>(), "event_time_microseconds"},
                         {std::make_shared<DB::DataTypeString>(), "host_name"},
                         {std::make_shared<DB::DataTypeString>(), "query_id"},
                         {std::make_shared<DB::DataTypeUInt64>(), "thread_id"},
                         {std::make_shared<DB::DataTypeInt8>(), "priority"},
                         {std::make_shared<DB::DataTypeString>(), "source"},
                         {std::make_shared<DB::DataTypeString>(), "text"}};
    }
};

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

class AggregatorLoaderRelatedTest : public ::testing::Test {
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

ContextWrapper* AggregatorLoaderRelatedTest::shared_context = nullptr;

TEST_F(AggregatorLoaderRelatedTest, testBlockOutputStreamWithOneBlockOutput) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    nuclm::LoaderBlockOutputStream loader_block_output_stream(context);

    // construct a sample block
    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"}};

    DB::Block sampleBlock{columns_with_types_and_names};
    bool result = true;
    try {

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        size_t n = 5;
        {
            // insert data: 'graphdb-5000", with the default value hopefully being populated by the server.
            const char* s = "graphdb-5000";
            size_t size = strlen(s) + 1;
            DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[0])).getChars();
            DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[0])).getOffsets();

            data.resize(n * size); // 1 entry
            offsets.resize(n);     // 1 entry

            for (size_t i = 0; i < n; ++i) {
                memcpy(&data[i * size], s, size);
                offsets[i] = (i + 1) * size;
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));

        size_t total_number_of_rows = sampleBlock.rows();
        LOG(INFO) << "total number of rows: " << total_number_of_rows;

        std::string names = sampleBlock.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        std::string structure = sampleBlock.dumpStructure();
        LOG(INFO) << "structure dumped: " << structure;

        loader_block_output_stream.initBlockOutputStream(sampleBlock);
        if (sampleBlock.rows() != 0) {
            loader_block_output_stream.write(sampleBlock);
        }

        /// Received data block is immediately displayed to the log
        std::string str_out = loader_block_output_stream.flush();
        /// use the following format to allow the block-flushed output to be aligned with each other.
        LOG(INFO) << "Loader OutputStream block flushed:\n" << str_out;

        loader_block_output_stream.writeSuffix();
        LOG(INFO) << "Also, Loader OutputStream write suffix before being cleaned up";

    } catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        result = false;
    }

    ASSERT_EQ(result, true);
}

TEST_F(AggregatorLoaderRelatedTest, testLogOutputStreamWithOneLogOutput) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    nuclm::LoaderLogOutputStream loader_log_output_stream;

    // construct a sample block
    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"}};

    DB::Block sampleBlock = SimplifiedInternalTextLogsQueue::getSampleBlock();

    bool result = true;
    try {

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        size_t n = 1;

        // column 0 (event_time)
        {
            DB::ColumnUInt32::Container& vec = (assert_cast<DB::ColumnUInt32&>(*columns[0])).getData();
            DB::DataTypeUInt32 data_type;

            vec.resize(n);
            for (size_t i = 0; i < n; ++i) {
                vec[i] = (unsigned long)(1990 * i);
            }
        }

        // column 1 (event time miscroseconds)
        {
            DB::ColumnUInt32::Container& vec = (assert_cast<DB::ColumnUInt32&>(*columns[1])).getData();
            DB::DataTypeUInt32 data_type;

            vec.resize(n);
            for (size_t i = 0; i < n; ++i) {
                vec[i] = (unsigned int)(20200000 * i);
            }
        }

        // column 2 (host name)
        {{const char* s = "graphdb-9000";
        size_t size = strlen(s) + 1;
        DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[2])).getChars();
        DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[2])).getOffsets();

        data.resize(n * size); // 1 entry
        offsets.resize(n);     // 1 entry

        for (size_t i = 0; i < n; ++i) {
            memcpy(&data[i * size], s, size);
            offsets[i] = (i + 1) * size;
        }
    }
}

// column 3 (query id)
{{const char* s = "query-800000";
size_t size = strlen(s) + 1;
DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[3])).getChars();
DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[3])).getOffsets();

data.resize(n* size); // 1 entry
offsets.resize(n);    // 1 entry

for (size_t i = 0; i < n; ++i) {
    memcpy(&data[i * size], s, size);
    offsets[i] = (i + 1) * size;
}
}
}

// column 4: (thread id)
{
    DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[4])).getData();
    DB::DataTypeUInt64 data_type;

    vec.resize(n);
    for (size_t i = 0; i < n; ++i) {
        vec[i] = (unsigned long)(9080786 * i);
    }
}

// column 5: (priority)
{
    DB::ColumnInt8::Container& vec = (assert_cast<DB::ColumnInt8&>(*columns[5])).getData();
    DB::DataTypeInt8 data_type;

    vec.resize(n);
    for (size_t i = 0; i < n; ++i) {
        vec[i] = (int8_t)(1 * i);
    }
}

// column 6: (source)
{{const char* s = "graphdb-200000";
size_t size = strlen(s) + 1;
DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[6])).getChars();
DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[6])).getOffsets();

data.resize(n* size); // 1 entry
offsets.resize(n);    // 1 entry

for (size_t i = 0; i < n; ++i) {
    memcpy(&data[i * size], s, size);
    offsets[i] = (i + 1) * size;
}
}
}

// column 7: (text)
{
    {
        const char* s = "no comments";
        size_t size = strlen(s) + 1;
        DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[7])).getChars();
        DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[7])).getOffsets();

        data.resize(n * size); // 1 entry
        offsets.resize(n);     // 1 entry

        for (size_t i = 0; i < n; ++i) {
            memcpy(&data[i * size], s, size);
            offsets[i] = (i + 1) * size;
        }
    }
}

/// construct the block.
sampleBlock.setColumns(std::move(columns));

size_t total_number_of_rows = sampleBlock.rows();
LOG(INFO) << "total number of rows: " << total_number_of_rows;

std::string names = sampleBlock.dumpNames();
LOG(INFO) << "column names dumped : " << names;

std::string structure = sampleBlock.dumpStructure();
LOG(INFO) << "structure dumped: " << structure;

loader_log_output_stream.initLogOutputStream();
if (sampleBlock.rows() != 0) {
    loader_log_output_stream.write(sampleBlock);
}

/// Received data block is immediately displayed to the log
std::string str_out = loader_log_output_stream.flush();
/// use the following format to allow the block-flushed output to be aligned with each other.
LOG(INFO) << "Loader LogOutputStream block flushed:\n" << str_out;

loader_log_output_stream.writeSuffix();
LOG(INFO) << "Also, Loader LogOutputStream write suffix before being cleaned up";
}
catch (const DB::Exception& e) {
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    result = false;
}

ASSERT_EQ(result, true);
}

TEST_F(AggregatorLoaderRelatedTest, testLogOutputStreamWithMutlipleLogOutputs) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    nuclm::LoaderLogOutputStream loader_log_output_stream;

    // construct a sample block
    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"}};

    DB::Block sampleBlock = SimplifiedInternalTextLogsQueue::getSampleBlock();

    bool result = true;
    size_t loop_counter = 5;
    for (size_t loop = 0; loop < loop_counter; loop++) {
        try {

            DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

            size_t n = 1;

            // column 0 (event_time)
            {
                DB::ColumnUInt32::Container& vec = (assert_cast<DB::ColumnUInt32&>(*columns[0])).getData();
                DB::DataTypeUInt32 data_type;

                vec.resize(n);
                for (size_t i = 0; i < n; ++i) {
                    vec[i] = (unsigned long)(1990 * i);
                }
            }

            // column 1 (event time miscroseconds)
            {
                DB::ColumnUInt32::Container& vec = (assert_cast<DB::ColumnUInt32&>(*columns[1])).getData();
                DB::DataTypeUInt32 data_type;

                vec.resize(n);
                for (size_t i = 0; i < n; ++i) {
                    vec[i] = (unsigned int)(20200000 * i);
                }
            }

            // column 2 (host name)
            {{std::string host_name = "graphdb-9000-loopcounter" + std::to_string(loop);
            const char* s = host_name.c_str();

            size_t size = strlen(s) + 1;
            DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[2])).getChars();
            DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[2])).getOffsets();

            data.resize(n * size); // 1 entry
            offsets.resize(n);     // 1 entry

            for (size_t i = 0; i < n; ++i) {
                memcpy(&data[i * size], s, size);
                offsets[i] = (i + 1) * size;
            }
        }
    }

    // column 3 (query id)
    {{std::string query_id = "query-800000-loopcounter" + std::to_string(loop);
    const char* s = query_id.c_str();
    size_t size = strlen(s) + 1;
    DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[3])).getChars();
    DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[3])).getOffsets();

    data.resize(n * size); // 1 entry
    offsets.resize(n);     // 1 entry

    for (size_t i = 0; i < n; ++i) {
        memcpy(&data[i * size], s, size);
        offsets[i] = (i + 1) * size;
    }
}
}

// column 4: (thread id)
{
    DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[4])).getData();
    DB::DataTypeUInt64 data_type;

    vec.resize(n);
    for (size_t i = 0; i < n; ++i) {
        vec[i] = (unsigned long)(9080786 * i);
    }
}

// column 5: (priority)
{
    DB::ColumnInt8::Container& vec = (assert_cast<DB::ColumnInt8&>(*columns[5])).getData();
    DB::DataTypeInt8 data_type;

    vec.resize(n);
    for (size_t i = 0; i < n; ++i) {
        vec[i] = (int8_t)(1 * i);
    }
}

// column 6: (source)
{{std::string source = "graphdb-200000-loopcounter" + std::to_string(loop);
const char* s = source.c_str();
size_t size = strlen(s) + 1;
DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[6])).getChars();
DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[6])).getOffsets();

data.resize(n* size); // 1 entry
offsets.resize(n);    // 1 entry

for (size_t i = 0; i < n; ++i) {
    memcpy(&data[i * size], s, size);
    offsets[i] = (i + 1) * size;
}
}
}

// column 7: (text)
{
    {
        std::string text = "no comments loopcounter:" + std::to_string(loop);
        const char* s = text.c_str();
        size_t size = strlen(s) + 1;
        DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[7])).getChars();
        DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[7])).getOffsets();

        data.resize(n * size); // 1 entry
        offsets.resize(n);     // 1 entry

        for (size_t i = 0; i < n; ++i) {
            memcpy(&data[i * size], s, size);
            offsets[i] = (i + 1) * size;
        }
    }
}

/// construct the block.
sampleBlock.setColumns(std::move(columns));

size_t total_number_of_rows = sampleBlock.rows();
LOG(INFO) << "total number of rows: " << total_number_of_rows;

std::string names = sampleBlock.dumpNames();
LOG(INFO) << "column names dumped : " << names;

std::string structure = sampleBlock.dumpStructure();
LOG(INFO) << "structure dumped: " << structure;

loader_log_output_stream.initLogOutputStream();
if (sampleBlock.rows() != 0) {
    loader_log_output_stream.write(sampleBlock);
}

/// Received data block is immediately displayed to the log
std::string str_out = loader_log_output_stream.flush();
/// use the following format to allow the block-flushed output to be aligned with each other.
LOG(INFO) << "Loader LogOutputStream block flushed:\n" << str_out;

loader_log_output_stream.writeSuffix();
LOG(INFO) << "Also, Loader LogOutputStream write suffix before being cleaned up";
}
catch (const DB::Exception& e) {
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    result = false;
}

ASSERT_EQ(result, true);
}
}

/**
 * Because we initialize the block for each data block. we should be able to see the block being flushed without
 * duplication.
 *
 */
TEST_F(AggregatorLoaderRelatedTest, testBlockOutputStreamWithMultipleConsecutiveBlockOutputs) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    nuclm::LoaderBlockOutputStream loader_block_output_stream(context);

    // construct a sample block
    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"}};

    size_t loop_counter = 5;

    for (size_t loop = 0; loop < loop_counter; loop++) {
        DB::Block sampleBlock{columns_with_types_and_names};
        bool result = true;
        try {

            DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

            size_t n = 3;
            {
                // insert data: 'graphdb-5000", with the default value hopefully being populated by the server.
                std::string row = "graphdb-5000" + std::to_string(loop);
                const char* s = row.c_str();
                size_t size = strlen(s) + 1;
                DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[0])).getChars();
                DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[0])).getOffsets();

                data.resize(n * size); // 1 entry
                offsets.resize(n);     // 1 entry

                for (size_t i = 0; i < n; ++i) {
                    memcpy(&data[i * size], s, size);
                    offsets[i] = (i + 1) * size;
                }
            }

            /// construct the block.
            sampleBlock.setColumns(std::move(columns));

            size_t total_number_of_rows = sampleBlock.rows();
            LOG(INFO) << "total number of rows: " << total_number_of_rows;

            std::string names = sampleBlock.dumpNames();
            LOG(INFO) << "column names dumped : " << names;

            std::string structure = sampleBlock.dumpStructure();
            LOG(INFO) << "structure dumped: " << structure;

            loader_block_output_stream.initBlockOutputStream(sampleBlock);
            if (sampleBlock.rows() != 0) {
                loader_block_output_stream.write(sampleBlock);
            }

            /// Received data block is immediately displayed to the log
            std::string str_out = loader_block_output_stream.flush();
            /// use the following format to allow the block-flushed output to be aligned with each other.
            LOG(INFO) << "Loader OutputStream block flushed:\n" << str_out;

            loader_block_output_stream.writeSuffix();
            LOG(INFO) << "Also, Loader OutputStream write suffix before being cleaned up";

        } catch (const DB::Exception& e) {
            std::cerr << e.what() << ", " << e.displayText() << std::endl;
            result = false;
        }

        ASSERT_EQ(result, true);
    }
}

TEST_F(AggregatorLoaderRelatedTest, initConnectionToCHServer) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    bool result = loader.init();
    ASSERT_TRUE(result);
}

/**
 *
 * Note: we need to ensure that each time we run the test case, the block being generated is unique. Otherwise, the
 block
 * being inserted will not get into the database due to de-duplication and when we retrieve the inserted block, we will
 not
 * find the block that we just inserted, even though we do not see any exception from the test run.
 *
 * The output of the row inserted is:
 * ┌─Host──────┬─Colo─┬─EventName─┬───Count─┬─Duration─┬───────────TimeStamp─┐
   │ graphdb-1 │ LVS  │ RESTART   │ 4792331 │  4793331 │ 2021-09-24 22:00:06 │
   └───────────┴──────┴───────────┴─────────┴──────────┴─────────────────────┘
 */

TEST_F(AggregatorLoaderRelatedTest, InsertARowToCHServerWithInsertQuery) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());

    srand(time(NULL)); // to gurantee that the random number is different each time when the test case runs.
    int counter_val_defined = rand() % 10000000;
    int duration_val_defined = counter_val_defined + 1000;

    /** https://clickhouse.com/docs/en/sql-reference/data-types/datetime/
     *  When inserting datetime as an integer, it is treated as Unix Timestamp (UTC). And when outputting as string,
     *  the datetime value will be shown in the format that follows the time zone specified in the column schema.
     *
     *  When inserting string value as datetime, it is treated as being in column timezone.
     */
    // The Epoch time below corresponds to: Friday, September 24, 2021 10:00:06 PM,  https://www.epochconverter.com/
    //
    uint32_t seconds_since_epoch = 1632520806;
    std::string query = "insert into " + table_name +
        " (`Host`, `Colo`, `EventName`, `Count`, `Duration`, `TimeStamp`)"
        " VALUES ('graphdb-1', 'LVS', 'RESTART', " +
        std::to_string(counter_val_defined) + ",  " + std::to_string(duration_val_defined) + ",  " +
        std::to_string(seconds_since_epoch) + ") ";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();
    bool result = loader.load_buffer(table_name, query);
    ASSERT_TRUE(result);

    std::string host_name = "graphdb-1";
    std::string colo_name = "LVS";
    std::string event_name = "RESTART";

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Host , \n"  /*  0. String */
                                 "     Colo,\n"      /* 1. String*/
                                 "     EventName,\n" /* 2. String */
                                 "     Count,\n"     /*  3. UInt64 */
                                 "     Duration,\n"  /*  4. Float32 */
                                 "     TimeStamp \n" /* 5. DateTime */
                                 "from " +
        table_being_queried +
        "\n"
        "order by (Host, Colo, EventName)";

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
        ASSERT_EQ(number_of_columns, 6U);

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);  // Host
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);  // Colo
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);  // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);  // Count
        auto& column_4 = assert_cast<DB::ColumnFloat32&>(*columns[4]); // Float32
        auto& column_5 = assert_cast<DB::ColumnUInt32&>(*columns[5]);  // DateTime

        // Host value retrieval.
        auto column_0_string = column_0.getDataAt(0); // We only have 1 row.
        std::string column_0_real_string(column_0_string.data, column_0_string.size);
        LOG(INFO) << "retrieved host name is: " << column_0_real_string;
        ASSERT_EQ(column_0_real_string, host_name);

        // Colo value retrieval
        auto column_1_real_string = column_1.getDataAt(0);
        std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
        LOG(INFO) << "retrieved fixed string colo name: " << column_1_real_string_value;
        ASSERT_EQ(column_1_real_string_value, colo_name);

        // EventName retrieval
        auto column_2_string = column_2.getDataAt(0); // We only have 1 row.
        std::string column_2_real_string(column_2_string.data, column_2_string.size);
        LOG(INFO) << "retrieved event name: " << column_2_real_string;
        ASSERT_EQ(column_2_real_string, event_name);

        // count retrieval
        uint64_t counter_val = column_3.getData()[0];
        LOG(INFO) << "retrieved Count: " << counter_val << " with expected value: " << counter_val_defined;
        ASSERT_EQ(counter_val, (uint64_t)counter_val_defined);

        // duration retrieval
        float duration_val = column_4.getData()[0];
        LOG(INFO) << "retrieved duration: " << duration_val << " with expected value: " << duration_val_defined;
        ASSERT_EQ(duration_val, (float)duration_val_defined);

        // timestamp retrieval
        uint32_t timestamp_val = column_5.getData()[0];
        LOG(INFO) << "retrieved timestamp value: " << timestamp_val << " with expected value: " << seconds_since_epoch;
        ASSERT_EQ(timestamp_val, seconds_since_epoch);
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * NOTE: the method of "insert" used for column insertion is wrong for date type. So this test is just to show that
 * the way to invoke the column insertion is incorrect.
 */
/*
 TEST_F (AggregatorLoaderRelatedTest, DISABLED_InsertARowToCHServerWithDirectBlock1) {
    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();

    SETTINGS_FACTORY.load(path); //force to load the configuration setting as the global instance.
    nuclm::AggregatorLoader loader (context);
    loader.init();

    std::string query ="insert into simple_event (`Host`, `Colo`, `EventName`, `Count`, `Duration`, `TimeStamp`) VALUES
('graphdb-1', 'LVS', 'RESTART', 1,  1000,  '2016-06-15 23:00:06');"; std::string table_name = "simple_event";

    DB::ColumnsWithTypeAndName columns_with_types_and_names {
            {std::make_shared<DB::DataTypeString>(), "Host"},
            {std::make_shared<DB::DataTypeString>(), "Colo"},
            {std::make_shared<DB::DataTypeString>(), "EventName"},
            {std::make_shared<DB::DataTypeUInt64>(), "Count"},
            {std::make_shared<DB::DataTypeFloat32>(), "Duration"},
            {std::make_shared<DB::DataTypeDateTime>(), "TimeStamp"}
    };

    DB::Block sampleBlock {columns_with_types_and_names};

    try
    {

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        {
            columns[0]->insert("graphdb-48");
            columns[1]->insert("LVS");
            columns[2]->insert("RESTARTED");
            columns[3]->insert(1);
            columns[4]->insert(0.20);

            //
            // NOTE: the following field is incorrectly constructed., from the retrieved query result. The returned
query result is:
            // graphdb-41 │ LVS  │ RESTARTED │     1 │      0.2 │ 2066-10-10 13:03:44 |
            //
            columns[5]->insert("2019-12-26 23:00:06");
        }

        ///construct the block.
        sampleBlock.setColumns(std::move(columns));

        size_t total_number_of_rows = sampleBlock.rows();
        LOG(INFO) << "total number of rows: " <<  total_number_of_rows;

        std::string names = sampleBlock.dumpNames();
        LOG(INFO) << "column names dumped : " <<  names;

        std::string structure = sampleBlock.dumpStructure();
        LOG(INFO) << "structure dumped: " << structure;

    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
    }

    bool result = loader.load_buffer (table_name, query, sampleBlock);

    ASSERT_TRUE(result);
}
*/

/**
 *  Note: we need to ensure that each time we run the test case, the block being generated is unique. Otherwise, the
 * block being inserted will not get into the database due to de-duplication and when we retrieve the inserted block, we
 * will not find the block that we just inserted, even though we do not see any exception from the test run.
 *
 * This test demonstrated that although the schema has two columns and we populate exactly two columns. The database
 * server still accept it but show that the retrieved COLO column is empty (or null?)
 *
 */
TEST_F(AggregatorLoaderRelatedTest, InsertARowToCHServerWithDirectBlock2) {

    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_3";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    std::string query = "insert into " + table_name + " ( `Count`, `Host`) VALUES (100, 'graphdb-18');";

    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeUInt64>(), "Count"},
                                                            {std::make_shared<DB::DataTypeString>(), "Host"}};

    DB::Block sampleBlock{columns_with_types_and_names};

    size_t rows = 20;
    srand(time(NULL));
    std::vector<uint64_t> counter_array;
    std::vector<std::string> host_name_array;

    int initial_val = rand() % 10000000;

    try {

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        // Count: UInt64
        {
            DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[0])).getData();
            DB::DataTypeUInt64 data_type;

            vec.resize(rows);
            for (size_t i = 0; i < rows; ++i) {
                // to produce random number instead, as otherwise, the blocks will be identical and no new block
                // insertion will be able to get retrieved.
                uint64_t val = (uint64_t)(initial_val + i);
                vec[i] = val;
                counter_array.push_back(val);
            }
        }

        // host: String
        {
            const char* s = "graphdb-360";
            std::string string_val(s);

            size_t size = strlen(s) + 1;
            DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[1])).getChars();
            DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[1])).getOffsets();

            data.resize(rows * size); // 1 entry
            offsets.resize(rows);     // 1 entry

            for (size_t i = 0; i < rows; ++i) {
                memcpy(&data[i * size], s, size);
                offsets[i] = (i + 1) * size;
                host_name_array.push_back(string_val);
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));

        size_t total_number_of_rows = sampleBlock.rows();
        LOG(INFO) << "total number of rows: " << total_number_of_rows;
        ASSERT_EQ(total_number_of_rows, rows);

        size_t total_number_of_columns = sampleBlock.columns();
        LOG(INFO) << "total number of columns: " << total_number_of_columns;
        ASSERT_EQ(total_number_of_columns, 2U);

        std::string names = sampleBlock.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        std::string structure = sampleBlock.dumpStructure();
        LOG(INFO) << "structure dumped: " << structure;

    } catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
    }

    bool result = loader.load_buffer(table_name, query, sampleBlock);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Host, \n" /*  0. String */
                                 " Count \n"       /*  1. UInt64 */
                                 "from " +
        table_being_queried +
        "\n"
        "order by (Host, Count)";

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
        ASSERT_EQ(number_of_columns, 2U);

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]); // Host
        auto& column_1 = assert_cast<DB::ColumnUInt64&>(*columns[1]); // Count

        for (size_t i = 0; i < rows; i++) {
            auto column_0_string = column_0.getDataAt(i); // We only have 1 row.
            std::string column_0_real_string(column_0_string.data, column_0_string.size);
            LOG(INFO) << "retrieved host name is: " << column_0_real_string;
            ASSERT_EQ(column_0_real_string, host_name_array[i]);

            // count retrieval
            uint64_t counter_val = column_1.getData()[i];
            LOG(INFO) << "retrieved Count: " << counter_val << " with expected value: " << counter_array[i];
            // ASSERT_EQ (counter_val, (uint64_t) counter_array[i]);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * The simple event is created via:
 *
 * CREATE TABLE simple_event (`Host` String, `Colo` String, `EventName` String, `Count` UInt64, `Duration` Float32,
 `TimeStamp` DateTime) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event', '{replica}') PARTITION BY
 intDiv(toUnixTimestamp(TimeStamp), 86400) ORDER BY (Host, Colo, EventName) SETTINGS index_granularity = 8192

   This test is to show that the client needs only populate with 2 columns: Count and Host, and the request can be
 accepted by the server, and populate with the system-provided default values,
 https://clickhouse.tech/docs/en/sql-reference/statements/create/table/#default

   "
   If an expression for the default value is not defined, the default values will be set to zeros for numbers, empty
 strings for strings, empty arrays for arrays, and 1970-01-01 for dates or zero unix timestamp for DateTime, NULL for
 Nullable.

   "

   With the following test, the inserted results are:

    ┌─Host────────┬─Colo─┬─EventName─┬─Count─┬─Duration─┬───────────TimeStamp─┐
    │ graphdb-360 │      │           │  4990 │        0 │ 0000-00-00 00:00:00 │
    │ graphdb-360 │      │           │  9980 │        0 │ 0000-00-00 00:00:00 │
    └─────────────┴──────┴───────────┴───────┴──────────┴─────────────────────┘
 */
TEST_F(AggregatorLoaderRelatedTest, InsertARowToCHServerWithInputBlocksHavingMissingColumnsComparedToServerSide) {

    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    std::string query = "insert into " + table_name + " ( `Count`, `Host`) VALUES (100, 'graphdb-18');";
    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeUInt64>(), "Count"},
                                                            {std::make_shared<DB::DataTypeString>(), "Host"}};

    DB::Block sampleBlock{columns_with_types_and_names};

    size_t rows = 2;
    srand(time(NULL));
    std::vector<uint64_t> counter_array;
    std::vector<std::string> host_name_array;
    int initial_val = rand() % 10000000;
    try {

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        // Count: UInt64
        {
            DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[0])).getData();
            DB::DataTypeUInt64 data_type;

            vec.resize(rows);
            for (size_t i = 0; i < rows; ++i) {
                // to produce random number instead, as otherwise, the blocks will be identical and no new block
                // insertion will be able to get retrieved.
                uint64_t val = (uint64_t)(initial_val + i);
                vec[i] = val;
                counter_array.push_back(val);
            }
        }

        // host: String
        {
            const char* s = "graphdb-360";
            std::string string_val(s);

            size_t size = strlen(s) + 1;
            DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[1])).getChars();
            DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[1])).getOffsets();

            data.resize(rows * size); // 1 entry
            offsets.resize(rows);     // 1 entry

            for (size_t i = 0; i < rows; ++i) {
                memcpy(&data[i * size], s, size);
                offsets[i] = (i + 1) * size;
                host_name_array.push_back(string_val);
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));

        size_t total_number_of_rows = sampleBlock.rows();
        LOG(INFO) << "total number of rows: " << total_number_of_rows;

        std::string names = sampleBlock.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        std::string structure = sampleBlock.dumpStructure();
        LOG(INFO) << "structure dumped: " << structure;

    } catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
    }

    bool result = loader.load_buffer(table_name, query, sampleBlock);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Host , \n"  /*  0. String */
                                 "     Colo,\n"      /* 1. String*/
                                 "     EventName,\n" /* 2. String */
                                 "     Count,\n"     /*  3. UInt64 */
                                 "     Duration,\n"  /*  4. Float32 */
                                 "     TimeStamp \n" /* 5. DateTime */
                                 "from " +
        table_being_queried +
        "\n"
        "order  by (Host, Colo, EventName)";

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
        ASSERT_EQ(number_of_columns, 6U);

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]);  // Host
        auto& column_1 = assert_cast<DB::ColumnString&>(*columns[1]);  // Colo
        auto& column_2 = assert_cast<DB::ColumnString&>(*columns[2]);  // EventName
        auto& column_3 = assert_cast<DB::ColumnUInt64&>(*columns[3]);  // Count
        auto& column_4 = assert_cast<DB::ColumnFloat32&>(*columns[4]); // Float32
        auto& column_5 = assert_cast<DB::ColumnUInt32&>(*columns[5]);  // DateTime

        for (size_t i = 0; i < rows; i++) {
            // Host value retrieval.
            auto column_0_string = column_0.getDataAt(i); // We only have 1 row.
            std::string column_0_real_string(column_0_string.data, column_0_string.size);
            LOG(INFO) << "retrieved host name is: " << column_0_real_string
                      << " with expected value: " << host_name_array[i];
            ASSERT_EQ(column_0_real_string, host_name_array[i]);

            // Colo value retrieval
            auto column_1_real_string = column_1.getDataAt(i);
            std::string column_1_real_string_value(column_1_real_string.data, column_1_real_string.size);
            LOG(INFO) << "retrieved fixed string colo name (should be empty): " << column_1_real_string_value;
            ASSERT_EQ(column_1_real_string_value, "");

            // EventName retrieval
            auto column_2_string = column_2.getDataAt(i); // We only have 1 row.
            std::string column_2_real_string(column_2_string.data, column_2_string.size);
            LOG(INFO) << "retrieved event name (should empty): " << column_2_real_string;
            ASSERT_EQ(column_2_real_string, "");

            // count retrieval
            uint64_t counter_val = column_3.getData()[i];
            LOG(INFO) << "retrieved Count: " << counter_val << " with expected value: " << counter_array[i];
            ASSERT_EQ(counter_val, counter_array[i]);

            // duration retrieval
            float duration_val = column_4.getData()[i];
            LOG(INFO) << "retrieved duration (should be 0): " << duration_val;
            ASSERT_EQ(duration_val, (float)0);

            // timestamp retrieval
            uint32_t timestamp_val = column_5.getData()[0];
            LOG(INFO) << "retrieved timestamp value (should be 0): " << timestamp_val;
            ASSERT_EQ(timestamp_val, 0U);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

/**
 * The default value will be populated by the backend. There is no need to perform population on the default value at
 the
 * INSERT of the blocks.
 *
             * SELECT *
            FROM simple_event_9

            ┌───────Time─┬─Host─────────┐
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            │ 2020-02-10 │ graphdb-5000 │
            └────────────┴──────────────┘
 *
 *
 *  NOTE: still what about mixing of a block that has default value in some rows and in some rows without default
 values.
 *
 */
TEST_F(AggregatorLoaderRelatedTest, InsertARowToCHServerWithDefaultValuesInSingleBlock) {

    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    // NOTE: for multiple test cases run in a single test application, we can not have multiple global context.
    DB::ContextMutablePtr context = AggregatorLoaderRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = AggregatorLoaderRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    std::string table_name = "simple_event_9";
    bool removed = removeTableContent(context, ioc, table_name);
    LOG(INFO) << "remove table content for table: " << table_name << " returned status: " << removed;
    ASSERT_TRUE(removed);

    nuclm::AggregatorLoaderManager manager(context, ioc);
    nuclm::AggregatorLoader loader(context, manager.getConnectionPool(), manager.getConnectionParameters());
    std::string query = "insert into " + table_name + " (`Time`, `Host`) VALUES ('2016-06-15 23:00:06', 'graphdb-1');";

    LOG(INFO) << "chosen table: " << table_name << "with insert query: " << query;
    loader.init();

    // NOTE: the following need to be changed, in order to support the server-side default expression of "today()"!
    DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"}};
    DB::Block sampleBlock{columns_with_types_and_names};

    srand(time(NULL));
    int initial_val = rand() % 10000000;
    std::vector<std::string> host_name_array;

    size_t rows = 20;
    std::string string_val = "graphdb-" + std::to_string(initial_val);
    try {

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        {
            // insert data: 'graphdb-5000", with the default value hopefully being populated by the server.
            const char* s = string_val.c_str();
            size_t size = strlen(s) + 1;
            DB::ColumnString::Chars& data = (assert_cast<DB::ColumnString&>(*columns[0])).getChars();
            DB::ColumnString::Offsets& offsets = (assert_cast<DB::ColumnString&>(*columns[0])).getOffsets();

            data.resize(rows * size); // 1 entry
            offsets.resize(rows);     // 1 entry

            for (size_t i = 0; i < rows; ++i) {
                memcpy(&data[i * size], s, size);
                offsets[i] = (i + 1) * size;

                host_name_array.push_back(string_val);
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));

        size_t total_number_of_rows = sampleBlock.rows();
        LOG(INFO) << "total number of rows: " << total_number_of_rows;

        std::string names = sampleBlock.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        std::string structure = sampleBlock.dumpStructure();
        LOG(INFO) << "structure dumped: " << structure;

    } catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
    }

    bool result = loader.load_buffer(table_name, query, sampleBlock);

    ASSERT_TRUE(result);

    std::string table_being_queried = "default." + table_name;
    std::string query_on_table = "select Host , \n" /*  0. String */
                                 "     Time \n"     /* 1. Date */
                                 "from " +
        table_being_queried +
        "\n"
        "order by (Host, Time)";

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
        ASSERT_EQ(number_of_columns, 2U);

        auto& column_0 = assert_cast<DB::ColumnString&>(*columns[0]); // Host
        auto& column_1 = assert_cast<DB::ColumnUInt16&>(*columns[1]); // Date

        for (size_t i = 0; i < rows; i++) {
            // Host value retrieval.
            auto column_0_string = column_0.getDataAt(i); // We only have 1 row.
            std::string column_0_real_string(column_0_string.data, column_0_string.size);
            LOG(INFO) << "retrieved host name is: " << column_0_real_string
                      << " with expected value: " << host_name_array[i];
            ASSERT_EQ(column_0_real_string, host_name_array[i]);

            // time retrieval
            uint16_t time_val = column_1.getData()[i];
            LOG(INFO) << "retrieved date value: " << time_val;

            // since we do not have the server-side default expression being turned on in this test case. It requires
            // the retrieval of the default expression based column schema.
            ASSERT_EQ(time_val, 0U);
        }
    };

    bool query_status = inspectRowBasedContent(table_being_queried, query_on_table, loader, row_based_inspector);
    ASSERT_TRUE(query_status);
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
