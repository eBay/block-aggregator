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
#include <Common/Stopwatch.h>

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
#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

#include <string.h>

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

/**
 * NOTE: the method of "insert" used for column insertion is wrong for date type. So this test is just to show that
 * the way to invoke the column insertion is incorrect.
 */
/*
TEST (AggregatorLoaderBlockRelatedTest, testBlockConstructionWithExplicitTableSchema1) {
    try
    {
        using namespace DB;

        ColumnsWithTypeAndName columns_with_types_and_names {
                {std::make_shared<DataTypeString>(), "Host"},
                {std::make_shared<DataTypeString>(), "Colo"},
                {std::make_shared<DataTypeString>(), "EventName"},
                {std::make_shared<DataTypeUInt64>(), "Count"},
                {std::make_shared<DataTypeFloat32>(), "Duration"},
                {std::make_shared<DataTypeDateTime>(), "TimeStamp"},
        };
        Block sampleBlock {columns_with_types_and_names};

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        //insert data: 'graphdb-1', 'LVS', 'RESTART', 1,  1000,  '2016-06-15 23:00:06`

        //for String
        {
            columns[0]->insert("graphdb-31");
            columns[1]->insert("LVS");
            columns[2]->insert("RESTARTED");
            columns[3]->insert(1);
            columns[4]->insert(0.20);
            columns[5]->insert("2019-12-15 23:00:06"); //will this go to the automatic conversion?
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

}
*/

TEST(AggregatorLoaderBlockRelatedTest, testBlockConstructionWithExplicitTableSchema2) {
    try {

        DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"},
                                                                {std::make_shared<DB::DataTypeUInt64>(), "Count"}};

        DB::Block sampleBlock{columns_with_types_and_names};

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        size_t n = 2;
        {
            // insert data: 'graphdb-1', 'LVS', 'RESTART', 1,  1000,  '2016-06-15 23:00:06`
            const char* s = "graphdb-20";
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

        {
            DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[1])).getData();
            DB::DataTypeUInt64 data_type;

            vec.resize(n);
            for (size_t i = 0; i < n; ++i) {
                vec[i] = (unsigned long)(1990 * i);
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));

        size_t total_number_of_rows = sampleBlock.rows();
        LOG(INFO) << "total number of rows: " << total_number_of_rows;
        ASSERT_EQ(total_number_of_rows, n);

        std::string names = sampleBlock.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        std::string structure = sampleBlock.dumpStructure();
        LOG(INFO) << "structure dumped: " << structure;

        size_t total_number_of_columns = sampleBlock.columns();
        ASSERT_EQ(total_number_of_columns, 2U);

    }

    catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
    }
}

/**
 *
 To start with the block to be swapped in, that is just an empty block with any column header definitions.

 I0626 15:33:11.736654 11512 test_blocks.cpp:209] total number of rows: 2
 I0626 15:33:11.736666 11512 test_blocks.cpp:212] column names dumped : Host, Count
 I0626 15:33:11.736685 11512 test_blocks.cpp:215] structure dumped: Host String String(size = 2), Count UInt64
 UInt64(size = 2) I0626 15:33:11.736696 11512 test_blocks.cpp:225] after block swap, total number of rows at the old
 block: 0 I0626 15:33:11.736706 11512 test_blocks.cpp:228] after block swap, column names dumped at the old block :
 I0626 15:33:11.736716 11512 test_blocks.cpp:231] after block swap, structure dumped at the old block:
 I0626 15:33:11.736726 11512 test_blocks.cpp:237] after block swap, total number of rows at the new block: 2
 I0626 15:33:11.736735 11512 test_blocks.cpp:240] after block swap, column names dumped at the new block : Host, Count
 I0626 15:33:11.736745 11512 test_blocks.cpp:243] after block swap, structure dumped at the new block: Host String
 String(size = 2), Count UInt64 UInt64(size = 2)
 */
TEST(AggregatorLoaderBlockRelatedTest, testBlockSwapFunctionWithBlockWithoutHeader) {
    try {

        DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"},
                                                                {std::make_shared<DB::DataTypeUInt64>(), "Count"}};

        DB::Block sampleBlock{columns_with_types_and_names};

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        size_t n = 2;
        {
            // insert data: 'graphdb-1', 'LVS', 'RESTART', 1,  1000,  '2016-06-15 23:00:06`
            const char* s = "graphdb-70";
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

        {
            DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[1])).getData();
            DB::DataTypeUInt64 data_type;

            vec.resize(n);
            for (size_t i = 0; i < n; ++i) {
                vec[i] = (unsigned long)(1200 * i);
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));
        {
            size_t total_number_of_rows = sampleBlock.rows();
            LOG(INFO) << "total number of rows: " << total_number_of_rows;
            ASSERT_EQ(total_number_of_rows, n);

            std::string names = sampleBlock.dumpNames();
            LOG(INFO) << "column names dumped : " << names;

            std::string structure = sampleBlock.dumpStructure();
            LOG(INFO) << "structure dumped: " << structure;

            size_t total_number_of_columns = sampleBlock.columns();
            ASSERT_EQ(total_number_of_columns, 2U);
        }

        // now do the swap, but without having the new block to construct with the header.
        DB::Block newBlock;
        newBlock.swap(sampleBlock);

        {
            // after the swap
            size_t total_number_of_rows = sampleBlock.rows();
            LOG(INFO) << "after block swap, total number of rows at the old block: " << total_number_of_rows;
            ASSERT_EQ(total_number_of_rows, 0);

            std::string names = sampleBlock.dumpNames();
            LOG(INFO) << "after block swap, column names dumped at the new block : " << names;

            std::string structure = sampleBlock.dumpStructure();
            LOG(INFO) << "after block swap, structure dumped at the old block: " << structure;

            size_t total_number_of_columns = sampleBlock.columns();
            ASSERT_EQ(total_number_of_columns, 0U); // The inner columns do not have header to be associated.
        }

        {
            // after the swap
            size_t total_number_of_rows = newBlock.rows();
            LOG(INFO) << "after block swap, total number of rows at the new block: " << total_number_of_rows;
            ASSERT_EQ(total_number_of_rows, n);

            std::string names = newBlock.dumpNames();
            LOG(INFO) << "after block swap, column names dumped at the new block : " << names;

            std::string structure = newBlock.dumpStructure();
            LOG(INFO) << "after block swap, structure dumped at the new block: " << structure;

            size_t total_number_of_columns = newBlock.columns();
            ASSERT_EQ(total_number_of_columns, 2U); // the columns are still the same.
        }

    }

    catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
    }
}

/**
 * The block to be moved to has the same header definition. The following are the test run logs:
 * I0626 15:45:18.335369 12102 test_blocks.cpp:308] total number of rows: 2
   I0626 15:45:18.335389 12102 test_blocks.cpp:311] column names dumped : Host, Count
   I0626 15:45:18.335409 12102 test_blocks.cpp:314] structure dumped: Host String String(size = 2), Count UInt64
 UInt64(size = 2) I0626 15:45:18.335427 12102 test_blocks.cpp:323] after block swap, total number of rows at the old
 block: 0 I0626 15:45:18.335444 12102 test_blocks.cpp:326] after block swap, column names dumped at the old block :
 Host, Count I0626 15:45:18.335464 12102 test_blocks.cpp:329] after block swap, structure dumped at the old block: Host
 String String(size = 0), Count UInt64 UInt64(size = 0) I0626 15:45:18.335481 12102 test_blocks.cpp:335] after block
 swap, total number of rows at the new block: 2 I0626 15:45:18.335498 12102 test_blocks.cpp:338] after block swap,
 column names dumped at the new block : Host, Count I0626 15:45:18.335518 12102 test_blocks.cpp:341] after block swap,
 structure dumped at the new block: Host String String(size = 2), Count UInt64 UInt64(size = 2)
 */
TEST(AggregatorLoaderBlockRelatedTest, testBlockSwapFunctionWithBlockInitializedWithSameHeader) {
    try {

        DB::ColumnsWithTypeAndName columns_with_types_and_names{{std::make_shared<DB::DataTypeString>(), "Host"},
                                                                {std::make_shared<DB::DataTypeUInt64>(), "Count"}};

        DB::Block sampleBlock{columns_with_types_and_names};

        DB::MutableColumns columns = sampleBlock.cloneEmptyColumns();

        size_t n = 2;
        {
            // insert data: 'graphdb-1', 'LVS', 'RESTART', 1,  1000,  '2016-06-15 23:00:06`
            const char* s = "graphdb-70";
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

        {
            DB::ColumnUInt64::Container& vec = (assert_cast<DB::ColumnUInt64&>(*columns[1])).getData();
            DB::DataTypeUInt64 data_type;

            vec.resize(n);
            for (size_t i = 0; i < n; ++i) {
                vec[i] = (unsigned long)(1200 * i);
            }
        }

        /// construct the block.
        sampleBlock.setColumns(std::move(columns));
        {
            size_t total_number_of_rows = sampleBlock.rows();
            LOG(INFO) << "total number of rows: " << total_number_of_rows;
            ASSERT_EQ(total_number_of_rows, n);

            std::string names = sampleBlock.dumpNames();
            LOG(INFO) << "column names dumped : " << names;

            std::string structure = sampleBlock.dumpStructure();
            LOG(INFO) << "structure dumped: " << structure;

            size_t total_number_of_columns = sampleBlock.columns();
            ASSERT_EQ(total_number_of_columns, 2U);
        }

        // now do the swap, but with new block having the header.
        DB::Block newBlock{columns_with_types_and_names};
        newBlock.swap(sampleBlock);
        {
            // after the swap
            size_t total_number_of_rows = sampleBlock.rows();
            LOG(INFO) << "after block swap, total number of rows at the old block: " << total_number_of_rows;
            ASSERT_EQ(total_number_of_rows, 0);

            std::string names = sampleBlock.dumpNames();
            LOG(INFO) << "after block swap, column names dumped at the old block : " << names;

            std::string structure = sampleBlock.dumpStructure();
            LOG(INFO) << "after block swap, structure dumped at the old block: " << structure;

            // The block structure has rows size = 0, but columns still there.
            size_t total_number_of_columns = sampleBlock.columns();
            ASSERT_EQ(total_number_of_columns, 2U);
        }

        {
            // after the swap
            size_t total_number_of_rows = newBlock.rows();
            LOG(INFO) << "after block swap, total number of rows at the new block: " << total_number_of_rows;
            ASSERT_EQ(total_number_of_rows, n);

            std::string names = newBlock.dumpNames();
            LOG(INFO) << "after block swap, column names dumped at the new block : " << names;

            std::string structure = newBlock.dumpStructure();
            LOG(INFO) << "after block swap, structure dumped at the new block: " << structure;

            size_t total_number_of_columns = newBlock.columns();
            ASSERT_EQ(total_number_of_columns, 2U);
        }

    }

    catch (const DB::Exception& e) {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return;
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
