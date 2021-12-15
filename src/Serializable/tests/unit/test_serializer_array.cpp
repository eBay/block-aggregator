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

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>
#include <Aggregator/TableColumnsDescription.h>
#include <Aggregator/AggregatorLoaderManager.h>

#include <Serializable/ISerializableDataType.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/SerializableDataTypeArray.h>
#include <Serializable/SerializableDataTypeNullable.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>

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

class SerializerForProtobufWithArrayRelatedTest : public ::testing::Test {
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

ContextWrapper* SerializerForProtobufWithArrayRelatedTest::shared_context = nullptr;

/**
 * test out Array (Array (UInt32)) on how nested data type gets parsed
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testArrayNestedDataTypeWithUInt32) {
    bool failed = false;
    try {
        auto& true_data_type_factory = DB::DataTypeFactory::instance();
        auto trueTypeFromString = [&true_data_type_factory](const std::string& str) {
            return true_data_type_factory.get(str);
        };

        DB::DataTypePtr trueDataTypePtr = trueTypeFromString("Array(Array(UInt32))");

        LOG(INFO) << "constructed true array type object has name: " << trueDataTypePtr->getName();
        LOG(INFO) << "constructed true array object has family name: " << trueDataTypePtr->getFamilyName();

        // Cast it to DataArrayType.
        DB::TypeIndex idx = trueDataTypePtr->getTypeId();
        ASSERT_TRUE(idx == DB::TypeIndex::Array);

        // Further retrieve the nested type
        auto nested_type = typeid_cast<const DB::DataTypeArray*>(trueDataTypePtr.get())->getNestedType();

        DB::TypeIndex nested_idx = nested_type->getTypeId();
        ASSERT_TRUE(nested_idx == DB::TypeIndex::Array);

        // Further retrieve the final primitive type
        auto nested_type_2 = typeid_cast<const DB::DataTypeArray*>(nested_type.get())->getNestedType();

        DB::TypeIndex nested_type_2_index = nested_type_2->getTypeId();
        ASSERT_TRUE(nested_type_2_index == DB::TypeIndex::UInt32);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * test out Array (Array (FixedString)) on how nested data type gets parsed
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testArrayNestedDataTypeWithFixedString) {
    bool failed = false;
    try {
        auto& true_data_type_factory = DB::DataTypeFactory::instance();
        auto trueTypeFromString = [&true_data_type_factory](const std::string& str) {
            return true_data_type_factory.get(str);
        };

        DB::DataTypePtr trueDataTypePtr = trueTypeFromString("Array(Array(FixedString (4)))");

        LOG(INFO) << "constructed true array type object has name: " << trueDataTypePtr->getName();
        LOG(INFO) << "constructed true array object has family name: " << trueDataTypePtr->getFamilyName();

        // Cast it to DataArrayType.
        DB::TypeIndex idx = trueDataTypePtr->getTypeId();
        ASSERT_TRUE(idx == DB::TypeIndex::Array);

        // Further retrieve the nested type
        auto nested_type = typeid_cast<const DB::DataTypeArray*>(trueDataTypePtr.get())->getNestedType();

        DB::TypeIndex nested_idx = nested_type->getTypeId();
        ASSERT_TRUE(nested_idx == DB::TypeIndex::Array);

        // Further retrieve the final primitive type
        auto nested_type_2 = typeid_cast<const DB::DataTypeArray*>(nested_type.get())->getNestedType();

        DB::TypeIndex nested_type_2_index = nested_type_2->getTypeId();
        ASSERT_TRUE(nested_type_2_index == DB::TypeIndex::FixedString);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * test out Array (Array (Nullable(String))) on how nested data type gets parsed
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testArrayNestedDataTypeWithNullableString) {
    bool failed = false;
    try {
        auto& true_data_type_factory = DB::DataTypeFactory::instance();
        auto trueTypeFromString = [&true_data_type_factory](const std::string& str) {
            return true_data_type_factory.get(str);
        };

        DB::DataTypePtr trueDataTypePtr = trueTypeFromString("Array(Array(Nullable(String)))");

        LOG(INFO) << "constructed true array type object has name: " << trueDataTypePtr->getName();
        LOG(INFO) << "constructed true array object has family name: " << trueDataTypePtr->getFamilyName();

        // Cast it to DataArrayType.
        DB::TypeIndex idx = trueDataTypePtr->getTypeId();
        ASSERT_TRUE(idx == DB::TypeIndex::Array);

        // Further retrieve the nested type
        auto nested_type = typeid_cast<const DB::DataTypeArray*>(trueDataTypePtr.get())->getNestedType();

        DB::TypeIndex nested_idx = nested_type->getTypeId();
        ASSERT_TRUE(nested_idx == DB::TypeIndex::Array);

        // Further retrieve the final primitive type
        auto nested_type_2 = typeid_cast<const DB::DataTypeArray*>(nested_type.get())->getNestedType();

        DB::TypeIndex nested_type_2_index = nested_type_2->getTypeId();
        ASSERT_TRUE(nested_type_2_index == DB::TypeIndex::Nullable);

        auto final_type_in_nullable = typeid_cast<const DB::DataTypeNullable*>(nested_type_2.get())->getNestedType();

        DB::TypeIndex final_type_in_nullable_index = final_type_in_nullable->getTypeId();
        ASSERT_TRUE(final_type_in_nullable_index == DB::TypeIndex::String);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * test out Array (Array (UInt32)) on how nested serializable data type gets parsed
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testSerializableArrayNestedDataTypeWithUInt32) {
    bool failed = false;
    try {
        auto& serializable_data_type_factory = nuclm::SerializableDataTypeFactory::instance();
        auto serializableTypeFromString = [&serializable_data_type_factory](const std::string& str) {
            return serializable_data_type_factory.get(str);
        };

        nuclm::SerializableDataTypePtr serializableDataTypePtr = serializableTypeFromString("Array(Array(UInt32))");

        LOG(INFO) << "constructed serializable array type object has name: " << serializableDataTypePtr->getName();
        LOG(INFO) << "constructed serializable array object has family name: "
                  << serializableDataTypePtr->getFamilyName();

        // Cast it to DataArrayType.
        DB::TypeIndex idx = serializableDataTypePtr->getTypeId();
        ASSERT_TRUE(idx == DB::TypeIndex::Array);

        // Further retrieve the nested type
        auto nested_type =
            typeid_cast<const nuclm::SerializableDataTypeArray*>(serializableDataTypePtr.get())->getNestedType();

        DB::TypeIndex nested_idx = nested_type->getTypeId();
        ASSERT_TRUE(nested_idx == DB::TypeIndex::Array);

        // Further retrieve the final primitive type
        auto nested_type_2 = typeid_cast<const nuclm::SerializableDataTypeArray*>(nested_type.get())->getNestedType();

        DB::TypeIndex nested_type_2_index = nested_type_2->getTypeId();
        ASSERT_TRUE(nested_type_2_index == DB::TypeIndex::UInt32);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * test out Array (Array (FixedString)) on how nested serializable data type gets parsed
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testSerializableArrayNestedDataTypeWithFixedString) {
    bool failed = false;
    try {
        auto& serializable_data_type_factory = nuclm::SerializableDataTypeFactory::instance();
        auto serializableTypeFromString = [&serializable_data_type_factory](const std::string& str) {
            return serializable_data_type_factory.get(str);
        };

        nuclm::SerializableDataTypePtr serializableDataTypePtr =
            serializableTypeFromString("Array(Array(FixedString (4)))");

        LOG(INFO) << "constructed serializable array type object has name: " << serializableDataTypePtr->getName();
        LOG(INFO) << "constructed serializable array object has family name: "
                  << serializableDataTypePtr->getFamilyName();

        // Cast it to DataArrayType.
        DB::TypeIndex idx = serializableDataTypePtr->getTypeId();
        ASSERT_TRUE(idx == DB::TypeIndex::Array);

        // Further retrieve the nested type
        auto nested_type =
            typeid_cast<const nuclm::SerializableDataTypeArray*>(serializableDataTypePtr.get())->getNestedType();

        DB::TypeIndex nested_idx = nested_type->getTypeId();
        ASSERT_TRUE(nested_idx == DB::TypeIndex::Array);

        // Further retrieve the final primitive type
        auto nested_type_2 = typeid_cast<const nuclm::SerializableDataTypeArray*>(nested_type.get())->getNestedType();

        DB::TypeIndex nested_type_2_index = nested_type_2->getTypeId();
        ASSERT_TRUE(nested_type_2_index == DB::TypeIndex::FixedString);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * test out Array (Array (Nullable(String))) on how nested serializable data type gets parsed
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testSerializableArrayNestedDataTypeWithNullableString) {
    bool failed = false;
    try {
        auto& serializable_data_type_factory = nuclm::SerializableDataTypeFactory::instance();
        auto serializableTypeFromString = [&serializable_data_type_factory](const std::string& str) {
            return serializable_data_type_factory.get(str);
        };

        nuclm::SerializableDataTypePtr serializableDataTypePtr =
            serializableTypeFromString("Array(Array(Nullable (String)))");

        LOG(INFO) << "constructed serializable array type object has name: " << serializableDataTypePtr->getName();
        LOG(INFO) << "constructed serializable array object has family name: "
                  << serializableDataTypePtr->getFamilyName();

        // Cast it to DataArrayType.
        DB::TypeIndex idx = serializableDataTypePtr->getTypeId();
        ASSERT_TRUE(idx == DB::TypeIndex::Array);

        // Further retrieve the nested type
        auto nested_type =
            typeid_cast<const nuclm::SerializableDataTypeArray*>(serializableDataTypePtr.get())->getNestedType();

        DB::TypeIndex nested_idx = nested_type->getTypeId();
        ASSERT_TRUE(nested_idx == DB::TypeIndex::Array);

        // Further retrieve the final primitive type
        auto nested_type_2 = typeid_cast<const nuclm::SerializableDataTypeArray*>(nested_type.get())->getNestedType();

        DB::TypeIndex nested_type_2_index = nested_type_2->getTypeId();
        ASSERT_TRUE(nested_type_2_index == DB::TypeIndex::Nullable);

        auto final_type_in_nullable =
            typeid_cast<const nuclm::SerializableDataTypeNullable*>(nested_type_2.get())->getNestedType();

        DB::TypeIndex final_type_in_nullable_index = final_type_in_nullable->getTypeId();
        ASSERT_TRUE(final_type_in_nullable_index == DB::TypeIndex::String);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * One row, with Array(UInt32)
 *
 * create table simple_event_62 (
     Counters Array(UInt32),
     Host String,
     Colo String)

   ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_62', '{replica}') ORDER BY(Host) SETTINGS
 index_granularity=8192;

   insert into simple_event_62 (`Counters`, `Host`, `Colo`) VALUES ([123, 456, 789], 'graphdb-1', 'LVS');
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testSerializationSingleArrayRow) {

    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = SerializerForProtobufWithArrayRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = SerializerForProtobufWithArrayRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table = "simple_event_62";
    std::string sql = "insert into simple_event_62 values(?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
    {
        // array value 1, list of long values
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        array_value->add_value()->set_uint_value(123);
        array_value->add_value()->set_uint_value(456);
        array_value->add_value()->set_uint_value(789);

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc12345");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz12345");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(UInt32)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));

    bool failed = false;
    try {
        nuclm::ColumnSerializers serializers =
            nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
        size_t total_number_of_columns = serializers.size();
        for (size_t i = 0; i < total_number_of_columns; i++) {
            std::string family_name = serializers[i]->getFamilyName();
            std::string name = serializers[i]->getName();

            LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
            LOG(INFO) << "name identified for Column: " << i << " is: " << name;
        }

        DB::Block block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
        std::string names = block_holder.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(table, table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        batchReader.read();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names;

        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder: " << structure;
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        failed = true;
    }

    ASSERT_FALSE(failed);
}

/**
 * One row, with Array(Array(UInt32))
 *
 * create table simple_event_63 (
     Counters Array(Array(UInt32)),
     Host String,
     Colo String)

   ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/simple_event_63', '{replica}') ORDER BY(Host) SETTINGS
 index_granularity=8192;

   insert into simple_event_63 (`Counters`, `Host`, `Colo`) VALUES ([[123, 456, 789], [1011, 1213]], 'graphdb-1',
 'LVS');
 */
TEST_F(SerializerForProtobufWithArrayRelatedTest, testSerializationSingleNestedArrayRow) {

    std::string path = getConfigFilePath("example_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    DB::ContextMutablePtr context = SerializerForProtobufWithArrayRelatedTest::shared_context->getContext();
    boost::asio::io_context& ioc = SerializerForProtobufWithArrayRelatedTest::shared_context->getIOContext();
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    nuclm::AggregatorLoaderManager manager(context, ioc);

    std::string table = "simple_event_63";
    std::string sql = "insert into simple_event_63 values(?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
    {
        // value 3, list of long values
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val1->mutable_list_value();
        {
            // each array value is also an array, with three elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_value_nested_array->add_value()->set_uint_value(123);
            array_value_nested_array->add_value()->set_uint_value(456);
            array_value_nested_array->add_value()->set_uint_value(789);
        }
        {
            // each array value is also an array, with two elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_value_nested_array->add_value()->set_uint_value(1011);
            array_value_nested_array->add_value()->set_uint_value(1213);
        }

        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc12345");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz12345");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nuclm::TableColumnsDescription table_definition(table);
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counters", "Array(Array(UInt32))"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));

    bool failed = false;
    try {
        nuclm::ColumnSerializers serializers =
            nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
        size_t total_number_of_columns = serializers.size();
        for (size_t i = 0; i < total_number_of_columns; i++) {
            std::string family_name = serializers[i]->getFamilyName();
            std::string name = serializers[i]->getName();

            LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
            LOG(INFO) << "name identified for Column: " << i << " is: " << name;
        }

        DB::Block block_holder =
            nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
        std::string names = block_holder.dumpNames();
        LOG(INFO) << "column names dumped : " << names;

        nuclm::TableSchemaUpdateTrackerPtr schema_tracker_ptr =
            std::make_shared<nuclm::TableSchemaUpdateTracker>(table, table_definition, manager);
        nuclm::ProtobufBatchReader batchReader(serializedSqlBatchRequestInString, schema_tracker_ptr, block_holder,
                                               context);
        batchReader.read();

        size_t total_number_of_rows_holder = block_holder.rows();
        LOG(INFO) << "total number of rows in block holder: " << total_number_of_rows_holder;
        std::string names_holder = block_holder.dumpNames();
        LOG(INFO) << "column names dumped in block holder : " << names;

        std::string structure = block_holder.dumpStructure();
        LOG(INFO) << "structure dumped in block holder: " << structure;
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
