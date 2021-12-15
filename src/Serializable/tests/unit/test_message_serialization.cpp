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

#include <Serializable/ISerializableDataType.h>
#include <Serializable/SerializableDataTypeFactory.h>

#include <nucolumnar/aggregator/v1/nucolumnaraggregator.pb.h>
#include <nucolumnar/datatypes/v1/columnartypes.pb.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

/**
 * testing of single row serializer and de-serializer.
 */
TEST(SerializableObjects, testOneRowSerializationDeserializationWithSingleRow) {
    std::string table = "t1";
    std::string sql = "insert into t1 values(?, ?, ?)";
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
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(123456);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nucolumnar::aggregator::v1::SQLBatchRequest deserializedSqlBatchRequest;
    deserializedSqlBatchRequest.ParseFromString(serializedSqlBatchRequestInString);

    LOG(INFO) << "shard id deserialized: " << deserializedSqlBatchRequest.shard();
    LOG(INFO) << "table name deserialized: " << deserializedSqlBatchRequest.table();

    LOG(INFO) << "has nucolumnar encoding or not: " << deserializedSqlBatchRequest.has_nucolumnarencoding();
    LOG(INFO) << "sql statement is: " << deserializedSqlBatchRequest.nucolumnarencoding().sql();

    {
        const nucolumnar::aggregator::v1::SqlWithBatchBindings& deserializedSqlWithBatchBindings =
            deserializedSqlBatchRequest.nucolumnarencoding();
        std::string sqlStatement = deserializedSqlWithBatchBindings.sql();
        int totalNumberOfRows = deserializedSqlWithBatchBindings.batch_bindings_size();
        LOG(INFO) << "total number of the rows involved are: " << totalNumberOfRows;

        // loop through multiple rows.
        for (int i = 0; i < totalNumberOfRows; i++) {
            const nucolumnar::aggregator::v1::DataBindingList& row = deserializedSqlWithBatchBindings.batch_bindings(i);
            LOG(INFO) << "row: " << i;
            int numberOfColumns = row.values().size();
            LOG(INFO) << " total number of columns in row: " << i << " is: " << numberOfColumns;
            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(0);
                LOG(INFO) << "column 0 has long value: " << rowValue.long_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(1);
                LOG(INFO) << "column 1 has string value: " << rowValue.string_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(2);
                LOG(INFO) << "column 2 has string value: " << rowValue.string_value();
            }
        }
    }
}

TEST(SerializableObjects, testOneRowSerializationDeserializationWithMultipleRow) {
    std::string table = "t1";
    std::string sql = "insert into t1 values(?, ?, ?)";
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;

    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(123456);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(8888);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc-8888");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz-8888");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(9999);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc-9999");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz-9999");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nucolumnar::aggregator::v1::SQLBatchRequest deserializedSqlBatchRequest;
    deserializedSqlBatchRequest.ParseFromString(serializedSqlBatchRequestInString);

    LOG(INFO) << "shard id deserialized: " << deserializedSqlBatchRequest.shard();
    LOG(INFO) << "table name deserialized: " << deserializedSqlBatchRequest.table();

    LOG(INFO) << "has nucolumnar encoding or not: " << deserializedSqlBatchRequest.has_nucolumnarencoding();
    LOG(INFO) << "sql statement is: " << deserializedSqlBatchRequest.nucolumnarencoding().sql();

    {
        const nucolumnar::aggregator::v1::SqlWithBatchBindings& deserializedSqlWithBatchBindings =
            deserializedSqlBatchRequest.nucolumnarencoding();
        std::string sqlStatement = deserializedSqlWithBatchBindings.sql();
        int totalNumberOfRows = deserializedSqlWithBatchBindings.batch_bindings_size();
        LOG(INFO) << "total number of the rows involved are: " << totalNumberOfRows;

        // loop through multiple rows.
        for (int i = 0; i < totalNumberOfRows; i++) {
            const nucolumnar::aggregator::v1::DataBindingList& row = deserializedSqlWithBatchBindings.batch_bindings(i);
            LOG(INFO) << "row: " << i;
            int numberOfColumns = row.values().size();
            LOG(INFO) << " total number of columns in row: " << i << " is: " << numberOfColumns;
            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(0);
                LOG(INFO) << "column 0 has long value: " << rowValue.long_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(1);
                LOG(INFO) << "column 1 has string value: " << rowValue.string_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(2);
                LOG(INFO) << "column 2 has string value: " << rowValue.string_value();
            }
        }
    }
}

TEST(SerializableObjects, testOneRowSerializationDeserializationWithMultipleRowWithTimeRelatedFields) {
    std::string table = "t1";
    std::string sql = "insert into t1 values(?, ?, ?, ?, ?)"; // the last two fields are with Date, and DateTime.
    std::string shard = "nudata.monstor.cdc.dev.marketing.1";

    nucolumnar::aggregator::v1::DataBindingList bindingList;

    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(123456);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz");

        // date
        nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
        val4->mutable_timestamp()->set_milliseconds(10);

        // date
        nucolumnar::datatypes::v1::ValueP* val5 = bindingList.add_values();
        val4->mutable_timestamp()->set_milliseconds(10);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
        pval4->CopyFrom(*val4);

        nucolumnar::datatypes::v1::ValueP* pval5 = dataBindingList->add_values();
        pval5->CopyFrom(*val5);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(8888);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc-8888");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz-8888");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(9999);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc-9999");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("xyz-9999");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nucolumnar::aggregator::v1::SQLBatchRequest deserializedSqlBatchRequest;
    deserializedSqlBatchRequest.ParseFromString(serializedSqlBatchRequestInString);

    LOG(INFO) << "shard id deserialized: " << deserializedSqlBatchRequest.shard();
    LOG(INFO) << "table name deserialized: " << deserializedSqlBatchRequest.table();

    LOG(INFO) << "has nucolumnar encoding or not: " << deserializedSqlBatchRequest.has_nucolumnarencoding();
    LOG(INFO) << "sql statement is: " << deserializedSqlBatchRequest.nucolumnarencoding().sql();

    {
        const nucolumnar::aggregator::v1::SqlWithBatchBindings& deserializedSqlWithBatchBindings =
            deserializedSqlBatchRequest.nucolumnarencoding();
        std::string sqlStatement = deserializedSqlWithBatchBindings.sql();
        int totalNumberOfRows = deserializedSqlWithBatchBindings.batch_bindings_size();
        LOG(INFO) << "total number of the rows involved are: " << totalNumberOfRows;

        // loop through multiple rows.
        for (int i = 0; i < totalNumberOfRows; i++) {
            const nucolumnar::aggregator::v1::DataBindingList& row = deserializedSqlWithBatchBindings.batch_bindings(i);
            LOG(INFO) << "row: " << i;
            int numberOfColumns = row.values().size();
            LOG(INFO) << " total number of columns in row: " << i << " is: " << numberOfColumns;
            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(0);
                LOG(INFO) << "column 0 has long value: " << rowValue.long_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(1);
                LOG(INFO) << "column 1 has string value: " << rowValue.string_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(2);
                LOG(INFO) << "column 2 has string value: " << rowValue.string_value();
            }
        }
    }
}

TEST(SerializableObjects, testOneRowSerializationDeserializationWithSingleArray) {
    std::string table = "t1";
    std::string sql = "insert into t1 values(?, ?, ?)";
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

    // add values
    {
        // value 1: long
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(123456);
        // value 2: string
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc");

        // value 3, list of long values
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val3->mutable_list_value();
        array_value->add_value()->set_long_value(123);
        array_value->add_value()->set_long_value(456);
        array_value->add_value()->set_long_value(789);

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nucolumnar::aggregator::v1::SQLBatchRequest deserializedSqlBatchRequest;
    deserializedSqlBatchRequest.ParseFromString(serializedSqlBatchRequestInString);

    LOG(INFO) << "shard id deserialized: " << deserializedSqlBatchRequest.shard();
    LOG(INFO) << "table name deserialized: " << deserializedSqlBatchRequest.table();

    LOG(INFO) << "has nucolumnar encoding or not: " << deserializedSqlBatchRequest.has_nucolumnarencoding();
    LOG(INFO) << "sql statement is: " << deserializedSqlBatchRequest.nucolumnarencoding().sql();

    {
        const nucolumnar::aggregator::v1::SqlWithBatchBindings& deserializedSqlWithBatchBindings =
            deserializedSqlBatchRequest.nucolumnarencoding();
        std::string sqlStatement = deserializedSqlWithBatchBindings.sql();
        int totalNumberOfRows = deserializedSqlWithBatchBindings.batch_bindings_size();
        LOG(INFO) << "total number of the rows involved are: " << totalNumberOfRows;

        // loop through multiple rows.
        for (int i = 0; i < totalNumberOfRows; i++) {
            const nucolumnar::aggregator::v1::DataBindingList& row = deserializedSqlWithBatchBindings.batch_bindings(i);
            LOG(INFO) << "row: " << i;
            int numberOfColumns = row.values().size();
            LOG(INFO) << " total number of columns in row: " << i << " is: " << numberOfColumns;
            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(0);
                LOG(INFO) << "column 0 has long value: " << rowValue.long_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(1);
                LOG(INFO) << "column 1 has string value: " << rowValue.string_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(2);
                const nucolumnar::datatypes::v1::ListValueP& list = rowValue.list_value();

                LOG(INFO) << "column 2 has array value detailed below, with size:  " << list.value_size();
                for (const nucolumnar::datatypes::v1::ValueP& elem : list.value()) {
                    // we need to know before-hand the type of the values in the array
                    LOG(INFO) << "array value (long) is: " << elem.long_value();
                }
            }
        }
    }
}

TEST(SerializableObjects, testOneRowSerializationDeserializationWithNextedArrayWithDepthTwo) {
    std::string table = "t1";
    std::string sql = "insert into t1 values(?, ?, ?)";
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

    // add values
    {
        // value 1: long
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(123456);
        // value 2: string
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("abc");

        // value 3, list of long values
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        nucolumnar::datatypes::v1::ListValueP* array_value = val3->mutable_list_value();
        {
            // each array value is also an array, with two elements
            nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                array_value->add_value()->mutable_list_value();
            array_value_nested_array->add_value()->set_long_value(123);
            array_value_nested_array->add_value()->set_long_value(456);
        }

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        // NOTE: for nested array, do we need to have nested CopyFrom also?
        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();

    nucolumnar::aggregator::v1::SQLBatchRequest deserializedSqlBatchRequest;
    deserializedSqlBatchRequest.ParseFromString(serializedSqlBatchRequestInString);

    LOG(INFO) << "shard id deserialized: " << deserializedSqlBatchRequest.shard();
    LOG(INFO) << "table name deserialized: " << deserializedSqlBatchRequest.table();

    LOG(INFO) << "has nucolumnar encoding or not: " << deserializedSqlBatchRequest.has_nucolumnarencoding();
    LOG(INFO) << "sql statement is: " << deserializedSqlBatchRequest.nucolumnarencoding().sql();

    {
        const nucolumnar::aggregator::v1::SqlWithBatchBindings& deserializedSqlWithBatchBindings =
            deserializedSqlBatchRequest.nucolumnarencoding();
        std::string sqlStatement = deserializedSqlWithBatchBindings.sql();
        int totalNumberOfRows = deserializedSqlWithBatchBindings.batch_bindings_size();
        LOG(INFO) << "total number of the rows involved are: " << totalNumberOfRows;

        // loop through multiple rows.
        for (int i = 0; i < totalNumberOfRows; i++) {
            const nucolumnar::aggregator::v1::DataBindingList& row = deserializedSqlWithBatchBindings.batch_bindings(i);
            LOG(INFO) << "row: " << i;
            int numberOfColumns = row.values().size();
            LOG(INFO) << " total number of columns in row: " << i << " is: " << numberOfColumns;
            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(0);
                LOG(INFO) << "column 0 has long value: " << rowValue.long_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(1);
                LOG(INFO) << "column 1 has string value: " << rowValue.string_value();
            }

            {
                const nucolumnar::datatypes::v1::ValueP& rowValue = row.values(2);
                const nucolumnar::datatypes::v1::ListValueP& list = rowValue.list_value();

                LOG(INFO) << "column 2 has array value detailed below, with size:  " << list.value_size();
                for (const nucolumnar::datatypes::v1::ValueP& elem : list.value()) {
                    // here it is a nested array
                    const nucolumnar::datatypes::v1::ListValueP& nested_list = elem.list_value();
                    LOG(INFO) << "nested array value is an array with size of: " << nested_list.value_size();
                    for (const nucolumnar::datatypes::v1::ValueP& nested_elem : nested_list.value()) {
                        // we need to know before-hand the type of the values in the array
                        LOG(INFO) << "nested array value (long) is: " << nested_elem.long_value();
                    }
                }
            }
        }
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
