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

#include <Serializable/SerializableDataTypeNullable.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>

#include "common/logging.hpp"

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnNullable.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Parsers/IAST.h>
#include <Common/assert_cast.h>

#include <glog/logging.h>

namespace nuclm {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
} // namespace ErrorCodes

SerializableDataTypeNullable::SerializableDataTypeNullable(
    const SerializableDataTypePtr& nested_serializable_data_type_, const DB::DataTypePtr& nested_true_data_type_) :
        nested_serializable_data_type{nested_serializable_data_type_}, nested_true_data_type(nested_true_data_type_) {

    if (!nested_serializable_data_type->canBeInsideNullable())
        throw DB::Exception("Nested type " + nested_serializable_data_type->getName() +
                                " cannot be inside Nullable type",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// Deserialize value into ColumnNullable.
/// We need to insert both to nested column and to null byte map, or, in case of exception, to not insert at all.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested,
          typename std::enable_if_t<std::is_same_v<ReturnType, void>, ReturnType>* = nullptr>
static ReturnType safeDeserialize(DB::IColumn& column, const DB::IDataType& /*nested_data_type*/,
                                  CheckForNull&& check_for_null, DeserializeNested&& deserialize_nested) {
    DB::ColumnNullable& col = assert_cast<DB::ColumnNullable&>(column);

    if (check_for_null()) {
        col.insertDefault();
    } else {
        deserialize_nested(col.getNestedColumn());

        try {
            col.getNullMapData().push_back(0);
        } catch (...) {
            col.getNestedColumn().popBack(1);
            throw;
        }
    }
}

/// Deserialize value into non-nullable column. In case of NULL, insert default value and return false.
template <typename ReturnType = void, typename CheckForNull, typename DeserializeNested,
          typename std::enable_if_t<std::is_same_v<ReturnType, bool>, ReturnType>* = nullptr>
static ReturnType safeDeserialize(DB::IColumn& column, const DB::IDataType& nested_data_type,
                                  CheckForNull&& check_for_null, DeserializeNested&& deserialize_nested) {
    assert(!dynamic_cast<DB::ColumnNullable*>(&column));
    assert(!dynamic_cast<const SerializableDataTypeNullable*>(&nested_data_type));
    bool insert_default = check_for_null();
    if (insert_default)
        nested_data_type.insertDefaultInto(column);
    else
        deserialize_nested(column);
    return !insert_default;
}

void SerializableDataTypeNullable::serializeProtobuf(const DB::IColumn& column, size_t row_num,
                                                     ProtobufWriter& protobuf, size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeNullable::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf,
                                                       bool allow_add_row, bool& row_added) const {
    DB::ColumnNullable& col = assert_cast<DB::ColumnNullable&>(column);
    DB::IColumn& nested_column = col.getNestedColumn();

    bool insert_default = protobuf.checkNull();
    if (insert_default) {
        LOG_MESSAGE(4)
            << " null value encountered, to insert default value for nested column under nullable-column with "
            << "column name: " << nested_column.getName() << " family name: " << nested_column.getFamilyName();

        // by following ColumnNullable.cpp, insertData(), line 85, along with corresponding comment at ColumnNullable.h
        // on this method " Will insert null value if pos=nullptr "
        /*
         * void ColumnNullable::insertData(const char * pos, size_t length)
            {
                if (pos == nullptr)
                {
                    getNestedColumn().insertDefault();
                    getNullMapData().push_back(1);
                }
                else
                {
                    getNestedColumn().insertData(pos, length);
                    getNullMapData().push_back(0);
                }
            }
         */
        col.insertData(nullptr, 0);
        row_added = true; // we do added a raw value.

        // nested_true_data_type->insertDefaultInto(nested_column); //other option 1, tried.
        // nested_column.insertDefault(); //other option 2, tried.

        protobuf.addNullBytes(); // for metrics purpose.
    } else {
        size_t old_size = nested_column.size();
        try {
            nested_serializable_data_type->deserializeProtobuf(nested_column, protobuf, allow_add_row, row_added);
            if (row_added)
                col.getNullMapData().push_back(0);
        } catch (...) {
            nested_column.popBack(nested_column.size() - old_size);
            col.getNullMapData().resize_assume_reserved(old_size);
            row_added = false;
            throw;
        }
    }
}

bool SerializableDataTypeNullable::equals(const ISerializableDataType& rhs) const {
    return rhs.isNullable() &&
        nested_serializable_data_type->equals(
            *static_cast<const SerializableDataTypeNullable&>(rhs).nested_serializable_data_type);
}

static SerializableDataTypePtr create(const DB::ASTPtr& arguments) {
    if (!arguments || arguments->children.size() != 1)
        throw DB::Exception("Nullable data type family must have exactly one argument - nested type",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    // only the nested serializable type
    SerializableDataTypePtr nested_serializable_type =
        SerializableDataTypeFactory::instance().get(arguments->children[0]);
    // the real data type
    DB::DataTypePtr nested_true_type = DB::DataTypeFactory::instance().get(arguments->children[0]);

    return std::make_shared<SerializableDataTypeNullable>(nested_serializable_type, nested_true_type);
}

void registerDataTypeNullable(SerializableDataTypeFactory& factory) { factory.registerDataType("Nullable", create); }

SerializableDataTypePtr makeNullable(const SerializableDataTypePtr& type) {
    if (type->isNullable())
        return type;
    else {
        // To use type id to get the true data type.
        DB::TypeIndex typeIndex = type->getTypeId();
        DB::DataTypePtr nested_true_type = DB::DataTypeFactory::instance().get(DB::getTypeName(typeIndex));
        return std::make_shared<SerializableDataTypeNullable>(type, nested_true_type);
    }
}

SerializableDataTypePtr removeNullable(const SerializableDataTypePtr& type) {
    if (type->isNullable())
        return static_cast<const SerializableDataTypeNullable&>(*type).getNestedType();
    return type;
}

} // namespace nuclm
