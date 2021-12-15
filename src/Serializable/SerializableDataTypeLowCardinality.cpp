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

#include <Serializable/SerializableDataTypeLowCardinality.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>

#include "common/logging.hpp"

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFactory.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
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
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
} // namespace ErrorCodes

namespace {
const DB::ColumnLowCardinality& getColumnLowCardinality(const DB::IColumn& column) {
    return typeid_cast<const DB::ColumnLowCardinality&>(column);
}

DB::ColumnLowCardinality& getColumnLowCardinality(DB::IColumn& column) {
    return typeid_cast<DB::ColumnLowCardinality&>(column);
}
} // namespace

SerializableDataTypeLowCardinality::SerializableDataTypeLowCardinality(
    const SerializableDataTypePtr& nested_serializable_data_type_,
    const DB::DataTypePtr& nested_true_dictionary_type_) :
        nested_serializable_data_type{nested_serializable_data_type_},
        nested_true_dictionary_type{nested_true_dictionary_type_} {
    auto inner_type = nested_true_dictionary_type;
    if (nested_true_dictionary_type->isNullable())
        inner_type = static_cast<const DB::DataTypeNullable&>(*nested_true_dictionary_type).getNestedType();

    if (!inner_type->canBeInsideLowCardinality())
        throw DB::Exception(
            "DataTypeLowCardinality is supported only for numbers, strings, Date or DateTime, but got " +
                nested_true_dictionary_type->getName(),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

void SerializableDataTypeLowCardinality::serializeProtobuf(const DB::IColumn& column, size_t row_num,
                                                           ProtobufWriter& protobuf, size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeLowCardinality::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf,
                                                             bool allow_add_row, bool& row_added) const {
    auto& low_cardinality_column = getColumnLowCardinality(column);
    auto temp_column = low_cardinality_column.getDictionary().getNestedColumn()->cloneEmpty();
    LOG_MESSAGE(4) << " LowCardinality value encountered, to go to nested column under lowcardinality-column with "
                   << "column name: " << temp_column->getName() << " family name: " << temp_column->getFamilyName();

    nested_serializable_data_type.get()->deserializeProtobuf(*temp_column, protobuf, allow_add_row, row_added);
    low_cardinality_column.insertFromFullColumn(*temp_column, 0);

    // the protobuf statics is performed in the nested type.
}

bool SerializableDataTypeLowCardinality::equals(const ISerializableDataType& rhs) const {
    if (typeid(rhs) != typeid(*this))
        return false;

    auto& low_cardinality_rhs = static_cast<const SerializableDataTypeLowCardinality&>(rhs);
    return nested_serializable_data_type->equals(*low_cardinality_rhs.nested_serializable_data_type);
}

static SerializableDataTypePtr create(const DB::ASTPtr& arguments) {
    if (!arguments || arguments->children.size() != 1)
        throw DB::Exception("LowCardinality data type family must have exactly one argument - type of elements",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    // only the nested serializable type
    SerializableDataTypePtr nested_serializable_type =
        SerializableDataTypeFactory::instance().get(arguments->children[0]);
    // the real data type
    DB::DataTypePtr nested_true_type = DB::DataTypeFactory::instance().get(arguments->children[0]);

    return std::make_shared<SerializableDataTypeLowCardinality>(nested_serializable_type, nested_true_type);
}

void registerDataTypeLowCardinality(SerializableDataTypeFactory& factory) {
    factory.registerDataType("LowCardinality", create);
}

} // namespace nuclm
