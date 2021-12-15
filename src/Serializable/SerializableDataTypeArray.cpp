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

#include <Serializable/SerializableDataTypeArray.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>
#include "common/logging.hpp"

#include <Columns/ColumnArray.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>

#include <glog/logging.h>

namespace nuclm {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
extern const int CANNOT_READ_ARRAY_FROM_PROTOBUF;

} // namespace ErrorCodes

SerializableDataTypeArray::SerializableDataTypeArray(const SerializableDataTypePtr& nested_serializable_data_type_) :
        nested_serializable_data_type(nested_serializable_data_type_) {}

void SerializableDataTypeArray::serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                                                  size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeArray::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                                                    bool& row_added) const {
    row_added = false;

    const nucolumnar::datatypes::v1::ValueP& value_p = protobuf.getValueP();
    nucolumnar::datatypes::v1::ValueP::KindCase kind = value_p.kind_case();
    LOG_MESSAGE(4) << "in protobuf deserializer: kind-case is: " << kind;

    DB::ColumnArray& column_array = assert_cast<DB::ColumnArray&>(column);
    DB::IColumn& nested_column = column_array.getData();
    DB::ColumnArray::Offsets& offsets = column_array.getOffsets();

    // To capture the nested array's data size. Note the total number of the bytes may not be accurate
    // as we do not know the exact layout in the protobuf format, for example, the size of the array.
    size_t total_deserialized_size = 0;
    if (kind == nucolumnar::datatypes::v1::ValueP::KindCase::kListValue) {
        // recursive array continues
        const nucolumnar::datatypes::v1::ListValueP& list = value_p.list_value();
        size_t list_size = list.value_size();

        // This may not be accurate as it may be variable-length encoding
        total_deserialized_size += list_size;

        size_t i = 0;
        try {
            for (; i < list_size; ++i) {
                const nucolumnar::datatypes::v1::ValueP& element = list.value(i);
                ProtobufReader nested_reader(element);
                nested_serializable_data_type->deserializeProtobuf(nested_column, nested_reader, allow_add_row,
                                                                   row_added);
                total_deserialized_size += nested_reader.getTotalBytesRead();
            }
        } catch (...) {
            if (i) {
                nested_column.popBack(i);
            }
            throw;
        }

        offsets.push_back(offsets.back() + list_size);
        protobuf.setTotalBytesRead(total_deserialized_size);

        row_added = true;
    } else {
        std::string exception_message =
            std::string(getFamilyName()) + " protobuf-based serialization expect to read array values";
        throw DB::Exception(exception_message, ErrorCodes::CANNOT_READ_ARRAY_FROM_PROTOBUF);
    }
}

bool SerializableDataTypeArray::equals(const ISerializableDataType& rhs) const {
    return typeid(rhs) == typeid(*this) &&
        nested_serializable_data_type->equals(
            *static_cast<const SerializableDataTypeArray&>(rhs).nested_serializable_data_type);
}

size_t SerializableDataTypeArray::getNumberOfDimensions() const {
    const SerializableDataTypeArray* nested_serializable_array =
        typeid_cast<const SerializableDataTypeArray*>(nested_serializable_data_type.get());
    if (!nested_serializable_array) {
        return 1;
    }

    return 1 + nested_serializable_array->getNumberOfDimensions();
}

static SerializableDataTypePtr create(const DB::ASTPtr& arguments) {
    if (!arguments || arguments->children.size() != 1)
        throw DB::Exception("Serializable Array data type family must have exactly one argument - type of elements",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<SerializableDataTypeArray>(
        SerializableDataTypeFactory::instance().get(arguments->children[0]));
}

void registerDataTypeArray(SerializableDataTypeFactory& factory) { factory.registerDataType("Array", create); }

} // namespace nuclm
