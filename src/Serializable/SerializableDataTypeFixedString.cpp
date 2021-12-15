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

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>

#include <Serializable/SerializableDataTypeFixedString.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>

#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/VarInt.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <glog/logging.h>

namespace nuclm {

namespace ErrorCodes {
extern const int CANNOT_READ_ALL_DATA;
extern const int TOO_LARGE_STRING_SIZE;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UNEXPECTED_AST_STRUCTURE;
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;

} // namespace ErrorCodes

std::string SerializableDataTypeFixedString::doGetName() const { return "FixedString(" + DB::toString(n) + ")"; }

void SerializableDataTypeFixedString::serializeProtobuf(const DB::IColumn& column, size_t row_num,
                                                        ProtobufWriter& protobuf, size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

static inline void alignStringLength(const SerializableDataTypeFixedString& type, DB::ColumnFixedString::Chars& data,
                                     size_t string_start) {
    size_t length = data.size() - string_start;
    if (length < type.getN()) {
        data.resize_fill(string_start + type.getN());
    } else if (length > type.getN()) {
        data.resize_assume_reserved(string_start);
        throw DB::Exception("Too large value for " + type.getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);
    }
}

void SerializableDataTypeFixedString::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf,
                                                          bool allow_add_row, bool& row_added) const {
    row_added = false;
    auto& column_string = assert_cast<DB::ColumnFixedString&>(column);
    DB::ColumnFixedString::Chars& data = column_string.getChars();
    size_t old_size = data.size();
    try {
        if (allow_add_row) {
            if (protobuf.readStringInto(data)) {
                alignStringLength(*this, data, old_size);
                row_added = true;
            } else
                data.resize_assume_reserved(old_size);
        } else {
            DB::ColumnFixedString::Chars temp_data;
            if (protobuf.readStringInto(temp_data)) {
                alignStringLength(*this, temp_data, 0);
                column_string.popBack(1);
                old_size = data.size();
                data.insertSmallAllowReadWriteOverflow15(temp_data.begin(), temp_data.end());
            }
        }
    } catch (...) {
        data.resize_assume_reserved(old_size);
        throw;
    }
}

bool SerializableDataTypeFixedString::equals(const ISerializableDataType& rhs) const {
    return typeid(rhs) == typeid(*this) && n == static_cast<const SerializableDataTypeFixedString&>(rhs).n;
}

static SerializableDataTypePtr create(const DB::ASTPtr& arguments) {
    if (!arguments || arguments->children.size() != 1)
        throw DB::Exception("FixedString data type family must have exactly one argument - size in bytes",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto* argument = arguments->children[0]->as<DB::ASTLiteral>();
    if (!argument || argument->value.getType() != DB::Field::Types::UInt64 || argument->value.get<DB::UInt64>() == 0)
        throw DB::Exception("FixedString data type family must have a number (positive integer) as its argument",
                            ErrorCodes::UNEXPECTED_AST_STRUCTURE);

    return std::make_shared<SerializableDataTypeFixedString>(argument->value.get<DB::UInt64>());
}

void registerDataTypeFixedString(SerializableDataTypeFactory& factory) {
    factory.registerDataType("FixedString", create);

    /// Compatibility alias.
    factory.registerAlias("BINARY", "FixedString", SerializableDataTypeFactory::CaseInsensitive);
}

} // namespace nuclm
