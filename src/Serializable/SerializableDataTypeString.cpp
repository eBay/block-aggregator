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

#include <Serializable/SerializableDataTypeString.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>

#include <Columns/ColumnString.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace nuclm {

namespace ErrorCodes {
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
} // namespace ErrorCodes

void SerializableDataTypeString::serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                                                   size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeString::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                                                     bool& row_added) const {
    row_added = false;
    auto& column_string = assert_cast<DB::ColumnString&>(column);
    DB::ColumnString::Chars& data = column_string.getChars();
    DB::ColumnString::Offsets& offsets = column_string.getOffsets();
    size_t old_size = offsets.size();
    try {
        if (allow_add_row) {
            if (protobuf.readStringInto(data)) {
                data.emplace_back(0);
                offsets.emplace_back(data.size());
                row_added = true;
            } else
                data.resize_assume_reserved(offsets.back());
        }
    } catch (...) {
        offsets.resize_assume_reserved(old_size);
        data.resize_assume_reserved(offsets.back());
        throw;
    }
}

bool SerializableDataTypeString::equals(const ISerializableDataType& rhs) const { return typeid(rhs) == typeid(*this); }

void registerDataTypeString(SerializableDataTypeFactory& factory) {
    auto creator = static_cast<SerializableDataTypePtr (*)()>(
        [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeString>()); });

    factory.registerSimpleDataType("String", creator);

    /// These synonyms are added for compatibility.

    factory.registerAlias("CHAR", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARCHAR", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("TEXT", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYTEXT", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMTEXT", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("LONGTEXT", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("BLOB", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("TINYBLOB", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("MEDIUMBLOB", "String", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("LONGBLOB", "String", SerializableDataTypeFactory::CaseInsensitive);
}

} // namespace nuclm
