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

#include <Columns/ColumnsNumber.h>
#include <Serializable/SerializableDataTypeDate.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>
#include "common/logging.hpp"

#include <common/LocalDateTime.h>

#include <glog/logging.h>

namespace nuclm {

namespace ErrorCodes {
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
} // namespace ErrorCodes

void SerializableDataTypeDate::serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                                                 size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeDate::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                                                   bool& row_added) const {
    row_added = false;

    const nucolumnar::datatypes::v1::ValueP& value_p = protobuf.getValueP();
    uint64_t raw_time = value_p.timestamp().milliseconds();
    time_t seconds = (time_t)(raw_time / 1000);
    // Note the following after toUnderType() is Int32, but our date range still falls into the two-byte represention.
    auto t = DateLUT::instance().toDayNum(seconds).toUnderType();

    LOG_MESSAGE(4) << "in protobuf deserializer read Date type has raw value (in ms): " << raw_time
                   << " to be converted to date value:  " << (DB::UInt16)t;

    auto& container = assert_cast<DB::ColumnUInt16&>(column).getData();
    if (allow_add_row) {
        container.emplace_back(t);
        protobuf.setTotalBytesRead(sizeof(raw_time));
        row_added = true;
    } else
        container.back() = t;
}

bool SerializableDataTypeDate::equals(const ISerializableDataType& rhs) const { return typeid(rhs) == typeid(*this); }

void registerDataTypeDate(SerializableDataTypeFactory& factory) {
    factory.registerSimpleDataType(
        "Date", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeDate>()); },
        SerializableDataTypeFactory::CaseInsensitive);
}

} // namespace nuclm
