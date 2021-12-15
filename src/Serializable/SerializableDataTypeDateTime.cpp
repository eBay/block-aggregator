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

#include <Serializable/SerializableDataTypeDateTime.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>
#include "common/logging.hpp"

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnsNumber.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Parsers/ASTLiteral.h>

#include <glog/logging.h>

namespace nuclm {

namespace ErrorCodes {
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
} // namespace ErrorCodes

SerializableDataTypeDateTime::SerializableDataTypeDateTime(const std::string& time_zone_name) :
        has_explicit_time_zone(!time_zone_name.empty()),
        time_zone(DateLUT::instance(time_zone_name)),
        utc_time_zone(DateLUT::instance("UTC")) {}

std::string SerializableDataTypeDateTime::doGetName() const {
    if (!has_explicit_time_zone)
        return "DateTime";

    DB::WriteBufferFromOwnString out;
    out << "DateTime(" << DB::quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void SerializableDataTypeDateTime::serializeProtobuf(const DB::IColumn& column, size_t row_num,
                                                     ProtobufWriter& protobuf, size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeDateTime::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf,
                                                       bool allow_add_row, bool& row_added) const {
    row_added = false;

    const nucolumnar::datatypes::v1::ValueP& value_p = protobuf.getValueP();
    uint64_t raw_time = value_p.timestamp().milliseconds();
    // It is the number of seconds that have elapsed since the Unix epoch, so millisecons to seconds conversion is
    // needed.
    time_t t = (time_t)raw_time / 1000;
    LOG_MESSAGE(4) << "in protobuf deserializer read DateTime type has raw value (in ms): " << raw_time
                   << " to be converted to DateTime value (in secs):  " << t;

    auto& container = assert_cast<DB::ColumnUInt32&>(column).getData();
    if (allow_add_row) {
        container.emplace_back(t);
        protobuf.setTotalBytesRead(sizeof(raw_time));
        row_added = true;
    } else
        container.back() = t;
}

bool SerializableDataTypeDateTime::equals(const ISerializableDataType& rhs) const {
    /// DateTime with different timezones are equal, because:
    /// "all types with different time zones are equivalent and may be used interchangingly."
    return typeid(rhs) == typeid(*this);
}

static SerializableDataTypePtr create(const DB::ASTPtr& arguments) {
    if (!arguments)
        return std::make_shared<SerializableDataTypeDateTime>();

    if (arguments->children.size() != 1)
        throw DB::Exception("DateTime data type can optionally have only one argument - time zone name",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto* arg = arguments->children[0]->as<DB::ASTLiteral>();
    if (!arg || arg->value.getType() != DB::Field::Types::String)
        throw DB::Exception("Parameter for DateTime data type must be string literal",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<SerializableDataTypeDateTime>(arg->value.get<DB::String>());
}

void registerDataTypeDateTime(SerializableDataTypeFactory& factory) {
    factory.registerDataType("DateTime", create, SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("TIMESTAMP", "DateTime", SerializableDataTypeFactory::CaseInsensitive);
}

} // namespace nuclm
