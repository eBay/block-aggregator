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

#include <Serializable/SerializableDataTypeDateTime64.h>
#include <Serializable/SerializableDataTypeFactory.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>
#include "common/logging.hpp"

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnDecimal.h>

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

SerializableDataTypeDateTime64::SerializableDataTypeDateTime64(UInt32 scale_, const std::string& time_zone_name) :
        has_explicit_time_zone(!time_zone_name.empty()),
        scale(scale_),
        time_zone(DateLUT::instance(time_zone_name)),
        utc_time_zone(DateLUT::instance("UTC")) {}

std::string SerializableDataTypeDateTime64::doGetName() const {
    if (!has_explicit_time_zone)
        return std::string(getFamilyName()) + "(" + std::to_string(this->scale) + ")";

    DB::WriteBufferFromOwnString out;
    out << "DateTime64(" << this->scale << ", " << DB::quote << time_zone.getTimeZone() << ")";
    return out.str();
}

void SerializableDataTypeDateTime64::serializeProtobuf(const DB::IColumn& column, size_t row_num,
                                                       ProtobufWriter& protobuf, size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

void SerializableDataTypeDateTime64::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf,
                                                         bool allow_add_row, bool& row_added) const {
    row_added = false;

    const nucolumnar::datatypes::v1::ValueP& value_p = protobuf.getValueP();
    uint64_t raw_time = value_p.timestamp().milliseconds();
    // The number of seconds that have elapsed since the Unix epoch, so milliseconds to seconds conversion is needed.
    time_t whole = (time_t)raw_time / 1000;
    uint64_t remained = raw_time - whole * 1000; // the number of milliseconds

    LOG_MESSAGE(4) << "in protobuf deserializer read DateTime64 type has raw value (in ms): " << raw_time
                   << " to be converted to DateTime64 with second part value:  " << whole
                   << " and smaller than second part value: " << remained;

    // DB::DateTime64 is with Decimal64.
    DB::DecimalUtils::DecimalComponents<DB::Decimal64> components{static_cast<DB::Decimal64>(whole), 0};

    components.fractional = 0; // for scale  = 0, for example, remained = 387, fractional = 0
    if (scale == 1) {
        components.fractional = remained / 100; // for scale = 1, for example, remained = 387, fractional = 3;
    } else if (scale == 2) {
        components.fractional = remained / 10; // for scale = 2, for example, remained = 387, fraction = 38
    } else if (scale >= 3) {
        components.fractional =
            remained; // our precision is at millisecond at best, for example, remained = 387, fraction = 387.
    }

    DB::DateTime64 datetime64 = DB::DecimalUtils::decimalFromComponents<DB::Decimal64>(components, scale);

    auto& container = assert_cast<DB::ColumnDecimal<DB::DateTime64>&>(column).getData();
    if (allow_add_row) {
        container.emplace_back(datetime64);
        protobuf.setTotalBytesRead(sizeof(datetime64));
        row_added = true;
    } else
        container.back() = datetime64;
}

bool SerializableDataTypeDateTime64::equals(const ISerializableDataType& rhs) const {
    if (const auto* ptype = typeid_cast<const SerializableDataTypeDateTime64*>(&rhs)) {
        return this->scale == ptype->getScale();
    }
    return false;
}

enum class ArgumentKind { Optional, Mandatory };

template <typename T, ArgumentKind Kind>
std::conditional_t<Kind == ArgumentKind::Optional, std::optional<T>, T>
getArgument(const DB::ASTPtr& arguments, size_t argument_index, const char* argument_name,
            const std::string context_data_type_name) {
    using NearestResultType = DB::NearestFieldType<T>;
    const auto field_type = DB::Field::TypeToEnum<NearestResultType>::value;
    const DB::ASTLiteral* argument = nullptr;

    auto exception_message = [=](const String& message) {
        return std::string("Parameter #") + std::to_string(argument_index) + " '" + argument_name + "' for " +
            context_data_type_name + message + ", expected: " + DB::Field::Types::toString(field_type) + " literal.";
    };

    if (!arguments || arguments->children.size() <= argument_index ||
        !(argument = arguments->children[argument_index]->as<DB::ASTLiteral>())) {
        if constexpr (Kind == ArgumentKind::Optional)
            return {};
        else
            throw DB::Exception(exception_message(" is missing"), ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    if (argument->value.getType() != field_type)
        throw DB::Exception(exception_message(String(" has wrong type: ") + argument->value.getTypeName()),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return argument->value.get<NearestResultType>();
}

static SerializableDataTypePtr create64(const DB::ASTPtr& arguments) {
    if (!arguments || arguments->size() == 0)
        return std::make_shared<SerializableDataTypeDateTime64>(SerializableDataTypeDateTime64::default_scale);

    // The following parsing follows the logic that is in DateTypeDateTime64.cpp.
    // If scale can be parsed successfully, then !!scale has value of 1, we move to parse argument index = 1 for
    // timezone. If scale does not exist and then !!scale will return 0, then we move to parse argument index = 0 for
    // timezone. If timezone does not exist, we are still OK in the parsing, that is, DateTime64(), for example. But if
    // scale does not exist and timezone exists, DateTime64('America/Phoenix'), then the string value will be
    // interpreted for scale, and we will have "wrong type" exception raised in getArgument. In summary, the following
    // statements support the syntax of: DateTime64(scale, [timezone]).
    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "DateType64");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, !!scale, "timezone", "DateType64");

    return std::make_shared<SerializableDataTypeDateTime64>(
        scale.value_or(SerializableDataTypeDateTime64::default_scale), timezone.value_or(String{}));
}

void registerDataTypeDateTime64(SerializableDataTypeFactory& factory) {
    factory.registerDataType("DateTime64", create64, SerializableDataTypeFactory::CaseInsensitive);
}

} // namespace nuclm
