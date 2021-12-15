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

#include <Serializable/SerializableDataTypeNumberBase.h>
#include <Serializable/ProtobufReader.h>
#include <Serializable/ProtobufWriter.h>

#include "common/logging.hpp"

#include <type_traits>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnConst.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <glog/logging.h>

namespace nuclm {

namespace ErrorCodes {
extern const int SERIALIZATION_METHOD_NOT_IMPLEMENTED;
} // namespace ErrorCodes

template <typename T>
void SerializableDataTypeNumberBase<T>::serializeProtobuf(const DB::IColumn& column, size_t row_num,
                                                          ProtobufWriter& protobuf, size_t& value_index) const {
    std::string exception_message = std::string(getFamilyName()) + " protobuf-based serialization is not implemented";
    throw DB::Exception(exception_message, ErrorCodes::SERIALIZATION_METHOD_NOT_IMPLEMENTED);
}

template <typename T>
void SerializableDataTypeNumberBase<T>::deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf,
                                                            bool allow_add_row, bool& row_added) const {
    row_added = false;
    T value{};
    const nucolumnar::datatypes::v1::ValueP& value_p = protobuf.getValueP();

    nucolumnar::datatypes::v1::ValueP::KindCase kind = value_p.kind_case();
    LOG_MESSAGE(4) << "in protobuf deserializer: kind-case is: " << kind;

    switch (kind) {
    case nucolumnar::datatypes::v1::ValueP::KindCase::kIntValue: {
        int rawvalue_int = value_p.int_value();
        value = rawvalue_int;
        protobuf.setTotalBytesRead(sizeof(value));
        row_added = true;
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << rawvalue_int;
        break;
    }
    case nucolumnar::datatypes::v1::ValueP::KindCase::kLongValue: {
        long rawvalue_long = value_p.long_value();
        value = rawvalue_long;
        protobuf.setTotalBytesRead(sizeof(value));
        row_added = true;
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << rawvalue_long;
        break;
    }
    case nucolumnar::datatypes::v1::ValueP::KindCase::kUintValue: {
        uint32_t rawvalue_uint32 = value_p.uint_value();
        value = rawvalue_uint32;
        protobuf.setTotalBytesRead(sizeof(value));
        row_added = true;
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << rawvalue_uint32;
        break;
    }
    case nucolumnar::datatypes::v1::ValueP::KindCase::kUlongValue: {
        uint64_t rawvalue_uint64 = value_p.ulong_value();
        value = rawvalue_uint64;
        protobuf.setTotalBytesRead(sizeof(value));
        row_added = true;
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << rawvalue_uint64;
        break;
    }
    case nucolumnar::datatypes::v1::ValueP::KindCase::kDoubleValue: {
        double rawvalue_double = value_p.double_value();
        value = rawvalue_double;
        protobuf.setTotalBytesRead(sizeof(value));
        row_added = true;
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << rawvalue_double;
        break;
    }
    case nucolumnar::datatypes::v1::ValueP::KindCase::kBoolValue: {
        bool rawvalue_bool = value_p.bool_value();
        value = rawvalue_bool;
        protobuf.setTotalBytesRead(sizeof(value));
        row_added = true;
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << rawvalue_bool;
        break;
    }
    default: {
        LOG_MESSAGE(4) << "in protobuf valuep kind-case enum, choose the default path";
        break;
    }
    }

    if (!row_added) {
        return;
    }

    auto& container = typeid_cast<DB::ColumnVector<T>&>(column).getData();
    if (allow_add_row) {
        container.emplace_back(value);
        row_added = true;
    } else
        container.back() = value;
}

template <typename T> bool SerializableDataTypeNumberBase<T>::isValueRepresentedByInteger() const {
    return is_integer_v<T>;
}

template <typename T> bool SerializableDataTypeNumberBase<T>::isValueRepresentedByUnsignedInteger() const {
    return is_integer_v<T> && is_unsigned_v<T>;
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class SerializableDataTypeNumberBase<DB::UInt8>;
template class SerializableDataTypeNumberBase<DB::UInt16>;
template class SerializableDataTypeNumberBase<DB::UInt32>;
template class SerializableDataTypeNumberBase<DB::UInt64>;
template class SerializableDataTypeNumberBase<DB::UInt128>;
template class SerializableDataTypeNumberBase<DB::Int8>;
template class SerializableDataTypeNumberBase<DB::Int16>;
template class SerializableDataTypeNumberBase<DB::Int32>;
template class SerializableDataTypeNumberBase<DB::Int64>;
template class SerializableDataTypeNumberBase<DB::Float32>;
template class SerializableDataTypeNumberBase<DB::Float64>;

} // namespace nuclm
