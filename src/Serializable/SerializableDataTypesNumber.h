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

#pragma once

#include <type_traits>
#include <Core/Field.h>
#include <Serializable/SerializableDataTypeNumberBase.h>

namespace nuclm {

template <typename T> class SerializableDataTypeNumber final : public SerializableDataTypeNumberBase<T> {
    bool equals(const ISerializableDataType& rhs) const override { return typeid(rhs) == typeid(*this); }
    bool canBeInsideNullable() const override { return true; }
};

using SerializableDataTypeUInt8 = SerializableDataTypeNumber<DB::UInt8>;
using SerializableDataTypeUInt16 = SerializableDataTypeNumber<DB::UInt16>;
using SerializableDataTypeUInt32 = SerializableDataTypeNumber<DB::UInt32>;
using SerializableDataTypeUInt64 = SerializableDataTypeNumber<DB::UInt64>;
using SerializableDataTypeInt8 = SerializableDataTypeNumber<DB::Int8>;
using SerializableDataTypeInt16 = SerializableDataTypeNumber<DB::Int16>;
using SerializableDataTypeInt32 = SerializableDataTypeNumber<DB::Int32>;
using SerializableDataTypeInt64 = SerializableDataTypeNumber<DB::Int64>;
using SerializableDataTypeFloat32 = SerializableDataTypeNumber<DB::Float32>;
using SerializableDataTypeFloat64 = SerializableDataTypeNumber<DB::Float64>;

template <typename DataType> constexpr bool IsDataTypeNumber = false;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::UInt8>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::UInt16>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::UInt32>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::UInt64>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::Int8>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::Int16>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::Int32>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::Int64>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::Float32>> = true;
template <> inline constexpr bool IsDataTypeNumber<SerializableDataTypeNumber<DB::Float64>> = true;

} // namespace nuclm
