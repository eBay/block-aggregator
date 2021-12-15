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

#include <ostream>

#include <Serializable/ISerializableDataType.h>

namespace nuclm {

class SerializableDataTypeString final : public ISerializableDataType {
  public:
    using FieldType = DB::String;
    static constexpr bool is_parametric = false;

    const char* getFamilyName() const override { return "String"; }

    DB::TypeIndex getTypeId() const override { return DB::TypeIndex::String; }

    void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                           size_t& value_index) const override;
    void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                             bool& row_added) const override;

    bool equals(const ISerializableDataType& rhs) const override;

    bool isParametric() const override { return false; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }
};

} // namespace nuclm
