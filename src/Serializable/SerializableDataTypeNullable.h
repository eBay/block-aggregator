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

#include <Serializable/ISerializableDataType.h>
#include <DataTypes/IDataType.h>

namespace nuclm {

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class SerializableDataTypeNullable final : public ISerializableDataType {
  public:
    static constexpr bool is_parametric = true;

    explicit SerializableDataTypeNullable(const SerializableDataTypePtr& nested_serializable_data_type,
                                          const DB::DataTypePtr& nested_true_data_type);
    std::string doGetName() const override { return "Nullable(" + nested_serializable_data_type->getName() + ")"; }
    const char* getFamilyName() const override { return "Nullable"; }
    DB::TypeIndex getTypeId() const override { return DB::TypeIndex::Nullable; }

    void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                           size_t& value_index) const override;
    void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                             bool& row_added) const override;

    bool equals(const ISerializableDataType& rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool isComparable() const override { return nested_serializable_data_type->isComparable(); }
    bool isNullable() const override { return true; }

    const SerializableDataTypePtr& getNestedType() const { return nested_serializable_data_type; }

    const DB::DataTypePtr& getNestedTrueDataType() const { return nested_true_data_type; }

  private:
    SerializableDataTypePtr nested_serializable_data_type;
    DB::DataTypePtr nested_true_data_type;
};

SerializableDataTypePtr makeNullable(const SerializableDataTypePtr& type);
SerializableDataTypePtr removeNullable(const SerializableDataTypePtr& type);

} // namespace nuclm
