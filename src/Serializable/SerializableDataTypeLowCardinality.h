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

/// LowCardinality
class SerializableDataTypeLowCardinality final : public ISerializableDataType {
  public:
    static constexpr bool is_parametric = true;

    explicit SerializableDataTypeLowCardinality(const SerializableDataTypePtr& nested_serializable_data_type_,
                                                const DB::DataTypePtr& nested_true_dictionary_type_);
    std::string doGetName() const override {
        return "LowCardinality(" + nested_serializable_data_type->getName() + ")";
    }
    const char* getFamilyName() const override { return "LowCardinality"; }
    DB::TypeIndex getTypeId() const override { return DB::TypeIndex::LowCardinality; }

    void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                           size_t& value_index) const override;
    void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                             bool& row_added) const override;

    bool equals(const ISerializableDataType& rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool isComparable() const override { return nested_serializable_data_type->isComparable(); }
    bool isNullable() const override { return false; }
    bool lowCardinality() const override { return true; }

    const SerializableDataTypePtr& getNestedType() const { return nested_serializable_data_type; }
    const DB::DataTypePtr& getNestedTrueDataType() const { return nested_true_dictionary_type; }

  private:
    SerializableDataTypePtr nested_serializable_data_type;
    DB::DataTypePtr nested_true_dictionary_type;
};

} // namespace nuclm
