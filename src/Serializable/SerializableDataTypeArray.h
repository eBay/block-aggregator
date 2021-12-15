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

namespace nuclm {

/**
 * The serializer class for array that can be nested.
 */
class SerializableDataTypeArray final : public ISerializableDataType {
  public:
    SerializableDataTypeArray(const SerializableDataTypePtr& nested_serializable_data_type);

    const char* getFamilyName() const override { return "Array"; }

    std::string doGetName() const override { return "Array(" + nested_serializable_data_type->getName() + ")"; }

    DB::TypeIndex getTypeId() const override { return DB::TypeIndex::Array; }

    void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                           size_t& value_index) const override;
    void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                             bool& row_added) const override;

    bool equals(const ISerializableDataType& rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    bool isComparable() const override { return true; }

    const SerializableDataTypePtr& getNestedType() const { return nested_serializable_data_type; }

    // 1 for plain array, 2 for array of arrays and so on
    size_t getNumberOfDimensions() const;

  private:
    SerializableDataTypePtr nested_serializable_data_type;
};

} // namespace nuclm
