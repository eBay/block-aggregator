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

#define MAX_FIXEDSTRING_SIZE 0xFFFFFF

namespace nuclm {

namespace ErrorCodes {
extern const int ARGUMENT_OUT_OF_BOUND;
}

class SerializableDataTypeFixedString final : public ISerializableDataType {
  private:
    size_t n;

  public:
    static constexpr bool is_parametric = true;

    SerializableDataTypeFixedString(size_t n_) : n(n_) {
        if (n == 0)
            throw DB::Exception("FixedString size must be positive", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        if (n > MAX_FIXEDSTRING_SIZE)
            throw DB::Exception("FixedString size is too large", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    std::string doGetName() const override;
    DB::TypeIndex getTypeId() const override { return DB::TypeIndex::FixedString; }

    const char* getFamilyName() const override { return "FixedString"; }

    size_t getN() const { return n; }

    void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                           size_t& value_index) const override;
    void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                             bool& row_added) const override;

    bool equals(const ISerializableDataType& rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return true; }
};

} // namespace nuclm
