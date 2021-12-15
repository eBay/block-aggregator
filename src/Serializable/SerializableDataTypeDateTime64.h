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

class DateLUTImpl;

namespace nuclm {

/**
 * DateTime64 is same as DateTime, but it stores values as Int64 and has configurable sub-second part.
 * `scale` determines number of decimal places for sub-second part of the DateTime64.
 * Since we retrieve the data from Kafka message, with time-stamp being defined as the milliseconds since Epoch
 * we will convert the time stmp to seconds (like what we did in DateType), and then take the remaining part
 * that represents the sub-seconds, and use it to compute the sub-seconds part with the scale as fraction.
 * from example, <time in second>.387:
 * scale = 0  --> fraction = 0
   scale = 1  --> fraction = 3
   scale = 2  --> fractor = 38
   scale = 3  --> fraction = 387

   for scale > 4, we will just keep the same fraction 387, as that is the precision that we can get from the client

   but for de-serializer, we will just take the largest one for scale = 3, and when the data gets displayed,
   it will be truncated.

 */
class SerializableDataTypeDateTime64 final : public ISerializableDataType {
  public:
    static constexpr UInt8 default_scale = 3;

    SerializableDataTypeDateTime64(UInt32 scale, const std::string& time_zone_name = "");

    const char* getFamilyName() const override { return "DateTime64"; }
    UInt32 getScale() const { return scale; }

    std::string doGetName() const override;
    DB::TypeIndex getTypeId() const override { return DB::TypeIndex::DateTime64; }

    void serializeProtobuf(const DB::IColumn& column, size_t row_num, ProtobufWriter& protobuf,
                           size_t& value_index) const override;
    void deserializeProtobuf(DB::IColumn& column, ProtobufReader& protobuf, bool allow_add_row,
                             bool& row_added) const override;

    bool equals(const ISerializableDataType& rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool isComparable() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool canBeInsideLowCardinality() const override { return false; }

    const DateLUTImpl& getTimeZone() const { return time_zone; }

  private:
    bool has_explicit_time_zone;
    UInt32 scale;
    const DateLUTImpl& time_zone;
    const DateLUTImpl& utc_time_zone;
};

} // namespace nuclm
