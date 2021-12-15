/************************************************************************
Copyright 2021, eBay, Inc.

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

#include <nucolumnar/datatypes/v1/columnartypes.pb.h>
#include <Columns/ColumnString.h>

namespace nuclm {

class ProtobufReader {
  public:
    ProtobufReader(const nucolumnar::datatypes::v1::ValueP& value_p_) : value_p(value_p_), total_bytes_read(0) {}

    ~ProtobufReader() = default;

    size_t getTotalBytesRead() { return total_bytes_read; }

    void setTotalBytesRead(size_t size) { total_bytes_read = size; }

    const nucolumnar::datatypes::v1::ValueP& getValueP() const { return value_p; }

    bool readStringInto(DB::ColumnString::Chars& data);

    bool checkNull(); // check whether the null value encountered.

    void addNullBytes(); // add to the bytes consumed due to the null value

  private:
    const nucolumnar::datatypes::v1::ValueP& value_p;
    size_t total_bytes_read;
};

} // namespace nuclm
