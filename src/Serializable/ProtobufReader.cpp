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

#include <Serializable/ProtobufReader.h>
#include "common/logging.hpp"
#include <glog/logging.h>

#include <string>

namespace nuclm {

bool ProtobufReader::readStringInto(DB::ColumnString::Chars& str) {
    bool result = false;
    size_t old_size = str.size();
    nucolumnar::datatypes::v1::ValueP::KindCase kind = value_p.kind_case();
    if (kind == nucolumnar::datatypes::v1::ValueP::KindCase::kStringValue) {
        std::string val = value_p.string_value();
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " raw value: " << val;
        size_t length = val.size();
        str.resize(old_size + length);
        // array includes the same sequence of characters that make up the value of the string object plus an additional
        // terminating null-character ('\0') at the end. thus, by memcpy just the string length, the '\0' is not copied.
        const char* source = val.c_str();
        char* destination = reinterpret_cast<char*>(str.data() + old_size);
        ::memcpy(destination, source, length);

        total_bytes_read = length;
        result = true;
    }

    return result;
}

bool ProtobufReader::checkNull() {
    nucolumnar::datatypes::v1::ValueP::KindCase kind = value_p.kind_case();
    if (kind == nucolumnar::datatypes::v1::ValueP::KindCase::kNullValue) {
        LOG_MESSAGE(4) << "in protobuf deserializer kind is: " << kind << " corresponding to null value encountered ";
        return true;
    } else {
        LOG_MESSAGE(4) << "in protobuf deserializer did not encounter null value for nullable type";
        return false;
    }
}

void ProtobufReader::addNullBytes() {
    size_t length = sizeof(value_p.null_value()); // this should be a constant value.
    total_bytes_read += length;
}

} // namespace nuclm
