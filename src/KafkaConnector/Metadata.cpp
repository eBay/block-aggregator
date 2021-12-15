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

#include "Metadata.h"
#include "sstream"
#include <string>

extern int getDefaultMetadataVersion();

namespace kafka {
const std::string Metadata::referenceSeparator = ",,";
int Metadata::metadataVersion = getDefaultMetadataVersion();

void Metadata::update(std::string table, int64_t begin, int64_t end, int64_t count) {
    offsets[table].begin = begin;
    offsets[table].end = end;
    offsets[table].count = count;
}

void Metadata::addFrom(Metadata* saved) {
    reference = saved->reference;
    for (auto& offset : saved->offsets) {
        offsets[offset.first].begin = offset.second.begin;
        offsets[offset.first].end = offset.second.end;
        offsets[offset.first].count = offset.second.count;
    }
}

int64_t Metadata::min() {
    int64_t min_ = INT64_MAX;
    for (auto& offset : offsets) {
        min_ = std::min(min_, offset.second.begin);
    }
    if (min_ == INT64_MAX)
        min_ = EARLIEST_OFFSET;
    return min_;
}

int64_t Metadata::max() {
    int64_t max_ = EARLIEST_OFFSET;
    for (auto& offset : offsets) {
        max_ = std::max(max_, offset.second.end);
    }
    return max_;
}

std::string Metadata::serialize_v1() const {
    std::ostringstream ss;
    ss << "1" << referenceSeparator << replica_id;
    for (auto it = offsets.begin(); it != offsets.end(); it++) {
        ss << separator;
        ss << it->first << separator << it->second.begin << separator << it->second.end << separator
           << it->second.count;
    }
    ss << referenceSeparator << reference;
    return ss.str();
}

std::string Metadata::serialize_v0() const {
    std::ostringstream ss;
    for (auto it = offsets.begin(); it != offsets.end(); it++) {
        if (it != offsets.begin())
            ss << separator;
        ss << it->first << separator << it->second.begin << separator << it->second.end;
    }
    return ss.str();
}

std::string Metadata::serialize(int version_) const {
    if (version_ == -1) {
        version_ = metadataVersion;
    }
    if (version_ == 0)
        return serialize_v0();
    else if (version_ == 1)
        return serialize_v1();
    else
        return ""; // implement future versions here
}

void Metadata::deserialize_v1(const std::string& meta) {
    std::vector<std::string> parts;
    Metadata::split(meta, parts, ",,");
    reference = std::stol(parts[2]);
    std::stringstream s_stream(parts[1]);
    getline(s_stream, replica_id, separator);
    while (s_stream.good()) {
        std::string table;
        getline(s_stream, table, separator);
        if (table.empty())
            break;
        std::string begin_str;
        getline(s_stream, begin_str, separator);

        std::string end_str;
        getline(s_stream, end_str, separator);

        std::string count_str;
        getline(s_stream, count_str, separator);

        offsets[table].begin = std::stol(begin_str);
        offsets[table].end = std::stol(end_str);
        offsets[table].count = std::stol(count_str);
    }
}

// Note that even when metadata is version 0, when we parse it we use the default version number for this object.
// so we don't set version in this function.
void Metadata::deserialize_v0(const std::string& meta) {
    reference = -1;
    std::stringstream s_stream(meta);
    while (s_stream.good()) {
        std::string table;
        getline(s_stream, table, separator);
        if (table.empty())
            break;

        std::string begin_str;
        getline(s_stream, begin_str, separator);

        std::string end_str;
        getline(s_stream, end_str, separator);

        offsets[table].begin = std::stol(begin_str);
        offsets[table].end = std::stol(end_str);
        offsets[table].count = 0;
    }
}

void Metadata::deserialize(const std::string& meta) {
    if (!meta.empty()) {
        auto index = meta.find(",,");
        if (index != std::string::npos) {
            auto metadata_version_ = std::stoi(meta.substr(0, index));
            if (metadata_version_ >= 1) { // Future versions must be backward compatible with this version.
                deserialize_v1(meta);
            }
        } else { // old formats (without version number)
            deserialize_v0(meta);
        }
    }
}

Offset Metadata::getOffset(const std::string& table) const {
    auto it = offsets.find(table);
    if (it != offsets.end())
        return it->second;
    else
        return {-1, -1, 0};
}

void Metadata::remove(std::string table) { offsets.erase(table); }
bool Metadata::empty() { return offsets.empty(); }

/**
 *Check the emptiness of the metadata while ignoring offset with special begin and end (i.e., begin =  end + 1)
 *As long as there is at least one table with begin <= end, then we are not done with the REPLAY.
 */
bool Metadata::emptyIgnoreSpecials() {
    for (auto it = offsets.begin(); it != offsets.end(); it++) {
        if (it->second.begin <= it->second.end)
            return false;
    }
    return true;
}

std::vector<std::string> Metadata::getTables() {
    std::vector<std::string> tables;
    for (auto it = offsets.begin(); it != offsets.end(); it++) {
        tables.push_back(it->first);
    }
    return tables;
}

void Metadata::clear() {
    offsets.clear();
    reference = -1;
}

void Metadata::split(const std::string& str, std::vector<std::string>& cont, std::string delim) {
    std::size_t current, previous = 0;
    current = str.find(delim);
    while (current != std::string::npos) {
        cont.push_back(str.substr(previous, current - previous));
        previous = current + delim.size();
        current = str.find(delim, previous);
    }
    cont.push_back(str.substr(previous, current - previous));
}

} // namespace kafka
