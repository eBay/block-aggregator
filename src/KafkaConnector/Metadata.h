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

#include <string>
#include <map>
#include <iostream>
#include <vector>

extern int getLatestMetdataVersion();
namespace kafka {

struct Offset {
  public:
    Offset(int64_t begin_ = -1, int64_t end_ = -1, int64_t count_ = 0) : begin(begin_), end(end_), count(count_) {}
    int64_t begin;
    int64_t end;
    int64_t count;
};

/**
 * Keeps track of the metadata for a single partition. For each table it keeps track of the begin and end offset
 * of the last batch sent for that table.
 */
class Metadata {
  protected:
    std::map<std::string, Offset> offsets; //<table, Offset>
    std::string replica_id;
    // We count number of messages for each table from this reference offset, instead of from the stream origin that
    // corresponds to the first message. How to force the reference to be check-pointed is not implemented yet.
    int64_t reference;

  public:
    // static constexpr int latestVersion = 1;
    static int metadataVersion;
    static const int64_t EARLIEST_OFFSET = -1l;
    static const char separator = ',';
    static const std::string referenceSeparator;

    Metadata(std::string replica_id_ = "no_replica_id", int reference_ = -1) :
            replica_id(replica_id_), reference(reference_) {}

    void addFrom(Metadata* saved);
    void update(std::string table, int64_t begin, int64_t end, int64_t count);
    int64_t min();
    int64_t max();
    int64_t getReference() { return reference; }
    void setReference(int64_t reference_) { reference = reference_; }

    static bool setVersion(int version_) {
        if (version_ <= getLatestMetdataVersion() && version_ >= 0) {
            metadataVersion = version_;
            return true;
        }
        return false;
    }
    static int getVersion() { return metadataVersion; }
    std::string serialize_v1() const;
    std::string serialize_v0() const;
    std::string serialize(int version_ = -1) const;
    void deserialize_v1(const std::string& meta);
    void deserialize_v0(const std::string& meta);
    void deserialize(const std::string& meta);
    void remove(std::string table);
    Offset getOffset(const std::string& table) const;
    bool empty();
    bool emptyIgnoreSpecials();
    std::vector<std::string> getTables();
    std::string getReplicaId() { return replica_id; }
    void setReplicaId(const std::string& replica_id_) {
        if (!replica_id_.empty())
            replica_id = replica_id_;
    }
    void clear();

    static void split(const std::string& str, std::vector<std::string>& cont, std::string delim);
};
} // namespace kafka
