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

#include <Aggregator/TableColumnsDescription.h>
#include <KafkaConnector/KafkaConnector.h>
#include <Aggregator/AggregatorLoaderManager.h>

namespace nuclm {

/**
 * The lifetime of the schema tracker is the same as the life time of a buffer that is used to construct a single block.
 * As a result, we expect that the total number of the schema being held in the tracker at most to be two. The situation
 * of two schemas happen is that during buffer construction, a new message with a new schema comes and that triggers the
 * table schema retrieval from the backend clickhouse.
 *
 */
class TableSchemaUpdateTracker {
  public:
    TableSchemaUpdateTracker(const std::string& table_name, const TableColumnsDescription& initial_table_definition,
                             const AggregatorLoaderManager& loader_manager);

    /**
     * To check whether the hash value corresponds to an entry in incoming_hash_to_schema_version.
     * If the entry does not exist, or the entry does exist, but the corresponding version is not the most recent one
     * held in the tracker, we need to return false, which then force the caller to retrieve the latest table schema.
     */
    bool checkHashWithLatestSchemaVersion(size_t hash_value);

    /**
     * This method is called after checkHashWithLatestSchemaVersion returns false.
     *
     * Try to add the new schema to the tracker that matches the new hash.
     * (1) It does not guarantee that the new schema is available at the backend database;
     * (2) if the new schema is available, the corresponding schema does not necessarily agree with
     * the specified hash_code, as the hash_code may be too old.
     *
     * If the new schema really gets fetched, then return the flag on new_schema_fetched.
     *
     * And the hash_code will be updated with the latest schema version, independent of whether the
     * schema has been fetched and updated or not.
     *
     */
    void tryAddNewSchemaVersionForNewHash(size_t hash_code, bool& new_schema_fetched);

    void updateHashWithLatestSchemaVersion(size_t hash_code) {
        incoming_hash_to_schema_version[hash_code] = getLatestSchemaVersion();
    }

    /**
     * Return the latest schema managed by the schema tracker.
     */
    const TableColumnsDescription& getLatestSchema() const { return schema_captured.back(); }

    /**
     * At most two in the lifetime of the buffer and thus the schema update tracker.
     */
    size_t getTotalNumberOfCapturedSchemas() const { return schema_captured.size(); }

    size_t getLatestSchemaVersion() const { return schema_captured.size() - 1; }

    /**
     * To check whether the hash of the schema used for message deserialization matches the
     * hash of the latest schema.
     */
    bool currentSchemaUsedInBlockMatchesLatestSchema() const {
        return (hash_of_schema_currently_used == schema_captured.back().getSchemaHash());
    }

    /**
     * To update the hash of the schema used for message deserialization to the hash of the
     * lastest schema.
     */
    void updateCurrentSchemaUsedInBlockWithLatestSchema() {
        hash_of_schema_currently_used = schema_captured.back().getSchemaHash();
    }

  protected:
    /**
     * Update incoming message's hash to the latest table schema version. This is for testing purpose
     */
    void updateHashMapping(size_t hash_in_message, const TableColumnsDescription& latest_schema);

  private:
    std::string table_name;
    const AggregatorLoaderManager& loader_manager;

    // the index of the vector is the version of the schema.
    std::vector<TableColumnsDescription> schema_captured;

    // key is the hash embedded in the incoming message, value is the schema version seen so far
    // by the incoming hash. If the version is not the latest version held, then we need to trigger
    // the new table schema retrieval, check whether it is the same as the latest one, and then update
    // this hash table. Whenever we see a new hash that does not match the latest version, schema retrieval
    // needs to happen.
    std::unordered_map<size_t, size_t> incoming_hash_to_schema_version;

    // the current schema hash is being used for block deserializaiton. the initial one is the one passed
    // from the initial table definition.
    size_t hash_of_schema_currently_used;
};

using TableSchemaUpdateTrackerPtr = std::shared_ptr<TableSchemaUpdateTracker>;

}; // namespace nuclm
