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

#include <Aggregator/TableSchemaUpdateTracker.h>
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

namespace nuclm {

TableSchemaUpdateTracker::TableSchemaUpdateTracker(const std::string& table_name_,
                                                   const TableColumnsDescription& initial_table_definition_,
                                                   const AggregatorLoaderManager& loader_manager_) :
        table_name(table_name_), loader_manager(loader_manager_) {
    schema_captured.push_back(initial_table_definition_);
    hash_of_schema_currently_used = initial_table_definition_.getSchemaHash();
}

bool TableSchemaUpdateTracker::checkHashWithLatestSchemaVersion(size_t hash_value) {
    bool with_latest_schema = true;
    auto entry_found = incoming_hash_to_schema_version.find(hash_value);
    if (entry_found != incoming_hash_to_schema_version.end()) {
        if ((*entry_found).second != getLatestSchemaVersion()) {
            with_latest_schema = false;
        } else {
            with_latest_schema = true;
        }
    } else {
        if (hash_value == getLatestSchema().getSchemaHash()) {
            with_latest_schema = true;
            incoming_hash_to_schema_version[hash_value] = getLatestSchemaVersion();
        } else {
            with_latest_schema = false;

            // do not update hash code to incoming_hash_to_schema_version, until the possible new schema is fetched.
        }
    }

    return with_latest_schema;
}

// NOTE: schema version computation currently does not involve default column's expression
void TableSchemaUpdateTracker::tryAddNewSchemaVersionForNewHash(size_t hash_code, bool& new_schema_fetched) {
    // force to retrieve the table definition from the backend. We may not get the new version.
    new_schema_fetched = false;
    try {
        const TableColumnsDescription& possible_new_schema =
            loader_manager.getTableColumnsDefinition(table_name, false);
        LOG_AGGRPROC(4) << "possible new schema's hash is: " << possible_new_schema.getSchemaHash()
                        << " and latest schema hash is: " << getLatestSchema().getSchemaHash();
        if (possible_new_schema.getSchemaHash() != getLatestSchema().getSchemaHash()) {
            // we have a new version
            schema_captured.push_back(possible_new_schema);
            new_schema_fetched = true;
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        std::string err_msg = "Aggregator Loader Manager cannot retrieve table definition for table: " + table_name;
        LOG(ERROR) << err_msg;
    }

    // no matter whether a new schema is fetched or not, we update the hash code's associated version, which is the
    // current latest version
    updateHashWithLatestSchemaVersion(hash_code);
}

/**
 * Note: this is for testing purpose, to allow update of the schema definition from the test.
 */
void TableSchemaUpdateTracker::updateHashMapping(size_t hash_in_message,
                                                 const nuclm::TableColumnsDescription& latest_schema) {
    schema_captured.push_back(latest_schema);
    updateHashWithLatestSchemaVersion(hash_in_message);
}

} // namespace nuclm
