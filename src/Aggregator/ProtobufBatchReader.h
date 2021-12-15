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
#include <Aggregator/SerializationHelper.h>
#include <Aggregator/TableSchemaUpdateTracker.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>

#include <nucolumnar/aggregator/v1/nucolumnaraggregator.pb.h>
#include <nucolumnar/datatypes/v1/columnartypes.pb.h>

#include <string>

namespace nuclm {

class ProtobufBatchReader {
  public:
    /**
     * To read out of the message that encodes the multi-rows.
     * @param message to be de-serialized
     * @param block the existing block that has already held the existing de-serialized data, and will be updated
     * as part of the de-serialization.
     * @param sample_block the block that contains only the definition.
     *
     */
    ProtobufBatchReader(const std::string& message_, TableSchemaUpdateTrackerPtr schema_tracker_, DB::Block& block_,
                        DB::ContextMutablePtr context_) :
            total_rows_processed(0),
            total_bytes_processed(0),
            message(message_),
            block(block_),
            context(context_),
            schema_tracker(schema_tracker_) {}

    ~ProtobufBatchReader() = default;

    // To perform the deserialization on the received message
    bool read();

    // to perform actual deserialization based on the latest table schema and the incoming message.
    bool read(const TableColumnsDescription& table_definition,
              const nucolumnar::aggregator::v1::SQLBatchRequest& deserialized_batch_request);

    /**
     * To populate the columns that are not in the serialized Kafka message,
     * (1) by using the user-provided column default expression,
     * (2) or using the system-default expression if the user-provided column default expression is not available

     * @param table_definition the table definition used for the batch reader
     * @param batched_columns_definition the subset of the table definitions for the columns that show up in the
     incoming
     * kafka message
     * @param current_columns the columns that have been constructed from the incoming message.
     *
     * @param block_under_transformation the candidate block that needs to  be populated with missing columns.
     *
     * @param transformation_context the context used for transformation.
     */
    static void
    populateMissingColumnsByFillingDefaults(const TableColumnsDescription& table_definition,
                                            const ColumnTypesAndNamesTableDefinition& batched_columns_definition,
                                            DB::MutableColumns& current_columns, DB::Block& block_under_transformation,
                                            DB::ContextMutablePtr transformation_context);

    // To perform missing column values filling for current block, to satisfy the latest schema. As a result of this
    // call, current_block's column definitions get changed to follow the new schema.
    static void migrateBlockToMatchLatestSchema(DB::Block& current_block, const TableColumnsDescription& latest_schema,
                                                DB::ContextMutablePtr migration_context);

    /**
     * Based on the passed-in SQL statement's column specification, return the corresponding column type/name pairs that
     * match the column specification. If the SQL statement does not have specific column specifications, then all of
     * the column t ype/name pairs from the table definition is returned.
     *
     * table_definition_followed to be true, if the per-insertion query statement does not have explicit column
     * ordering. table_definition_followed to be false, if the per-insertion query statement has its own explicit column
     * ordering.
     *
     * default_columns_missing to be true, if the per-insertion query statement does not cover all of the default
     * columns specified in the table definition.
     *
     * columns_not_covererd_no_deflexpres is about the following situation due to a sort-by-key column in ClickHouse
     * cannot have user-defined default expression attached to at the time the sort-by-key clause is defined. Therefore,
     * a user-application that still does not have schema being update, will have its columns with the previous version
     * schema. So the table schema held at the Aggregator will have the columns that are not covered by the application,
     * that is, the newly added columns, and some of these newly added columns can potentially have no user-defined
     * default expressions, if some of these newly added columns are in the sort-by-key expression.
     *
     * When this variable is true, it means that: we have found columns not covered by the application and some of
     * these columns have no default expressions defined.
     *
     * The order_mapping of the constructed columns definition is part of the return also.
     *
     * TODo: make comment on schema being updated
     *
     */
    ColumnTypesAndNamesTableDefinition
    determineColumnsDefinition(const std::string& sql_statement, bool& table_definition_followed,
                               bool& default_columns_missing, bool& columns_not_covered_no_deflexpres,
                               std::vector<size_t>& order_mapping, const TableColumnsDescription& table_definition);

    size_t getRowsProcessed() { return total_rows_processed; }

    size_t getBytesProcessed() { return total_bytes_processed; }

    static std::pair<std::vector<std::string>, size_t> extractColumnNames(const std::string& sql_statement,
                                                                          const std::string& table_name);

  private:
    size_t total_rows_processed;
    size_t total_bytes_processed;

    const std::string& message;
    DB::Block& block;
    DB::ContextMutablePtr context;

    // it holds multiple versions of the schemas seen in the life-time of the buffer.
    TableSchemaUpdateTrackerPtr schema_tracker;
};

} // namespace nuclm
