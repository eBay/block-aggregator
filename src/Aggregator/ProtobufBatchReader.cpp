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

#include <Aggregator/ProtobufBatchReader.h>
#include <Serializable/ProtobufReader.h>
#include <Aggregator/BlockAddMissingDefaults.h>

#include "common/logging.hpp"
#include "monitor/metrics_collector.hpp"

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>

#include <boost/algorithm/string.hpp>
#include <string.h>

namespace nuclm {

namespace ErrorCodes {
extern const int COLUMN_DEFINITION_NOT_CORRECT;
extern const int FAILED_TO_PARSE_INSERT_QUERY;
extern const int FAILED_TO_DESERIALIZE_MESSAGE;
extern const int FAILED_TO_RECONSTRUCT_BLOCKS;
extern const int COLUMN_DEFINITION_NOT_MATCHED_WITH_SCHEMA;
} // namespace ErrorCodes

static DB::ASTPtr parseQuery(const char*& pos, const char* end, bool allow_multi_statements) {
    DB::ParserQuery parser(end);
    DB::ASTPtr res;

    DB::String message;
    res = DB::tryParseQuery(parser, pos, end, message, true, "", allow_multi_statements, 0, 0);

    if (!res) {
        LOG(ERROR) << "failed to parse query with error: " << message;
        return nullptr;
    }

    return res;
}

/**
 * Case 1 (Implicit Columns):
 *          insert into table values(?, ?, ?, ....?), that indicates that we are inserting full columns
 *
 * Case 2 (Explicit Columns):
 *          insert into table (`c1`, `c2`, ...,`ck`) values (?, ?, ?, ...,?) that indicates that we are inserting
 * certain selected columns or no single quote, or:
 *
 *          insert into table (c1, c2, ..., ck) values (?, ?, ?, ...,?)
 *
 * return a pair:
 *    (1) a list of column names, for Case 1, it is empty, and for Case 2, it is the number of the explicit columns
 * used. (2) the number of the ? placeholders, for Case 1, it is the number of all of the implicit columns specified
 * (ie., the number of "?" specified), and for Case 2, it is the number of the explicitly selected columns (i.e., the
 * number of "?" specified)
 *
 */
std::pair<std::vector<std::string>, size_t> ProtobufBatchReader::extractColumnNames(const std::string& sql_statement,
                                                                                    const std::string& table_name) {
    std::vector<std::string> columns_identified;
    size_t number_of_placeholders = 0;

    const char* begin = sql_statement.data();
    const char* end = begin + sql_statement.size();

    DB::ASTPtr ast = parseQuery(begin, end, true);
    if (ast == nullptr) {
        std::string err_msg = "Failed to parse query statement: " + sql_statement + " for table: " + table_name;
        LOG(ERROR) << err_msg;
        throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_PARSE_INSERT_QUERY);
    }

    if (ast != nullptr) {
        auto* insert = ast->as<DB::ASTInsertQuery>();

        if (insert == nullptr) {
            std::string err_msg =
                "Failed to parse the insert query statement: " + sql_statement + " for table: " + table_name;
            LOG(ERROR) << err_msg;
            throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_PARSE_INSERT_QUERY);
        } else {
            if (insert->columns != nullptr) {
                LOG_AGGRPROC(4) << "columns specified is not null...thus we have columns specified explicitly";

                for (const auto& identifier : insert->columns->children) {
                    std::string current_column_name = identifier->getColumnName();
                    LOG_AGGRPROC(4) << "column name extracted: " << current_column_name;
                    columns_identified.push_back(current_column_name);
                }
            } else {
                LOG_AGGRPROC(4) << "columns specified is null.. thus we have no columns specified explicitly";
            }

            // Also identify how many ? placeholders we have.
            std::string insert_data(insert->data, insert->end - insert->data + 1); // is this correct?
            std::vector<std::string> placeholders;
            boost::split(placeholders, insert_data, boost::is_any_of(","));
            number_of_placeholders = placeholders.size();
        }
    }

    return std::make_pair(columns_identified, number_of_placeholders);
}

/**
 * Based on the passed-in SQL statement's column specifications, return the corresponding column type/name pairs that
 * match the column specifications. If the SQL statement does not have specific column specifications, all of the
 * column type/name pairs from the table definition are returned.
 *
 * table_definition_followed to be true, if the per-insertion query statement does not have explicit column ordering.
 * table_definition_followed to be false, if the per-insertion query statement has its own explicit column ordering.
 * It could be that the columns are fully used and with the same ordering as the table definition. but this function
 * does not provide this checking, and table definition_followed still is false.
 *
 * default_columns_missing to be true, if the per-insertion query statement does not cover all of the default columns
   specified in the table definition.

 * if table_definition_followed to be true, then default_columns_missing is to false.
 *
 * if table_definition_followed to be false, the columns from the incoming message may not fully cover all of the
 columns
 * defined in the schema, and some of these un-covered columns may not have user-defined default expressions to be
 * associated with (in the case when sort-by-key columns are added). If this is the case,
 * columns_not_covered_no_deflexpress is set to be true.
 *
 * In the situation when we have dynamic schema update happening, the passed-in table definition will be the latest one
 * managed by the schema tracker, and the checking rules described above for static table definition are still followed.
 *
 */
ColumnTypesAndNamesTableDefinition
ProtobufBatchReader::determineColumnsDefinition(const std::string& sql_statement, bool& table_definition_followed,
                                                bool& default_columns_missing, bool& columns_not_covered_no_deflexpres,
                                                std::vector<size_t>& order_mapping,
                                                const TableColumnsDescription& table_definition) {
    std::string table_name = table_definition.getTableName();
    // TODO: Potential opportunity to improve performance by caching sql parsed columns.
    auto extracted_column_names = ProtobufBatchReader::extractColumnNames(sql_statement, table_name);

    ColumnTypesAndNamesTableDefinition columns_chosen_for_deserialization;
    // This is the implicit column insertion,
    if (extracted_column_names.first.empty()) {
        // Need to check how many "?" we have in the current insertion, to decide whether
        size_t number_of_placeholders = extracted_column_names.second;
        if (number_of_placeholders == table_definition.getFullColumnTypesAndNamesDefinitionCache().size()) {
            table_definition_followed = true;
            default_columns_missing = false;

            LOG_AGGRPROC(4) << " no explicit columns specified from: " + sql_statement + " for table: " + table_name;
            columns_chosen_for_deserialization = table_definition.getFullColumnTypesAndNamesDefinitionCache();
        } else {
            table_definition_followed = false;
            // for sure, default columns are missing as the incoming message's assumed schema is the backward compatible
            // one
            default_columns_missing = true;
            table_definition.getSubColumnTypesAndNamesDefinition(number_of_placeholders,
                                                                 columns_chosen_for_deserialization);
        }
    } else {
        table_definition_followed = false;
        default_columns_missing = false;
        size_t total_defaults_columns_involved = 0;
        LOG_AGGRPROC(4) << " obtain explicit columns specified from: " + sql_statement + " for table: " + table_name;
        if (extracted_column_names.first.size() != extracted_column_names.second) {
            std::string err_msg =
                "insert query does not have number of ? placeholders matched with number of explicit columns: " +
                sql_statement;
            LOG(ERROR) << err_msg;
            throw DB::Exception(err_msg, ErrorCodes::COLUMN_DEFINITION_NOT_MATCHED_WITH_SCHEMA);
        }

        // In normal situation, the explicit columns shown up in the insert statement may not cover all of the default
        // columns. In dynamic schema update situation,  if the explicit columns shown in the insert statement is from
        // the older schema definition and thus for sure that the total defaults columns missing is true, because the
        // missing columns in the insert statement is always be associated with a default expression, according to the
        // way that we require dynamic schema update to be performed from the application.
        size_t total_columns_count = 0;
        for (auto& t : extracted_column_names.first) {
            try {
                const TableColumnDescription& col_des = table_definition.getColumnDescription(t);
                if (col_des.column_default_description.column_kind == TableColumnKind::Default_Column) {
                    total_defaults_columns_involved++;
                }
                total_columns_count++;
            } catch (...) {
                std::string err_msg = "invalid insertion query statement: " + sql_statement +
                    " for table: " + table_name + " as column: " + t + " does not exist in columns definition";
                LOG(ERROR) << err_msg;
                throw DB::Exception(err_msg, ErrorCodes::COLUMN_DEFINITION_NOT_MATCHED_WITH_SCHEMA);
            }
        }

        // When we come here, total_columns_count = extracted_column_names.size(). But in the dynamic schema update
        // situation, the total_columns_count extracted from the insert query statement's explicit columns can be
        // smaller than the number of total columns defined in the latest table schema.
        if (total_defaults_columns_involved < table_definition.getSizeOfDefaultColumns()) {
            default_columns_missing = true;
        }

        // Then pick up the selected columns definition, following the orders specified in c1, c2, cn. In the situation
        // of dynamic schema update, for sure, these explicitly specified columns will be in the same subset of the new
        // schema.
        columns_chosen_for_deserialization =
            table_definition.getColumnTypesAndNamesDefinitionWithDefaultColumns(extracted_column_names.first);
        if (total_columns_count != columns_chosen_for_deserialization.size()) {
            std::string err_msg = "invalid insertion query statement: " + sql_statement + " for table: " + table_name +
                " with total columns identified: " + std::to_string(total_columns_count) +
                " not match with ordinary + default columns in table definition with size of: " +
                std::to_string(columns_chosen_for_deserialization.size());
            LOG(ERROR) << err_msg;
            throw DB::Exception(err_msg, ErrorCodes::COLUMN_DEFINITION_NOT_CORRECT);
        } else {
            LOG_AGGRPROC(4) << "explicit columns specified from query: " + sql_statement + " for table: " + table_name
                            << " match what has been specified in table schema";
        }
    }

    order_mapping = table_definition.getColumnToSequenceMapping(columns_chosen_for_deserialization);
    columns_not_covered_no_deflexpres =
        table_definition.checkColumnsNotCoveredAndWithoutDefaultExpression(order_mapping);

    return columns_chosen_for_deserialization;
}

/**
 * To check whether the per-insertion query statement has the same ordering of the columns as specified by the table
 * definition. Even when the user-specified columns are full columns, the explicit ordering of the columns can still be
 * different from the table definition.
 *
 * In the case of dynamic schema update, the two table definitions can be different, and thus the column sizes will be
 * different and thus the function always return false;
 */
static bool check_column_order_different(const ColumnTypesAndNamesTableDefinition& table_definition_specified,
                                         const ColumnTypesAndNamesTableDefinition& table_definition) {
    size_t used_columns_size = table_definition_specified.size();
    size_t expected_columns_size = table_definition.size();
    if (used_columns_size != expected_columns_size) {
        return true;
    }

    for (size_t column_index = 0; column_index < used_columns_size; column_index++) {
        std::string used_column_name = table_definition_specified[column_index].name;
        std::string expected_column_name = table_definition[column_index].name;
        if (used_column_name != expected_column_name) {
            return true;
        }
    }

    return false;
}

bool ProtobufBatchReader::read() {
    nucolumnar::aggregator::v1::SQLBatchRequest deserialized_batch_request;
    deserialized_batch_request.ParseFromString(message);

    LOG_AGGRPROC(4) << " total received debug message size: " << deserialized_batch_request.ByteSizeLong()
                    << " has content: " << deserialized_batch_request.DebugString();

    LOG_AGGRPROC(4) << "shard id deserialized: " << deserialized_batch_request.shard();
    LOG_AGGRPROC(4) << "table name deserialized: " << deserialized_batch_request.table();

    size_t hash_code = deserialized_batch_request.schema_hashcode();
    LOG_AGGRPROC(4) << "table schema hash value: " << hash_code;

    std::shared_ptr<SchemaTrackingMetrics> schema_tracking_metrics =
        MetricsCollector::instance().getSchemaTrackingMetrics();
    std::string table_name = schema_tracker->getLatestSchema().getTableName();
    schema_tracking_metrics->schema_tracking_service_schema_passed
        ->labels({{"table", table_name}, {"version", std::to_string(hash_code)}})
        .update(1);

    bool with_latest_schema = schema_tracker->checkHashWithLatestSchemaVersion(hash_code);
    bool new_schema_fetched = false;
    if (!with_latest_schema) {
        LOG_AGGRPROC(2) << "need to fetch latest schema due to new hash not seen at the current batch reader";
        schema_tracking_metrics->schema_tracking_not_with_latest_schema_total->labels({{"table", table_name}})
            .increment(1);

        // NOTE: schema version computation currently does not involve default column's expression
        schema_tracker->tryAddNewSchemaVersionForNewHash(hash_code, new_schema_fetched);
        LOG_AGGRPROC(4) << "new version of schema has been fetched or not: " << new_schema_fetched;
    } else {
        LOG_AGGRPROC(4) << "current schema version matches latest version";
    }

    // If the new schema is fetched, or current schema used for block construction is not the latest one
    // (this can happen, for example, in testing, that we force the schema to be updated using the protected method
    // of updateHashMapping(), rather than going through the regular tryAndNewSchemaVersionForNewHash() call.),
    // then we need to migrate the current still-under-construction block to fit to the new schema
    if (new_schema_fetched || !schema_tracker->currentSchemaUsedInBlockMatchesLatestSchema()) {
        // To Do: need to have a metric to show the migration
        if (new_schema_fetched) {
            LOG_AGGRPROC(2) << "new version schema is fetched for table: " << deserialized_batch_request.table()
                            << " thus block migration to latest schema is needed";
            schema_tracking_metrics->schema_tracking_new_schema_fetched_total->labels({{"table", table_name}})
                .increment(1);
        }
        if (!schema_tracker->currentSchemaUsedInBlockMatchesLatestSchema()) {
            LOG_AGGRPROC(2) << "current block construction associated schema is not latest for table: "
                            << deserialized_batch_request.table() << " thus block migration to latest schema is needed";
        }
        migrateBlockToMatchLatestSchema(block, schema_tracker->getLatestSchema(), context);
        schema_tracker->updateCurrentSchemaUsedInBlockWithLatestSchema();

        schema_tracking_metrics->schema_tracking_block_schema_migration_total->labels({{"table", table_name}})
            .increment(1);
    } else {
        LOG_AGGRPROC(4)
            << "new version schema is not fetched and current schema used for block construction is latest for table: "
            << deserialized_batch_request.table();
    }

    std::string latest_schema_hash = std::to_string(schema_tracker->getLatestSchema().getSchemaHash());
    schema_tracking_metrics->schema_tracking_current_schema_used
        ->labels({{"table", table_name}, {"version", latest_schema_hash}})
        .update(1);

    bool processing_result = read(schema_tracker->getLatestSchema(), deserialized_batch_request);
    return processing_result;
}

bool ProtobufBatchReader::read(const TableColumnsDescription& table_definition,
                               const nucolumnar::aggregator::v1::SQLBatchRequest& deserialized_batch_request) {
    bool processing_result = true;
    LOG_AGGRPROC(4) << "has nucolumnar encoding or not: " << deserialized_batch_request.has_nucolumnarencoding();
    const nucolumnar::aggregator::v1::SqlWithBatchBindings& deserialized_batch_bindings =
        deserialized_batch_request.nucolumnarencoding();

    // The sql statement will tell us whether:
    //    (1) the insertion is on all of the columns
    //    (2) all the insertion is on some of the columns
    LOG_AGGRPROC(4) << "insert related sql statement is: " << deserialized_batch_bindings.sql();
    size_t total_number_rows = deserialized_batch_bindings.batch_bindings_size();
    LOG_AGGRPROC(4) << "total number of the rows involved are: " << total_number_rows;

    // Get the per-insertion specific ordered columns definition.
    bool table_definition_followed = false;
    bool default_columns_missing = true;
    // The "true value" indicates that: the columns that are not covered in batched columns definition from incoming
    // message, do not have user-defined default expressions associated with.
    bool columns_not_covered_no_deflexpres = false;
    // Construct the column order_mapping for the returned columns definition also.
    std::vector<size_t> order_mapping;
    ColumnTypesAndNamesTableDefinition batched_columns_definition = determineColumnsDefinition(
        deserialized_batch_bindings.sql(), table_definition_followed, default_columns_missing,
        columns_not_covered_no_deflexpres, order_mapping, table_definition);

    LOG_AGGRPROC(4)
        << " columns not covered by those specified in incoming message have default expressions associated: "
        << (columns_not_covered_no_deflexpres ? "YES" : "NO");

    // The corresponding column deserializer based on the per-insertion specific ordered columns definition.
    ColumnSerializers columnSerializers = SerializationHelper::getColumnSerializers(batched_columns_definition);
    LOG_AGGRPROC(4) << "column serializer selected is: "
                    << SerializationHelper::strOfColumnSerializers(columnSerializers)
                    << " with table definition being followed: " << table_definition_followed
                    << " with default columns missing: " << default_columns_missing;

    // Get the block header definition based on the per-insertion ordered columns definition for the current batch.
    DB::Block current_sample_block = SerializationHelper::getBlockDefinition(batched_columns_definition);
    DB::MutableColumns current_columns = current_sample_block.cloneEmptyColumns();

    bool column_shuffled_needed = false;

    // Include both ordinary + default.
    // FullColumnTypesAndNamesDefinition from cache instead of rebuilding the definition each time.
    const ColumnTypesAndNamesTableDefinition& full_columns_definition =
        table_definition.getFullColumnTypesAndNamesDefinitionCache();
    // The columns can be full columns, but still being altered by the Client application. Thus, checking full size does
    // not change the shuffle or not decision.
    // The dynamic schema update can also introduce the situation that always, table_definition_followed is false, for
    // both implicit columns insert statement, and explicit columns insert statements.
    if (!table_definition_followed) {
        if (check_column_order_different(batched_columns_definition, full_columns_definition)) {
            column_shuffled_needed = true;
            LOG_AGGRPROC(4) << " column_shuffled_needed is: " << column_shuffled_needed;
            size_t column_index = 0;
            for (size_t mapping_result : order_mapping) {
                LOG_AGGRPROC(4) << " column index : " << column_index++ << " mapped to column: " << mapping_result;
            }
        } else {
            LOG_AGGRPROC(4) << " no column shuffled_needed ";
        }
    } else {
        LOG_AGGRPROC(4) << " table definition fully followed, thus no column shuffled_needed";
    }

    // Only some columns are populated in the full column definition, because of the incoming message to be
    // deserialized.
    bool columns_number_mismatched = false;
    size_t total_mismatched_rows_count = 0;
    try {
        for (size_t rindex = 0; rindex < total_number_rows; rindex++) {
            const nucolumnar::aggregator::v1::DataBindingList& row = deserialized_batch_bindings.batch_bindings(rindex);

            size_t number_of_columns = row.values().size();
            size_t expected_number_of_columns = columnSerializers.size();
            LOG_AGGRPROC(4) << "total number of columns available in received row: " << rindex
                            << " is: " << number_of_columns;
            LOG_AGGRPROC(4) << "total number of columns expected from the defined schema in received row: " << rindex
                            << " is: " << expected_number_of_columns;

            // still let the deserializer to continue, but log the detected error.
            if (number_of_columns != expected_number_of_columns) {
                columns_number_mismatched = true;
                total_mismatched_rows_count++;
                std::string err_msg = "in row: " + std::to_string(rindex) +
                    " number of available columns: " + std::to_string(number_of_columns) +
                    " does not match number of expected columns: " + std::to_string(expected_number_of_columns) +
                    " according to schema defined for: " + table_definition.getTableName();
                LOG(ERROR) << err_msg;
            }

            // The number of the columns need to follow what columnSerializer has provided.
            for (size_t cindex = 0; cindex < expected_number_of_columns; cindex++) {
                const nucolumnar::datatypes::v1::ValueP& row_value = row.values(cindex);
                ProtobufReader reader(row_value);

                // need to invoke the deserializer: SerializableDataTypePtr
                LOG_AGGRPROC(4) << "to deserialize column: " << cindex;
                bool row_added;
                columnSerializers[cindex]->deserializeProtobuf(*current_columns[cindex], reader, true, row_added);
                if (!row_added) {
                    processing_result = false;
                    LOG(ERROR) << "protobuf-reader deserialization for column index: " << cindex << " and column name "
                               << current_columns[cindex]->getName() + " failed";
                } else {
                    LOG_AGGRPROC(4) << "finish deserialize column: " << cindex;
                }

                total_bytes_processed += reader.getTotalBytesRead();

                if (!column_shuffled_needed) {
                    LOG_AGGRPROC(4) << "row: " << rindex << " column:  " << cindex
                                    << " protobuf-reader processes: " << reader.getTotalBytesRead();
                } else {
                    LOG_AGGRPROC(4) << "row: " << rindex << " column:  " << cindex
                                    << " protobuf-reader processes: " << reader.getTotalBytesRead();
                }
            }
        }
    } catch (const DB::Exception& ex) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        std::string err_msg =
            "DB::Exception captured. Failed to de-serialize received message with total number of rows: " +
            std::to_string(total_number_rows) + " for table: " + table_definition.getTableName();
        LOG(ERROR) << err_msg;
        throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_DESERIALIZE_MESSAGE);
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        std::string err_msg = "Exception other than DB::Exception captured. Failed to de-serialize received message "
                              "with total number of rows: " +
            std::to_string(total_number_rows) + " for table: " + table_definition.getTableName();
        LOG(ERROR) << err_msg;
        throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_DESERIALIZE_MESSAGE);
    }

    LOG_AGGRPROC(4) << "totally rows: " << total_number_rows << " processed with bytes: " << total_bytes_processed
                    << " compared to passed in message with bytes: " << message.size();

    // if columns number mismatched, throw exception.
    if (columns_number_mismatched) {
        size_t expected_number_of_columns = columnSerializers.size();
        std::string err_msg =
            " number of available columns in the received rows does not match number of expected columns: " +
            std::to_string(expected_number_of_columns) +
            " according to schema defined for: " + table_definition.getTableName();
        +" total number of rows identified such mismatch: " + std::to_string(total_mismatched_rows_count);
        LOG(ERROR) << err_msg;
        throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_DESERIALIZE_MESSAGE);
    }

    try {
        if (!default_columns_missing) {
            // The block is the accumulated bock up to now, which we need to make sure that the result is fully
            // populated by following the latest schema version.
            DB::MutableColumns block_columns = block.mutateColumns();

            // This is the high-performance path that we do not fill missing columns, in the case when we have
            // columns_not_covered_no_deflexpress to be false, as no further processing needed to fill missing columns
            // with user-defined default expressions or system-provided default values
            if (!columns_not_covered_no_deflexpres) {
                LOG_AGGRPROC(4) << "no default columns are missing in incoming message, and columns not covered in "
                                   "table definition do not have default expressions";
                if (!column_shuffled_needed) {
                    LOG_AGGRPROC(4) << "no column shuffle needed at the protobuf reader";
                    // no mapping of columns changed.
                    size_t number_of_columns = block_columns.size();
                    for (size_t column_index = 0; column_index < number_of_columns; column_index++) {
                        LOG_AGGRPROC(4) << " name of column to be inserted: "
                                        << current_columns[column_index]->getName()
                                        << " with column size: " << current_columns[column_index]->size();
                        block_columns[column_index]->insertRangeFrom(*current_columns[column_index], 0,
                                                                     current_columns[column_index]->size());
                    }
                } else {
                    LOG_AGGRPROC(4) << "column shuffle is needed at the protobuf reader";
                    // change on the columns mapping happens, which leads to some block_columns to be empty
                    // NOTE: we need to test how clickhouse server fills out the missing columns or missing rows.
                    size_t number_of_columns = batched_columns_definition.size();
                    LOG_AGGRPROC(4) << " current batched columns have number of columns: " << number_of_columns;

                    for (size_t column_index = 0; column_index < number_of_columns; column_index++) {
                        size_t mapped_index = order_mapping[column_index];
                        LOG_AGGRPROC(4) << " name of column to be inserted: "
                                        << current_columns[column_index]->getName()
                                        << " with column size: " << current_columns[column_index]->size()
                                        << " mapping is from: " << column_index << " to " << mapped_index;
                        block_columns[mapped_index]->insertRangeFrom(*current_columns[column_index], 0,
                                                                     current_columns[column_index]->size());
                    }
                }

                /// Finally, update to the block holder.
                block.setColumns(std::move(block_columns));

            } else {
                // Perform column filling based on the system-default expression for those columns that are missing
                // in incoming message and these columns do not have explicit default expressions
                populateMissingColumnsByFillingDefaults(table_definition, batched_columns_definition, current_columns,
                                                        block, context);
            }
        } else {
            // Perform column filling for the columns that have the user-provided explicit default expression, or
            // otherwise, use the system-default expression for the columns that do not have user-provided explicit
            // default expression.
            populateMissingColumnsByFillingDefaults(table_definition, batched_columns_definition, current_columns,
                                                    block, context);
        }

        total_rows_processed += total_number_rows;
    } catch (const DB::Exception& ex) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;

        std::string err_msg = "Failed to construct block from deserialized message with total number of rows: " +
            std::to_string(total_number_rows) + " for table: " + table_definition.getTableName();
        throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_RECONSTRUCT_BLOCKS);
    }

    return processing_result;
}

void ProtobufBatchReader::populateMissingColumnsByFillingDefaults(
    const TableColumnsDescription& table_definition,
    const ColumnTypesAndNamesTableDefinition& batched_columns_definition, DB::MutableColumns& current_columns,
    DB::Block& block_under_transformation, DB::ContextMutablePtr transformation_context) {
    LOG_AGGRPROC(4) << "missing columns encountered, to fix by filling default values with user/system-provided "
                       "default expressions";
    //(1) prepare for the required_columns and to populate the current_columns
    DB::NamesAndTypesList required_columns(block_under_transformation.getNamesAndTypesList());

    // the columns that have default expressions provided by the user-defined table definition.
    const DB::ColumnDefaults& column_defaults = table_definition.getColumnDefaultsCache();
    LOG_AGGRPROC(4) << "number of column defaults identified is: " << column_defaults.size();

    DB::MutableColumns block_columns = block_under_transformation.mutateColumns();

    if (CVLOG_IS_ON(VMODULE_AGGR_PROCESSOR, 4)) {
        for (const std::pair<std::string, DB::ColumnDefault> column_default : column_defaults) {
            LOG_AGGRPROC(4) << "column name is: " << column_default.first;
            if (column_default.second.kind == DB::ColumnDefaultKind::Default) {
                LOG_AGGRPROC(4) << " column default kind is: "
                                << " default column";
            } else if (column_default.second.kind == DB::ColumnDefaultKind::Materialized) {
                LOG_AGGRPROC(4) << " column default kind is: "
                                << " materialized column";
            } else if (column_default.second.kind == DB::ColumnDefaultKind::Alias) {
                LOG_AGGRPROC(4) << " column default kind is: "
                                << " alias column";
            }

            //          {
            //              DB::ASTPtr expression_ptr = column_default.second.expression;
            //              std::cout << std::endl;
            //              DB::formatAST(*expression_ptr, std::cout);
            //              std::cout << std::endl << std::endl;
            //          }
        }
    }

    //(2) Call the missing-columns processor to fill-up the missing columns.
    // the following is required to get the header and only afterwards to get the column.
    DB::Block current_constructed_block = SerializationHelper::getBlockDefinition(batched_columns_definition);
    current_constructed_block.setColumns(std::move(current_columns));

    const DB::ColumnsDescription& required_columns_definition = table_definition.getNativeDBColumnsDescriptionCache();
    DB::Block processed_columns = blockAddMissingDefaults(current_constructed_block, required_columns,
                                                          required_columns_definition, transformation_context);
    DB::MutableColumns processed_mutable_columns = processed_columns.mutateColumns();
    // finally, update to the block holder, following the table ordered used in the required_columns,
    size_t number_of_columns = block_columns.size();
    for (size_t column_index = 0; column_index < number_of_columns; column_index++) {
        LOG_AGGRPROC(4) << " name of column to be inserted: " << processed_mutable_columns[column_index]->getName()
                        << " with column size: " << processed_mutable_columns[column_index]->size();
        block_columns[column_index]->insertRangeFrom(*processed_mutable_columns[column_index], 0,
                                                     processed_mutable_columns[column_index]->size());
    }

    block_under_transformation.setColumns(std::move(block_columns));
}

void ProtobufBatchReader::migrateBlockToMatchLatestSchema(DB::Block& current_block,
                                                          const TableColumnsDescription& latest_schema,
                                                          DB::ContextMutablePtr migration_context) {
    // Construct an empty block from the latest schema,
    DB::Block block_from_latest_schema =
        SerializationHelper::getBlockDefinition(latest_schema.getFullColumnTypesAndNamesDefinitionCache());
    // DB::MutableColumns current_columns = sample_block_from_latest_schema.cloneEmptyColumns();
    DB::NamesAndTypesList required_columns(block_from_latest_schema.getNamesAndTypesList());
    // const DB::ColumnDefaults& column_defaults = latest_schema.getColumnDefaultsCache();
    const DB::ColumnsDescription& required_columns_definition = latest_schema.getNativeDBColumnsDescriptionCache();
    LOG_AGGRPROC(4) << "required columns definition has number of columns: " << required_columns_definition.size();

    DB::Block migrated_block =
        blockAddMissingDefaults(current_block, required_columns, required_columns_definition, migration_context);

    // Change current block's definition and then swap it with the one that has been populated with the latest schema
    LOG_AGGRPROC(4) << "migrated block has number of columns: " << migrated_block.columns();
    current_block.clear();
    current_block = SerializationHelper::getBlockDefinition(latest_schema.getFullColumnTypesAndNamesDefinitionCache());
    current_block.swap(migrated_block);
}

} // namespace nuclm
