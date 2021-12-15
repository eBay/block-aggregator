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

#include <Aggregator/TableColumnsDescription.h>
#include <Aggregator/SerializationHelper.h>
#include <Aggregator/TableSchemaHash.h>

#include <Columns/ColumnString.h>
#include <Common/assert_cast.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>

#include <glog/logging.h>

#include <sstream>
#include <algorithm>

namespace nuclm {

namespace ErrorCodes {
extern const int NO_COLUMN_FOUND_IN_TABLE;
extern const int BAD_TABLE_DEFINITION_RETRIEVED;
extern const int FAILED_TO_PARSE_INSERT_QUERY;
extern const int TABLE_DEFINITION_NOT_FOUND;
} // namespace ErrorCodes

static DB::ASTPtr build_ast_default_expression(DB::ParserExpression& parser, const std::string& expression) {
    DB::ASTPtr ast =
        DB::parseQuery(parser, expression.data(), expression.data() + expression.size(), "default expression", 0, 0);
    return ast;
}

void TableColumnsDescription::addColumnDescription(const TableColumnDescription& column_description) {
    columns_description.push_back(column_description);
    hashed_description.insert({column_description.column_name, column_description});

    // Update full_column_types_and_names_def_cache by recontructing the full columns definition
    full_column_types_and_names_def_cache = getFullColumnTypesAndNamesDefinition();
    // Update column_defaults_cache, by reconstructing the full column defaults definition
    column_defaults_cache = getColumnDefaults();
    // Update DB native columns definition by reconstructing the full DB native columns definition
    db_columns_description_cache = getNativeDBColumnsDescription();
    // Update schema hash
    table_schema_hash = computeTableSchemaHash();
    // build column to sequence mapping
    buildColumnToSequenceMapping();
}

size_t TableColumnsDescription::computeTableSchemaHash() const { return TableSchemaHash::hash_concat(*this); }

size_t TableColumnsDescription::getSizeOfDefaultColumns() const {
    size_t count = 0;
    for (auto& description : columns_description) {
        if (description.column_default_description.column_kind == TableColumnKind::Default_Column) {
            count++;
        }
    }

    return count;
}

size_t TableColumnsDescription::getSizeOfOrdinaryColumns() const {
    size_t count = 0;
    for (auto& description : columns_description) {
        if (description.column_default_description.column_kind == TableColumnKind::Ordinary_Column) {
            count++;
        }
    }

    return count;
}

bool TableColumnsDescription::queryIncludeAllDefaultColumns(const std::vector<std::string>& columns_in_insert) const {
    size_t columns_covered = 0; // for defaults;
    for (auto& column_name : columns_in_insert) {
        const TableColumnDescription& description = getColumnDescription(column_name);
        if (description.column_default_description.column_kind == TableColumnKind::Default_Column) {
            columns_covered++;
        }
    }

    bool result = (columns_covered == getSizeOfDefaultColumns());
    return result;
}

ColumnTypesAndNamesTableDefinition TableColumnsDescription::getColumnTypesAndNamesDefinitionWithDefaultColumns(
    const std::vector<std::string>& columns_in_insert) const {
    ColumnTypesAndNamesTableDefinition columns_definition;
    for (auto& column_name : columns_in_insert) {
        const TableColumnDescription& description = getColumnDescription(column_name);
        if ((description.column_default_description.column_kind == TableColumnKind::Default_Column) ||
            (description.column_default_description.column_kind == TableColumnKind::Ordinary_Column)) {
            // Copy by parsed type instead raw string type to avoid parsing each time.
            columns_definition.push_back({description.column_type, description.column_type_ptr,
                                          description.column_type_serializer, description.column_name});
        }
    }

    return columns_definition;
}

ColumnTypesAndNamesTableDefinition TableColumnsDescription::getFullColumnTypesAndNamesDefinition() const {
    ColumnTypesAndNamesTableDefinition columns_definition;

    for (auto& description : columns_description) {
        if ((description.column_default_description.column_kind == TableColumnKind::Default_Column) ||
            (description.column_default_description.column_kind == TableColumnKind::Ordinary_Column)) {
            // Copy by parsed type instead raw string type to avoid parsing each time.
            columns_definition.push_back({description.column_type, description.column_type_ptr,
                                          description.column_type_serializer, description.column_name});
        }
    }

    return columns_definition;
}

void TableColumnsDescription::getSubColumnTypesAndNamesDefinition(
    size_t first_n_columns, ColumnTypesAndNamesTableDefinition& sub_columns_definition) const {
    size_t counter = 0;
    for (auto& description : columns_description) {
        if ((description.column_default_description.column_kind == TableColumnKind::Default_Column) ||
            (description.column_default_description.column_kind == TableColumnKind::Ordinary_Column)) {
            // Copy by parsed type instead raw string type to avoid parsing each time.
            sub_columns_definition.push_back({description.column_type, description.column_type_ptr,
                                              description.column_type_serializer, description.column_name});
        }
        counter++;
        if (counter == first_n_columns) {
            break;
        }
    }
}

const TableColumnDescription& TableColumnsDescription::getColumnDescription(const std::string& column_name) const {
    auto result = hashed_description.find(column_name);
    if (result != hashed_description.end()) {
        return (*result).second;
    } else {
        throw DB::Exception("No column having name: " + column_name + " in table",
                            ErrorCodes::NO_COLUMN_FOUND_IN_TABLE);
    }
}

std::vector<size_t> TableColumnsDescription::getColumnToSequenceMapping(
    const ColumnTypesAndNamesTableDefinition& sub_columns_definition) const {
    std::vector<size_t> result;
    for (auto& column : sub_columns_definition) {
        auto sequence_found = column_to_sequence_mapping.find(column.name);
        if (sequence_found != column_to_sequence_mapping.end()) {
            result.push_back((*sequence_found).second);
        } else {
            throw DB::Exception("No column having name: " + column.name + " found its sequence in table definition",
                                ErrorCodes::NO_COLUMN_FOUND_IN_TABLE);
        }
    }

    return result;
}

bool TableColumnsDescription::checkColumnsNotCoveredAndWithoutDefaultExpression(
    const std::vector<size_t>& order_mapping) const {
    bool check_result = false;
    // Sort the mapping on the close
    std::vector<size_t> cloned_mapping = order_mapping;
    std::sort(cloned_mapping.begin(), cloned_mapping.end());
    std::vector<int> diff;
    std::set_difference(column_sequence.begin(), column_sequence.end(), cloned_mapping.begin(), cloned_mapping.end(),
                        std::inserter(diff, diff.begin()));

    // Pick up the columns that are in the diff set, find whether each column is with Nullable but no default expression
    for (auto chosenColumnIndex : diff) {
        const TableColumnDescription& column_desc = columns_description[chosenColumnIndex];
        if (column_desc.column_default_description.expression.empty()) {
            check_result = true;
            break;
        }
    }

    return check_result;
}

void TableColumnsDescription::buildColumnsDescription(AggregatorLoader& loader) {

    // expected table schema retrieval;
    ColumnTypesAndNamesTableDefinition expected_table_definition{
        {"String", "name"},           {"String", "type"},
        {"String", "default_type"},   {"String", "default_expression"},
        {"String", "comment"},        {"String", "codec_expression"},
        {"String", "ttl_expression"},
    };

    try {
        DB::Block query_result;

        loader.init();
        bool status = loader.getTableDefinition(table_name, query_result);

        // the block header has the definition of:
        // name String String(size = 3), type String String(size = 3), default_type String String(size = 3),
        //       default_expression String String(size = 3), comment String String(size = 3),
        //       codec_expression String String(size = 3), ttl_expression String String(size = 3)
        if (status) {
            // header definition for the table definition block:
            const DB::ColumnsWithTypeAndName& columns_with_type_and_name = query_result.getColumnsWithTypeAndName();
            int column_index = 0;
            for (auto& p : columns_with_type_and_name) {
                LOG_AGGRPROC(4) << "column index: " << column_index << " column type: " << p.type->getName()
                                << " column name: " << p.name;

                if (expected_table_definition[column_index].type_name != p.type->getName()) {
                    std::string err_msg = "Table definition schema returned for table: " + table_name +
                        " with column: " + std::to_string(column_index) +
                        " does not have column type: " + expected_table_definition[column_index].type_name +
                        " matched with " + p.type->getName();
                    LOG(ERROR) << err_msg;
                    throw DB::Exception(err_msg, ErrorCodes::BAD_TABLE_DEFINITION_RETRIEVED);
                }

                if (expected_table_definition[column_index].name != p.name) {
                    std::string err_msg = "Table definition schema returned for table: " + table_name +
                        " with column: " + std::to_string(column_index) +
                        " does not have column name: " + expected_table_definition[column_index].name +
                        " matched with " + p.name;
                    LOG(ERROR) << err_msg;
                    throw DB::Exception(err_msg, ErrorCodes::BAD_TABLE_DEFINITION_RETRIEVED);
                }

                column_index++;
            }

            // We only care about the first four columns
            const DB::ColumnWithTypeAndName& header_column_0 = columns_with_type_and_name[0]; // column name
            const DB::ColumnWithTypeAndName& header_column_1 = columns_with_type_and_name[1]; // column type;
            const DB::ColumnWithTypeAndName& header_column_2 = columns_with_type_and_name[2]; // default type.
            const DB::ColumnWithTypeAndName& header_column_3 = columns_with_type_and_name[3]; // default expression;

            size_t rows_in_column0 = header_column_0.column->size();
            size_t rows_in_column1 = header_column_1.column->size();
            size_t rows_in_column2 = header_column_2.column->size();
            size_t rows_in_column3 = header_column_3.column->size();

            LOG_AGGRPROC(4) << "number of rows in column 0: " << rows_in_column0;
            LOG_AGGRPROC(4) << "number of rows in column 1: " << rows_in_column1;
            LOG_AGGRPROC(4) << "number of rows in column 2: " << rows_in_column2;
            LOG_AGGRPROC(4) << "number of rows in column 3: " << rows_in_column3;

            if ((rows_in_column0 != rows_in_column1) || (rows_in_column0 != rows_in_column2) ||
                (rows_in_column0 != rows_in_column3)) {
                std::string err_msg = "Table definition schema returned for table: " + table_name +
                    " does not have the same number of the rows  in different columns " +
                    " column 0: " + std::to_string(rows_in_column0) + " column 1: " + std::to_string(rows_in_column1) +
                    " column 2: " + std::to_string(rows_in_column2) + " column 3: " + std::to_string(rows_in_column3);
                LOG(ERROR) << err_msg;
                throw DB::Exception(err_msg, ErrorCodes::BAD_TABLE_DEFINITION_RETRIEVED);
            }

            try {
                DB::MutableColumns columns = query_result.mutateColumns();

                // NOTE: to test, do we always have 4 rows in each column?
                // We only care about the first four columns
                auto& column_string_0 = assert_cast<DB::ColumnString&>(*columns[0]);
                auto& column_string_1 = assert_cast<DB::ColumnString&>(*columns[1]);
                auto& column_string_2 = assert_cast<DB::ColumnString&>(*columns[2]);
                auto& column_string_3 = assert_cast<DB::ColumnString&>(*columns[3]);

                for (size_t i = 0; i < rows_in_column0; i++) {
                    std::string column_name = column_string_0.getDataAt(i).toString();
                    std::string column_type = column_string_1.getDataAt(i).toString();
                    std::string default_type = column_string_2.getDataAt(i).toString();
                    std::string default_expression = column_string_3.getDataAt(i).toString();

                    LOG_AGGRPROC(4) << " Table Definition retrieved, Column Name: " << column_name
                                    << " Column Type: " << column_type
                                    << " default type (can be empty): " << default_type
                                    << " default expression (can be empty): " << default_expression;
                    if (default_type.empty()) {
                        TableColumnDescription column_description(column_name, column_type);
                        addColumnDescription(column_description);
                    } else {
                        ColumnDefaultDescription default_description(default_type, default_expression);
                        TableColumnDescription column_description(column_name, column_type, default_description);
                        addColumnDescription(column_description);
                    }
                }
            } catch (const DB::Exception& ex) {
                // To capture exception detail thrown from column definition's construction, for example, when a
                // data type is not supported.
                LOG(ERROR) << DB::getCurrentExceptionMessage(true);
                auto code = DB::getCurrentExceptionCode();
                LOG(ERROR) << "with exception return code: " << code;

                std::string err_msg = "Failed to retrieve table definition for table: " + table_name;
                throw DB::Exception(err_msg, ErrorCodes::BAD_TABLE_DEFINITION_RETRIEVED);
            }
        } else {
            std::string err_msg = "Failed to retrieve table definition for table: " + table_name;
            throw DB::Exception(err_msg, ErrorCodes::TABLE_DEFINITION_NOT_FOUND);
        }
    } catch (...) {
        LOG(ERROR) << DB::getCurrentExceptionMessage(true);
        auto code = DB::getCurrentExceptionCode();

        LOG(ERROR) << "with exception return code: " << code;
        throw;
    }
}

std::string TableColumnsDescription::str() const {
    std::stringstream description;
    size_t column_index = 0;

    for (auto column_description = columns_description.begin(); column_description != columns_description.end();
         ++column_description) {
        if (std::distance(column_description, columns_description.end()) == 1) {
            description << " column: " << column_index++ << " " << (*column_description).str();
        } else {
            description << " column: " << column_index++ << "  " << (*column_description).str() << ",";
        }
    }

    return description.str();
}

DB::ColumnDefaults TableColumnsDescription::getColumnDefaults() const {
    DB::ColumnDefaults res;
    DB::ParserExpression expr_parser;
    for (auto& description : columns_description) {
        if (description.column_default_description.column_kind == TableColumnKind::Default_Column) {
            DB::ColumnDefault column_default;
            column_default.kind = DB::ColumnDefaultKind::Default;
            column_default.expression =
                build_ast_default_expression(expr_parser, description.column_default_description.expression);

            if (column_default.expression == nullptr) {
                std::string err_msg =
                    "Failed to parse default expression: " + description.column_default_description.expression +
                    " for default column name: " + description.column_name + " in table: " + table_name;
                LOG(ERROR) << err_msg;
                throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_PARSE_INSERT_QUERY);
            }

            res.insert(std::make_pair(description.column_name, column_default));
        }
    }
    return res;
}

DB::ColumnsDescription TableColumnsDescription::getNativeDBColumnsDescription() const {
    DB::ColumnsDescription db_columns_description;
    DB::ParserExpression expr_parser;
    for (auto& description : columns_description) {
        DB::ColumnDescription db_column_description;
        db_column_description.name = description.column_name;
        db_column_description.type = description.column_type_ptr;

        DB::ColumnDefault column_default;
        if (description.column_default_description.column_kind == TableColumnKind::Default_Column) {
            column_default.expression =
                build_ast_default_expression(expr_parser, description.column_default_description.expression);
            if (column_default.expression == nullptr) {
                std::string err_msg =
                    "Failed to parse default expression: " + description.column_default_description.expression +
                    " for default column name: " + description.column_name + " in table: " + table_name;
                LOG(ERROR) << err_msg;
                throw DB::Exception(err_msg, ErrorCodes::FAILED_TO_PARSE_INSERT_QUERY);
            }
        }
        db_column_description.default_desc = column_default;

        db_columns_description.add(db_column_description);
    }

    return db_columns_description;
}

void TableColumnsDescription::buildColumnToSequenceMapping() {
    size_t column_index = 0;
    column_sequence.clear();
    column_to_sequence_mapping.clear();

    for (auto& description : columns_description) {
        column_to_sequence_mapping.insert({description.column_name, column_index});
        column_sequence.push_back(column_index);
        column_index++;
    }
}

} // namespace nuclm
