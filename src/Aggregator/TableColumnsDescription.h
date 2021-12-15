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

#include <Aggregator/AggregatorLoader.h>
#include <Aggregator/SerializationHelper.h>
#include <Serializable/SerializableDataTypeFactory.h>

#include <Storages/ColumnDefault.h>
#include <Common/Exception.h>
#include <Core/Block.h>

#include <boost/functional/hash.hpp>
#include <vector>
#include <unordered_map>
#include <string>
#include <sstream>

namespace nuclm {

enum TableColumnKind { Ordinary_Column, Default_Column, Materialized_Column, Alias_Column };

struct ColumnDefaultDescription {
    TableColumnKind column_kind;
    std::string expression; // if the column is one of {default, materialized, alias}.

    ColumnDefaultDescription() : column_kind(TableColumnKind::Ordinary_Column), expression{} {}

    ColumnDefaultDescription(const std::string& default_type, const std::string& expression_) :
            expression(expression_) {
        if ((default_type == "default") || (default_type == "DEFAULT")) {
            column_kind = TableColumnKind::Default_Column;
        } else if ((default_type == "materialized") || (default_type == "MATERIALIZED")) {
            column_kind = TableColumnKind::Materialized_Column;
        } else if ((default_type == "alias") || (default_type == "ALIAS")) {
            column_kind = TableColumnKind::Alias_Column;
        } else {
            column_kind = TableColumnKind ::Ordinary_Column;
        }
    }

    ColumnDefaultDescription& operator=(const ColumnDefaultDescription& other) {
        if (&other != this) {
            column_kind = other.column_kind;
            expression = other.expression;
        }

        return *this;
    }

    ColumnDefaultDescription(const ColumnDefaultDescription& other) :
            column_kind(other.column_kind), expression(other.expression) {}
};

struct TableColumnDescription {
    std::string column_name;
    std::string column_type;

    ColumnDefaultDescription column_default_description; // can be empty.

    DB::DataTypePtr column_type_ptr;
    SerializableDataTypePtr column_type_serializer;

    TableColumnDescription(const std::string& column_name_, const std::string& column_type_) :
            column_name(column_name_),
            column_type(column_type_),
            column_default_description{},
            column_type_ptr(DB::DataTypeFactory::instance().get(column_type_)),
            column_type_serializer(SerializableDataTypeFactory::instance().get(column_type_)) {}

    TableColumnDescription(const std::string& column_name_, const std::string& column_type_,
                           const ColumnDefaultDescription& column_default_description_) :
            column_name(column_name_),
            column_type(column_type_),
            column_default_description(column_default_description_),
            column_type_ptr(DB::DataTypeFactory::instance().get(column_type_)),
            column_type_serializer(SerializableDataTypeFactory::instance().get(column_type_)) {}

    std::string str() const {
        std::stringstream description;
        description << "(";
        description << "name: " << column_name << ", "
                    << " type: " << column_type << ", ";
        if (column_default_description.column_kind == TableColumnKind::Default_Column) {
            description << "default type: "
                        << "DEFAULT"
                        << ", " << column_default_description.expression;
        } else if (column_default_description.column_kind == TableColumnKind::Materialized_Column) {
            description << " default type: "
                        << "MATERIALIZED"
                        << ", " << column_default_description.expression;
        } else if (column_default_description.column_kind == TableColumnKind::Alias_Column) {
            description << " default type: "
                        << "ALIAS"
                        << ", " << column_default_description.expression;
        } else {
            description << " default type: "
                        << "<ordinary>"
                        << ", "
                        << "<empty expression>";
        }

        description << ")";

        return description.str();
    }

    // This call will be invoked from TableSchema.hash(.) call. But since the Boost hash computation result is platform
    // dependent, we use TableSchema.hash_concat(.) call instead to get the platform independent hash result.
    friend size_t hash_value(TableColumnDescription const& cdef) {
        size_t seed = 0;
        LOG_AGGRPROC(4) << "to compute hash for column with name: " << cdef.column_name
                        << " and type: " << cdef.column_type;
        boost::hash_combine(seed, cdef.column_name);
        boost::hash_combine(seed, cdef.column_type);
        // Do not include default expression for hash computation.
        return seed;
    }
};

class TableColumnsDescription {
  public:
    TableColumnsDescription(const std::string& table_name_) : table_name(table_name_), table_schema_hash(0) {}

    TableColumnsDescription(const TableColumnsDescription& copy) = default;
    TableColumnsDescription(TableColumnsDescription&& move) = default;
    TableColumnsDescription& operator=(const TableColumnsDescription&) = default;
    ~TableColumnsDescription() = default;

    void addColumnDescription(const TableColumnDescription& column_description);

    void buildColumnsDescription(AggregatorLoader& loader);

    size_t getSizeOfDefaultColumns() const;

    size_t getSizeOfOrdinaryColumns() const;

    /**
     * Given an insert query from the client, to identify whether all of the default fields are covered.
     * Note: At this time, we are not sure whether each block sent to the ClickHouse server can contain sub-blocks,
     * while each sub-block can have different defaults covered (for example, sub-block 1 covers default column 1, and
     * sub-block 2 cover default column 2).
     *
     * Case 2 to be supported.
     *
     */
    bool queryIncludeAllDefaultColumns(const std::vector<std::string>& columns_in_insert) const;

    /**
     * Case 2 to be supported. That insertion covers some of the default columns.
     * Should that be  OK that some default columns are missing completely (no some rows having default values, and some
     * other rows do not have default values)
     *
     */
    ColumnTypesAndNamesTableDefinition
    getColumnTypesAndNamesDefinitionWithDefaultColumns(const std::vector<std::string>& columns_in_insert) const;

    const ColumnTypesAndNamesTableDefinition& getFullColumnTypesAndNamesDefinitionCache() const {
        return full_column_types_and_names_def_cache;
    }

    void getSubColumnTypesAndNamesDefinition(size_t first_n_columns,
                                             ColumnTypesAndNamesTableDefinition& sub_columns_definition) const;

    const TableColumnDescription& getColumnDescription(const std::string& column_name) const;

    // Given a column-definition, find out their order mappings with respect to the full ordinary/default table
    // definition according to the schema.
    std::vector<size_t>
    getColumnToSequenceMapping(const ColumnTypesAndNamesTableDefinition& sub_columns_definition) const;

    // based on the ordered mapping that describes what positions the columns coming from Kafka message showed up
    // in the full column definitions, to identify what the are the columns that are not populated in Kafka message,
    // and whether these columns do not have default definitions. If they are with default definitions,
    // they will be picked up already in the Default Columns.
    bool checkColumnsNotCoveredAndWithoutDefaultExpression(const std::vector<size_t>& order_mapping) const;

    const DB::ColumnDefaults& getColumnDefaultsCache() const { return column_defaults_cache; }

    // To return the DB::ColumnsDescription version of the table definition
    const DB::ColumnsDescription& getNativeDBColumnsDescriptionCache() const { return db_columns_description_cache; }

    const std::vector<TableColumnDescription>& getColumnsDescription() const { return columns_description; }

    const std::string& getTableName() const { return table_name; }

    size_t getSchemaHash() const { return table_schema_hash; }

    size_t computeTableSchemaHash() const;

    std::string str() const;

  public:
    // The following calls are only invoked at table columns description creation time and not recommended for access by
    // the batch reader directly. To make them public is to make the current test cases runnable.

    /**
     * The column types and names definition that will cover all of the columns (default + ordinary) for insertion.
     *
     * Case 1 to be supported. This is the block header that we will need to construct to send to the server.
     */
    ColumnTypesAndNamesTableDefinition getFullColumnTypesAndNamesDefinition() const;

    // Identify the columns that have default expression, which are needed to fill in the default missing columns.
    DB::ColumnDefaults getColumnDefaults() const;

    // Construct the DB native columns description
    DB::ColumnsDescription getNativeDBColumnsDescription() const;

  private:
    // build column to sequence mapping
    void buildColumnToSequenceMapping();

  private:
    std::string table_name;
    std::vector<TableColumnDescription> columns_description;
    std::unordered_map<std::string, TableColumnDescription> hashed_description;

    // for ordinary and default column's name to order mapping.
    std::unordered_map<std::string, size_t> column_to_sequence_mapping;

    // a sequence 0, 1, 2, N-1, with N being the number of columns in the definition.
    std::vector<size_t> column_sequence;

    ColumnTypesAndNamesTableDefinition full_column_types_and_names_def_cache;
    DB::ColumnDefaults column_defaults_cache;
    DB::ColumnsDescription db_columns_description_cache;

    // schema hash
    size_t table_schema_hash;
};
}; // namespace nuclm
