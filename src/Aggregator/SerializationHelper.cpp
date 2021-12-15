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

#include <Serializable/SerializableDataTypeFactory.h>

#include <Aggregator/SerializationHelper.h>
#include <Core/Block.h>

#include <sstream>

namespace nuclm {

DB::Block SerializationHelper::getBlockDefinition(const ColumnTypesAndNamesTableDefinition& table_definition) {
    size_t number_of_columns = table_definition.size();
    DB::ColumnsWithTypeAndName columns_with_types_and_names;
    for (size_t i = 0; i < number_of_columns; i++) {
        // const ColumnPtr & column_, const DataTypePtr & type_, const String & name_
        const ColumnTypeAndNameDefinition& column_def = table_definition[i];
        columns_with_types_and_names.emplace_back(column_def.type, column_def.name);
    }

    DB::Block sampleBlock{std::move(columns_with_types_and_names)};
    return sampleBlock;
}

ColumnSerializers
SerializationHelper::getColumnSerializers(const ColumnTypesAndNamesTableDefinition& table_definition) {
    size_t number_of_columns = table_definition.size();
    ColumnSerializers columnSerializers;
    for (size_t i = 0; i < number_of_columns; i++) {
        const ColumnTypeAndNameDefinition& column_def = table_definition[i];
        columnSerializers.emplace_back(column_def.serializer);
    }

    return columnSerializers;
}

std::string SerializationHelper::strOfColumnSerializers(const ColumnSerializers& serializers) {
    std::stringstream description;
    for (auto elem = serializers.begin(); elem != serializers.end(); ++elem) {
        if (std::distance(elem, serializers.end()) == 1) {
            description << "( " << (*elem)->getName() << ")";
        } else {
            description << "( " << (*elem)->getName() << "),";
        }
    }

    return description.str();
}

std::string SerializationHelper::strOfColumnTypesAndNamesTableDefinition(
    const ColumnTypesAndNamesTableDefinition& column_definitions) {
    std::stringstream description;
    for (auto elem = column_definitions.begin(); elem != column_definitions.end(); ++elem) {
        if (std::distance(elem, column_definitions.end()) == 1) {
            description << "(column type: " << elem->type->getName() << " column name: " << elem->name << ")";
        } else {
            description << "(column type: " << elem->type->getName() << " column name: " << elem->name << "),";
        }
    }

    return description.str();
}

} // namespace nuclm
