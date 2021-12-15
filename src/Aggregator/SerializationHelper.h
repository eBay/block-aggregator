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

#include <Core/Block.h>

#include <vector>
#include <utility>
#include <DataTypes/DataTypeFactory.h>
#include "Serializable/ISerializableDataType.h"
#include "Serializable/SerializableDataTypeFactory.h"

namespace nuclm {

struct ColumnTypeAndNameDefinition {
    std::string type_name;
    // column type
    DB::DataTypePtr type;
    // column type serializer
    SerializableDataTypePtr serializer;
    // column name.
    std::string name;

    ColumnTypeAndNameDefinition(const std::string& type_name_, const std::string& name_) :
            type_name(type_name_),
            type(DB::DataTypeFactory::instance().get(type_name_)),
            serializer(SerializableDataTypeFactory::instance().get(type_name_)),
            name(name_) {}

    ColumnTypeAndNameDefinition(const std::string& type_name_, const DB::DataTypePtr& type_,
                                const SerializableDataTypePtr& serializer_, const std::string& name_) :
            type_name(type_name_), type(type_), serializer(serializer_), name(name_) {}

    ColumnTypeAndNameDefinition(const ColumnTypeAndNameDefinition& copy) = default;
    ColumnTypeAndNameDefinition(ColumnTypeAndNameDefinition&& move) = default;
    ColumnTypeAndNameDefinition& operator=(const ColumnTypeAndNameDefinition&) = default;
    ~ColumnTypeAndNameDefinition() = default;
};

using ColumnTypesAndNamesTableDefinition = std::vector<ColumnTypeAndNameDefinition>;
using ColumnSerializers = std::vector<SerializableDataTypePtr>;

class SerializationHelper {
  public:
    SerializationHelper() = default;
    ~SerializationHelper() = default;

    static DB::Block getBlockDefinition(const ColumnTypesAndNamesTableDefinition& table_definition);
    static ColumnSerializers getColumnSerializers(const ColumnTypesAndNamesTableDefinition& table_definition);

    static std::string strOfColumnSerializers(const ColumnSerializers& serializers);

    static std::string
    strOfColumnTypesAndNamesTableDefinition(const ColumnTypesAndNamesTableDefinition& column_definitions);
};

} // namespace nuclm
