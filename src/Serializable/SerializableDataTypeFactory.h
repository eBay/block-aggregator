/************************************************************************
Modifications Copyright 2021, eBay, Inc.

Original Copyright:
See URL: https://github.com/ClickHouse/ClickHouse

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

#include <Serializable/ISerializableDataType.h>
#include <Parsers/IAST_fwd.h>
#include <Common/IFactoryWithAliases.h>
#include <DataTypes/DataTypeFactory.h>

#include <functional>
#include <memory>
#include <unordered_map>

namespace nuclm {

class ISerializableDataType;
// using SerializableDataTypePtr = std::shared_ptr<const ISerializableDataType>;
/** Creates a data type by name of data type family and parameters.
 */
class SerializableDataTypeFactory final
        : private boost::noncopyable,
          public DB::IFactoryWithAliases<std::function<SerializableDataTypePtr(const DB::ASTPtr& parameters)>> {
  private:
    using Value = std::function<SerializableDataTypePtr(const DB::ASTPtr& parameters)>;
    using Creator = Value;
    using SimpleCreator = std::function<SerializableDataTypePtr()>;
    using DataTypesDictionary = std::unordered_map<DB::String, Creator>;

  public:
    static SerializableDataTypeFactory& instance();

    SerializableDataTypePtr get(const DB::String& full_name) const;
    SerializableDataTypePtr get(const DB::String& family_name, const DB::ASTPtr& parameters) const;
    SerializableDataTypePtr get(const DB::ASTPtr& ast) const;

    /// Register a type family by its name.
    /// Note: Creator and CaseSensitiveness are declared in DB::IFactoryWithAliases
    void registerDataType(const DB::String& family_name, Creator creator,
                          CaseSensitiveness case_sensitiveness = CaseSensitive);

    /// Register a simple data type, that have no parameters.
    void registerSimpleDataType(const DB::String& name, SimpleCreator creator,
                                CaseSensitiveness case_sensitiveness = CaseSensitive);

  private:
    const Creator& findCreatorByName(const DB::String& family_name) const;

  private:
    DataTypesDictionary data_types;

    /// Case insensitive data types will be additionally added here with lowercased name.
    DataTypesDictionary case_insensitive_data_types;

    SerializableDataTypeFactory();
    virtual ~SerializableDataTypeFactory();

    const DataTypesDictionary& getMap() const override { return data_types; }
    const DataTypesDictionary& getCaseInsensitiveMap() const override { return case_insensitive_data_types; }
    String getFactoryName() const override { return "SerializableDataTypeFactory"; }
};

} // namespace nuclm
