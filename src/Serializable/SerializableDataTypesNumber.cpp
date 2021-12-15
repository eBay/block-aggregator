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

#include <Serializable/SerializableDataTypesNumber.h>
#include <Serializable/SerializableDataTypeFactory.h>

namespace nuclm {

void registerDataTypeNumbers(SerializableDataTypeFactory& factory) {
    factory.registerSimpleDataType(
        "UInt8", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeUInt8>()); });
    factory.registerSimpleDataType(
        "UInt16", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeUInt16>()); });
    factory.registerSimpleDataType(
        "UInt32", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeUInt32>()); });
    factory.registerSimpleDataType(
        "UInt64", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeUInt64>()); });

    factory.registerSimpleDataType(
        "Int8", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeInt8>()); });
    factory.registerSimpleDataType(
        "Int16", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeInt16>()); });
    factory.registerSimpleDataType(
        "Int32", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeInt32>()); });
    factory.registerSimpleDataType(
        "Int64", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeInt64>()); });

    factory.registerSimpleDataType(
        "Float32", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeFloat32>()); });
    factory.registerSimpleDataType(
        "Float64", [] { return SerializableDataTypePtr(std::make_shared<SerializableDataTypeFloat64>()); });

    /// These synonyms are added for compatibility.

    factory.registerAlias("TINYINT", "Int8", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("SMALLINT", "Int16", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("INT", "Int32", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("INTEGER", "Int32", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("BIGINT", "Int64", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("FLOAT", "Float32", SerializableDataTypeFactory::CaseInsensitive);
    factory.registerAlias("DOUBLE", "Float64", SerializableDataTypeFactory::CaseInsensitive);
}

} // namespace nuclm
