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

#include <Serializable/SerializableDataTypeFactory.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>

namespace nuclm {

namespace ErrorCodes {
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
extern const int UNEXPECTED_AST_STRUCTURE;
extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
} // namespace ErrorCodes

SerializableDataTypePtr SerializableDataTypeFactory::get(const DB::String& full_name) const {
    DB::ParserIdentifierWithOptionalParameters parser;
    DB::ASTPtr ast = DB::parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "data type", 0, 0);
    return get(ast);
}

SerializableDataTypePtr SerializableDataTypeFactory::get(const DB::ASTPtr& ast) const {
    if (const auto* func = ast->as<DB::ASTFunction>()) {
        if (func->parameters)
            throw DB::Exception("Data type cannot have multiple parenthesed parameters.",
                                ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
        return get(func->name, func->arguments);
    }

    if (const auto* ident = ast->as<DB::ASTIdentifier>()) {
        return get(ident->name(), {});
    }

    if (const auto* lit = ast->as<DB::ASTLiteral>()) {
        if (lit->value.isNull())
            return get("Null", {});
    }

    throw DB::Exception("Unexpected AST element for data type.", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

SerializableDataTypePtr SerializableDataTypeFactory::get(const DB::String& family_name_param,
                                                         const DB::ASTPtr& parameters) const {
    DB::String family_name = getAliasToOrName(family_name_param);

    if (endsWith(family_name, "WithDictionary")) {
        DB::ASTPtr low_cardinality_params = std::make_shared<DB::ASTExpressionList>();
        DB::String param_name = family_name.substr(0, family_name.size() - strlen("WithDictionary"));
        if (parameters) {
            auto func = std::make_shared<DB::ASTFunction>();
            func->name = param_name;
            func->arguments = parameters;
            low_cardinality_params->children.push_back(func);
        } else
            low_cardinality_params->children.push_back(std::make_shared<DB::ASTIdentifier>(param_name));

        return get("LowCardinality", low_cardinality_params);
    }

    return findCreatorByName(family_name)(parameters);
}

void SerializableDataTypeFactory::registerDataType(const DB::String& family_name, Creator creator,
                                                   CaseSensitiveness case_sensitiveness) {
    if (creator == nullptr)
        throw DB::Exception("DataTypeFactory: the data type family " + family_name +
                                " has been provided "
                                " a null constructor",
                            ErrorCodes::LOGICAL_ERROR);

    DB::String family_name_lowercase = Poco::toLower(family_name);

    if (isAlias(family_name) || isAlias(family_name_lowercase))
        throw DB::Exception("DataTypeFactory: the data type family name '" + family_name +
                                "' is already registered as alias",
                            ErrorCodes::LOGICAL_ERROR);

    if (!data_types.emplace(family_name, creator).second)
        throw DB::Exception("DataTypeFactory: the data type family name '" + family_name + "' is not unique",
                            ErrorCodes::LOGICAL_ERROR);

    if (case_sensitiveness == CaseInsensitive &&
        !case_insensitive_data_types.emplace(family_name_lowercase, creator).second)
        throw DB::Exception("DataTypeFactory: the case insensitive data type family name '" + family_name +
                                "' is not unique",
                            ErrorCodes::LOGICAL_ERROR);
}

void SerializableDataTypeFactory::registerSimpleDataType(const DB::String& name, SimpleCreator creator,
                                                         CaseSensitiveness case_sensitiveness) {
    if (creator == nullptr)
        throw DB::Exception("DataTypeFactory: the data type " + name +
                                " has been provided "
                                " a null constructor",
                            ErrorCodes::LOGICAL_ERROR);

    registerDataType(
        name,
        [name, creator](const DB::ASTPtr& ast) {
            if (ast)
                throw DB::Exception("Data type " + name + " cannot have arguments",
                                    ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS);
            return creator();
        },
        case_sensitiveness);
}

const SerializableDataTypeFactory::Creator&
SerializableDataTypeFactory::findCreatorByName(const DB::String& family_name) const {
    {
        DataTypesDictionary::const_iterator it = data_types.find(family_name);
        if (data_types.end() != it)
            return it->second;
    }

    DB::String family_name_lowercase = Poco::toLower(family_name);

    {
        DataTypesDictionary::const_iterator it = case_insensitive_data_types.find(family_name_lowercase);
        if (case_insensitive_data_types.end() != it)
            return it->second;
    }

    auto hints = this->getHints(family_name);
    if (!hints.empty())
        throw DB::Exception("Unknown data type family: " + family_name + ". Maybe you meant: " + DB::toString(hints),
                            ErrorCodes::UNKNOWN_TYPE);
    else
        throw DB::Exception("Unknown data type family: " + family_name, ErrorCodes::UNKNOWN_TYPE);
}

void registerDataTypeNumbers(SerializableDataTypeFactory& factory);
void registerDataTypeDate(SerializableDataTypeFactory& factory);
void registerDataTypeDateTime(SerializableDataTypeFactory& factory);
void registerDataTypeDateTime64(SerializableDataTypeFactory& factory);
void registerDataTypeString(SerializableDataTypeFactory& factory);
void registerDataTypeFixedString(SerializableDataTypeFactory& factory);
void registerDataTypeNullable(SerializableDataTypeFactory& factory);
void registerDataTypeLowCardinality(SerializableDataTypeFactory& factory);
void registerDataTypeArray(SerializableDataTypeFactory& factory);

SerializableDataTypeFactory::SerializableDataTypeFactory() {
    registerDataTypeNumbers(*this);
    registerDataTypeDate(*this);
    registerDataTypeDateTime(*this);
    registerDataTypeDateTime64(*this);
    registerDataTypeString(*this);
    registerDataTypeFixedString(*this);
    registerDataTypeNullable(*this);
    registerDataTypeLowCardinality(*this);
    registerDataTypeArray(*this);
}

SerializableDataTypeFactory::~SerializableDataTypeFactory() {}

SerializableDataTypeFactory& SerializableDataTypeFactory::instance() {
    static SerializableDataTypeFactory ret;
    return ret;
}

} // namespace nuclm
