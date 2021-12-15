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

#include <Aggregator/SerializationHelper.h>

#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Core/Block.h>
#include <Storages/ColumnsDescription.h>

namespace nuclm {

/**
 * Perform fixing on the columns with missing defaults.
 *
 * @param block  the passed-in block that needs to be transformed
 * @param required_columns  the columns from the table definition that requires full default columns to be filled
 * @param required_columns_descriptions the full column definition that is based on the table definition. It contains
 * default expression.
 * @param context  the DB context that is passed from the top
 * @return the fixed blocks that corresponding to the passed-in block.
 */
DB::Block blockAddMissingDefaults(const DB::Block& block, const DB::NamesAndTypesList& required_columns,
                                  const DB::ColumnsDescription& required_columns_description,
                                  DB::ContextMutablePtr context);

} // namespace nuclm
