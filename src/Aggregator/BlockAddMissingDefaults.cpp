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

#include <Aggregator/BlockAddMissingDefaults.h>
#include <Interpreters/addMissingDefaults.h>
#include <Storages/ColumnsDescription.h>
#include <Interpreters/ExpressionActions.h>

#include "common/logging.hpp"

/**
 * This utility takes care the filling of the missing columns with:
 * (1) explicit user-defined default expressions attached to the column type definition;
 * (2) or implicit system-provided default expression, documented at:
 * https://clickhouse.tech/docs/en/sql-reference/statements/create/table/#create-default-values
 *
 */
namespace nuclm {

DB::Block blockAddMissingDefaults(const DB::Block& block, const DB::NamesAndTypesList& required_columns,
                                  const DB::ColumnsDescription& required_columns_description,
                                  DB::ContextMutablePtr context) {
    if (CVLOG_IS_ON(VMODULE_AGGR_PROCESSOR, 4)) {
        for (const auto& column : required_columns) {
            LOG_AGGRPROC(4) << "required column name: " << column.name
                            << " with column type: " << column.type->getName();
        }
    }

    // Existing block that has been constructed
    if (CVLOG_IS_ON(VMODULE_AGGR_PROCESSOR, 4)) {
        LOG_AGGRPROC(4) << "structure dumped for passed in block structure: " << block.dumpStructure();
    }

    DB::Block header = block.cloneEmpty();
    // we choose the default parameter value: bool null_as_default = false, as it is used only for the INSERT SELECT
    // statement
    DB::ActionsDAGPtr dag = DB::addMissingDefaults(header, required_columns, required_columns_description, context);

    DB::ExpressionActionsPtr adding_defaults_actions = std::make_shared<DB::ExpressionActions>(std::move(dag));
    auto copy_block = block;
    adding_defaults_actions->execute(copy_block);

    return copy_block;
}

} // namespace nuclm
