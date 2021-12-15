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

#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>

#include <Core/Defines.h>

#include <IO/WriteHelpers.h>

#include <Serializable/ISerializableDataType.h>

namespace nuclm {

ISerializableDataType::ISerializableDataType() {}

ISerializableDataType::~ISerializableDataType() {}

DB::String ISerializableDataType::getName() const { return doGetName(); }

DB::String ISerializableDataType::doGetName() const { return getFamilyName(); }

} // namespace nuclm
