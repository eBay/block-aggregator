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

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/TableColumnsDescription.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <iostream>
#include <fstream>
#include <sstream>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

/**
 * For primitive types: family name and name are identical
 *
 *  family name identified for FixedString(8) is: FixedString
 *  name identified for FixedString(8) is: FixedString(8)
 *
 *  family name identified for Nullable (String) is: Nullable
 *  name identified for Nullable (String)) is: Nullable(String)
 */
TEST(SerializionHelper, testSerializationHelperOnBlock) {

    nuclm::TableColumnsDescription table_definition("simple_event_5");
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counter", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("DateField", "Date"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("DateTimeField", "DateTime"));

    DB::Block sample_block =
        nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
    std::string names = sample_block.dumpNames();
    LOG(INFO) << "column names dumped : " << names;

    nuclm::ColumnSerializers serializers =
        nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
    size_t total_number_of_columns = serializers.size();
    for (size_t i = 0; i < total_number_of_columns; i++) {
        std::string family_name = serializers[i]->getFamilyName();
        std::string name = serializers[i]->getName();

        LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
        LOG(INFO) << "name identified for Column: " << i << " is: " << name;
    }
}

TEST(SerializionHelper, testSerializationHelperOnBlockWithFixedStringAndNullAble) {

    nuclm::TableColumnsDescription table_definition("simple_event_5");
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Counter", "UInt64"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Host", "String"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("Colo", "FixedString(10)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("DateField", "Nullable (Date)"));
    table_definition.addColumnDescription(nuclm::TableColumnDescription("DateTimeField", "DateTime"));

    DB::Block sample_block =
        nuclm::SerializationHelper::getBlockDefinition(table_definition.getFullColumnTypesAndNamesDefinition());
    std::string names = sample_block.dumpNames();
    LOG(INFO) << "column names dumped : " << names;

    nuclm::ColumnSerializers serializers =
        nuclm::SerializationHelper::getColumnSerializers(table_definition.getFullColumnTypesAndNamesDefinition());
    size_t total_number_of_columns = serializers.size();
    for (size_t i = 0; i < total_number_of_columns; i++) {
        std::string family_name = serializers[i]->getFamilyName();
        std::string name = serializers[i]->getName();

        LOG(INFO) << "family name identified for Column: " << i << " is: " << family_name;
        LOG(INFO) << "name identified for Column: " << i << " is: " << name;
    }
}

// Call RUN_ALL_TESTS() in main()
int main(int argc, char** argv) {

    // with main, we can attach some google test related hooks.
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
