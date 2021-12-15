#!/bin/bash
set -e

# parameters of setting library path
#export LD_LIBRARY_PATH=/usr/local/lib

echo "current user: ${current_user}"
echo "in scripts directory, test_top_dir is:  ${test_top_dir}"

ls -al ${test_top_dir}
ls -al ${test_top_dir}/deployed

echo -e "\n===========Serializable: run test_serialization_creation=====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_creation

echo -e "\n===========Serializable: run test_message_serialization====================\n"
cd ${test_top_dir}/deployed; ./test_message_serialization

echo -e "\n===========Serializable: run test_serializer_helper====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_helper

echo -e "\n===========Serializable: run test_serializer_basic ====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_basic

echo -e "\n===========Serializable: run test_serializer_fixedstring ====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_fixedstring

echo -e "\n===========Serializable: run test_serializer_datetime64 ====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_datetime64

echo -e "\n===========Serializable: run test_serializer_lowcardinality ====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_lowcardinality

echo -e "\n===========Serializable: run test_serializer_nullable ====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_nullable

echo -e "\n===========Serializable: run test_serializer_array ====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_array

echo -e "\n===========Serializable: run time_unit_conversion  ====================\n"
cd ${test_top_dir}/deployed; ./test_timeunit_conversion


echo -e "\n===========Aggregator: run test_blocks=====================\n"
cd ${test_top_dir}/deployed; ./test_blocks

echo -e  "\n===========Aggregator: run test_columns====================\n"
cd ${test_top_dir}/deployed; ./test_columns

echo -e  "\n===========Aggregator: run test_columns_description====================\n"
cd ${test_top_dir}/deployed; ./test_columns_description

echo -e "\n===========Aggregator: run test_configuration_loading==========\n"
cd ${test_top_dir}/deployed; ./test_configuration_loading

echo -e "\n===========Aggregator: run test_serializer_loader==========\n"
cd ${test_top_dir}/deployed; ./test_serializer_loader

echo -e "\n===========Aggregator: run test_datetime64_serializer_loader==========\n"
cd ${test_top_dir}/deployed; ./test_datetime64_serializer_loader

echo -e "\n===========Aggregator: run test_missing_columns_with_defaults==========\n"
cd ${test_top_dir}/deployed; ./test_missing_columns_with_defaults

echo -e "\n===========Aggregator: run test_nullable_serializer_loader==========\n"
cd ${test_top_dir}/deployed; ./test_nullable_serializer_loader

echo -e "\n===========Aggregator: run test_lowcardinality_serializer_loader==========\n"
cd ${test_top_dir}/deployed; ./test_lowcardinality_serializer_loader

echo -e "\n===========Aggregator: run test_array_serializer_loader==========\n"
cd ${test_top_dir}/deployed; ./test_array_serializer_loader

echo -e "\n===========Aggregator: run test_defaults_serializer_loader==========\n"
cd ${test_top_dir}/deployed; ./test_defaults_serializer_loader

echo -e "\n===========Aggregator: run test_aggregator_loader==========\n"
cd ${test_top_dir}/deployed; ./test_aggregator_loader

echo -e "\n===========Aggregator: run test_aggregator_tabledefinitions==========\n"
cd ${test_top_dir}/deployed; ./test_aggregator_tabledefinitions

echo -e "\n===========Aggregator: run test_aggregator_tablequeries==========\n"
cd ${test_top_dir}/deployed; ./test_aggregator_tablequeries

echo -e "\n===========Aggregator: run test_tabledefinition_retrievals==========\n"
cd ${test_top_dir}/deployed; ./test_tabledefinition_retrievals

echo -e "\n===========Aggregator: run test_aggregator_loader_manager==========\n"
cd ${test_top_dir}/deployed; ./test_aggregator_loader_manager

echo -e "\n===========Aggregator: run test_protobuf_reader==========\n"
cd ${test_top_dir}/deployed; ./test_protobuf_reader

echo -e "\n===========Aggregator: run test_ioservice_threadpool==========\n"
cd ${test_top_dir}/deployed; ./test_ioservice_threadpool

echo -e "\n===========Aggregator: run test_connection_pooling==========\n"
cd ${test_top_dir}/deployed; ./test_connection_pooling

#In tess130, we have not had TLS-enabled clickhouse server setup.
#echo -e "\n===========run test_tls_setting_and_loading====\n"
#cd ${test_top_dir}/deployed; ./test_tls_setting_and_loading

echo -e "\n===========Aggregator: run test_dynamic_schema_update disabled for now====\n"
#cd ${test_top_dir}/deployed; ./test_dynamic_schema_update

echo -e "\n===========Aggregator: run test_block_add_missing_defaults====\n"
cd ${test_top_dir}/deployed; ./test_block_add_missing_defaults

echo -e "\n===========Aggregator: run test_system_table_extractor====\n"
cd ${test_top_dir}/deployed; ./test_system_table_extractor

echo -e "\n===========Aggregator: run test_distributed_locking====\n"
cd ${test_top_dir}/deployed; ./test_distributed_locking

echo -e "\n===========Aggregator: run test_persistent_command_flags====\n"
cd ${test_top_dir}/deployed; ./test_persistent_command_flags

echo -e "\n===========Aggregator: run test_main_launcher with config file on ../conf/simple_aggregator_config.json====\n"
cd ${test_top_dir}/deployed; ./test_main_launcher

#echo -e "\n====run test_main_launcher_tls with config file on ../conf/simple_aggregator_config_with_tls.json====\n"
#cd ${test_top_dir}/deployed; ./test_main_launcher_tls


