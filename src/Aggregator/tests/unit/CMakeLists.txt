function (add_local_test targetname)
  handle_common_unit_tests (${targetname} ${targetname} ${targetname})
endfunction()

function (add_local_test_with_multifiles targetname)
  handle_common_unit_tests_with_multifiles (${targetname} ${targetname} ${ARGN})
endfunction()

function (add_common_test targetname)
  add_local_test(${targetname})
endfunction()

function (add_common_test_with_multifiles targetname)
  add_local_test_with_multifiles(${targetname} ${ARGN})
endfunction()

if (COVERAGE)
  message("choose coverage = 1 path")
  
  add_common_test_with_multifiles(
    test_aggregator_all

    test_blocks_no_main
    test_configuration_loading_no_main
    test_serializer_loader_no_main
    test_datetime64_serializer_loader_no_main
    test_missing_columns_with_defaults_no_main
    test_nullable_serializer_loader_no_main
    test_lowcardinality_serializer_loader_no_main
    test_array_serializer_loader_no_main
    test_defaults_serializer_loader_no_main
    test_aggregator_loader_no_main
    test_aggregator_tabledefinitions_no_main
    test_aggregator_tablequeries_no_main
    test_tabledefinition_retrievals_no_main
    test_aggregator_loader_manager_no_main
    test_protobuf_reader_no_main
    test_ioservice_threadpool_no_main
    test_connection_pooling_no_main
    test_dynamic_schema_update_no_main
    test_system_table_extractor_no_main
    test_distributed_locking_no_main
    test_persistent_command_flags_no_main
    
    test_context test_main
  )

  # no changes for test coverage testing for this special test suite
  add_common_test(test_main_launcher)

else()
  message("choose coverage = 0 path")
  

  add_common_test(test_blocks)
  add_common_test (test_columns_description)
  add_common_test(test_configuration_loading)
  add_common_test(test_serializer_loader)
  add_common_test(test_datetime64_serializer_loader)
  add_common_test(test_missing_columns_with_defaults)
  add_common_test(test_nullable_serializer_loader)
  add_common_test(test_lowcardinality_serializer_loader)
  add_common_test(test_array_serializer_loader)
  add_common_test(test_defaults_serializer_loader)
  add_common_test(launch_serializer_based_producer)

  add_common_test(test_aggregator_loader)
  add_common_test(test_aggregator_tabledefinitions)
  add_common_test(test_aggregator_tablequeries)
  add_common_test(test_tabledefinition_retrievals)
  add_common_test(test_aggregator_loader_manager)
  add_common_test(test_protobuf_reader)
  add_common_test(test_ioservice_threadpool)
  add_common_test(test_connection_pooling)
  add_common_test(test_main_launcher)
  add_common_test(test_main_launcher_tls)
  add_common_test(test_tls_setting_and_loading)
  add_common_test(test_dynamic_schema_update)
  add_common_test(test_block_add_missing_defaults)
  add_common_test(test_system_table_extractor)
  add_common_test(test_distributed_locking)
  add_common_test(test_persistent_command_flags)

endif()
