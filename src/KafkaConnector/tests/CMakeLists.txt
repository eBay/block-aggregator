get_target_property(nucolumnar_aggregator_lib NuColumnarAggr LINK_LIBRARIES)

function (add_common_libaries targetname)
  AddStaticLibraries(${targetname} ${deps_prefix}/lib
      gtest
  )
endfunction()

function (handle_common_unit_tests targetname srcname binname)
  add_executable(${targetname} ${CMAKE_CURRENT_SOURCE_DIR}/${srcname}.cpp $<TARGET_OBJECTS:nucolumnar_aggregator_lib> )
  target_link_libraries(${targetname} ${nucolumnar_aggregator_lib})
  set_target_properties(${targetname} PROPERTIES OUTPUT_NAME ${binname})
  add_common_libaries(${targetname})
  add_test(NAME ${targetname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${binname})
  install (TARGETS ${targetname} DESTINATION ${PROJECT_SOURCE_DIR}/run-tests/deployed COMPONENT test)
endfunction()

function (add_local_test targetname)
  handle_common_unit_tests (${targetname} ${targetname} ${targetname})
endfunction()

function (add_common_test targetname)
  add_local_test(${targetname})
endfunction()

add_common_test (metadata_tests)
add_common_test (flushtask_tests)
add_common_test (buffer_tests)
add_common_test (kafka_connector_tests)
add_common_test (token_generator)
