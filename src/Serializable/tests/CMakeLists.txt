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

function (handle_common_unit_tests_with_multifiles targetname binname)
  #add all list files
  foreach(TEST_SOURCE_FILE IN LISTS ARGN)
    list(APPEND TEST_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/${TEST_SOURCE_FILE}")
  endforeach()
  message ("choose source files: ${TEST_SOURCE_FILES} to build target: ${targetname}")
  add_executable(${targetname} ${TEST_SOURCE_FILES} $<TARGET_OBJECTS:nucolumnar_aggregator_lib> )
  target_link_libraries(${targetname} ${nucolumnar_aggregator_lib})
  set_target_properties(${targetname} PROPERTIES OUTPUT_NAME ${binname})
  add_common_libaries(${targetname})
  add_test(NAME ${targetname} COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${binname})
  install (TARGETS ${targetname} DESTINATION ${PROJECT_SOURCE_DIR}/run-tests/deployed COMPONENT test)
endfunction()


add_subdirectory(unit)
