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
      test_serializer_all

      test_serializer_creation_no_main
      test_message_serialization_no_main
      test_serializer_helper_no_main
      test_serializer_basic_no_main
      test_serializer_fixedstring_no_main
      test_serializer_lowcardinality_no_main
      test_serializer_datetime64_no_main
      test_serializer_nullable_no_main
      test_serializer_array_no_main
      test_timeunit_conversion_no_main

      test_context test_main
  )
  
else()
  message("choose coverage = 0 path")
  
  add_common_test(test_serializer_creation)
  add_common_test(test_message_serialization)
  add_common_test(test_serializer_helper)
  add_common_test(test_serializer_basic)
  add_common_test(test_serializer_fixedstring)
  add_common_test(test_serializer_lowcardinality)
  add_common_test(test_serializer_datetime64)
  add_common_test(test_serializer_nullable)
  add_common_test(test_serializer_array)
  add_common_test(test_timeunit_conversion)
endif()  

