# Find the glog library.
# Output variables:
#  GLOG_INCLUDE_DIR : e.g., /usr/local/include/.
#  GLOG_LIBRARY     : Library path of glog library
#  GLOG_FOUND       : True if found.

# for GLOG, we force it to find the one that we packaged in monstordb
#deps_prefix is relative to the project source directory.
set(deps_prefix ${CMAKE_DEPENDENT_MODULES_DIR})
MESSAGE ("dep_prefix is set: ${deps_prefix}")

FIND_PATH(LIBRDKAFKA_INCLUDE_DIR NAME librdkafka
  HINTS ${deps_prefix}/include /usr/include /usr/local/include)

FIND_LIBRARY(LIBRDKAFKA_LIBRARY NAME librdkafka.so
  HINTS ${deps_prefix}/lib /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib
)

FIND_LIBRARY(LIBRDKAFKA_LIBRARY_PLUSPLUS NAME librdkafka++.so
        HINTS ${deps_prefix}/lib /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib
        )

IF (LIBRDKAFKA_INCLUDE_DIR AND LIBRDKAFKA_LIBRARY)
    SET(LIBRDKAFKA_FOUND TRUE)
    MESSAGE(STATUS "In KafkaConnector, found librdkafka library: inc=${LIBRDKAFKA_INCLUDE_DIR}, lib=${LIBRDKAFKA_LIBRARY}")
ELSE ()
    SET(LIBRDKAFKA_FOUND FALSE)
    MESSAGE(STATUS "WARNING: In KafkaConnector, librdkafka library not found.")
    MESSAGE(STATUS "Try: 'sudo apt-get install librdkafka-dev")
ENDIF ()

IF (LIBRDKAFKA_LIBRARY_PLUSPLUS)
    SET(LIBRDKAFKA_LIBRARY_PLUSPLUS_FOUND TRUE)
    MESSAGE(STATUS "In Kafkaconnector, found librdkafka++ library: inc=${LIBRDKAFKA_INCLUDE_DIR}, lib=${LIBRDKAFKA_LIBRARY_PLUSPLUS}")
ELSE ()
    SET(LIBRDKAFKA_LIBRARY_PLUSPLUS_FOUND FALSE)
    MESSAGE(STATUS "WARNING: In KafkaConnector, librdkafka++ library not found.")
    MESSAGE(STATUS "Try: 'sudo apt-get install librdkafka-dev")
ENDIF ()