cmake_minimum_required(VERSION 3.0.0)
project(KafkaConnector VERSION 0.1.0)

set(CMAKE_MODULE_PATH $(CMAKE_MODULE_PATH) ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_DEPENDENT_MODULES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../deps_prefix")
#to find the dependent libraries related to librdkafka
find_package (kafka REQUIRED)

include_directories(BEFORE ${LIBRDKAFKA_INCLUDE_DIR})

set(CMAKE_CXX_STANDARD 14)

set(KAFKA_CONNECTOR_CORE
        ${PROJECT_SOURCE_DIR}/SimpleBuffer.cpp
        ${PROJECT_SOURCE_DIR}/SimpleFlushTask.cpp
        ${PROJECT_SOURCE_DIR}/GlobalContext.cpp
        ${PROJECT_SOURCE_DIR}/KafkaConnector.cpp
        ${PROJECT_SOURCE_DIR}/logger.cpp
        ${PROJECT_SOURCE_DIR}/Metadata.cpp
        ${PROJECT_SOURCE_DIR}/MetadataVersion.cpp
        ${PROJECT_SOURCE_DIR}/PartitionHandler.cpp
        ${PROJECT_SOURCE_DIR}/RebalanceCb.cpp
        ${PROJECT_SOURCE_DIR}/InvariantChecker.cpp)

add_library(kafkaconnector
        ${KAFKA_CONNECTOR_CORE}
        )

set (DEPENDENCIES
        ${LIBRDKAFKA_LIBRARY}
        ${LIBRDKAFKA_LIBRARY_PLUSPLUS}
        pthread)

add_subdirectory("${PROJECT_SOURCE_DIR}/tests")
#include_directories("${PROJECT_SOURCE_DIR}/tests")

add_executable(main main.cpp ${KAFKA_CONNECTOR_CORE})
target_link_libraries(main PRIVATE kafkaconnector ${DEPENDENCIES})
