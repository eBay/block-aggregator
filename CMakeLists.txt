
cmake_minimum_required(VERSION 3.9)
set(CMAKE_VERBOSE_MAKEFILE on )
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) # Export compilation data-base

include(CMakeScripts/utils.cmake)
include(CMakeScripts/protobuf.cmake)
include(CMakeScripts/flatbuffers.cmake)
include(CMakeScripts/PrecompiledHeader-clang.cmake)

project(NuColumnarAggr VERSION 1.0.0 LANGUAGES CXX)

option(UNITTEST "Build Unit Tests" OFF)
option(DATAGENERATOR "Build Data Generator" OFF)
option(COVERAGE "Enable Coverage" OFF)
option(WITH_ASAN "Build ASan+UBSan instrumented code" OFF)
option(WITH_TSAN "Build TSan instrumented code" OFF)
option(WITH_PCH "Build using precompiled headers" OFF)
option(WITH_CLANG_TIDY "Use clang-tidy" OFF)
if (COVERAGE)
    set(CMAKE_BUILD_TYPE "Debug")
    include(CMakeScripts/CodeCoverage.cmake)
    message(STATUS "----Code coverage profiling is ON----")
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING
      "Choose the type of build, options are: Debug Release PreRelease."
      FORCE)
endif()

message(STATUS "~~~ ${PROJECT_NAME} v${PROJECT_VERSION} Build-Type=${CMAKE_BUILD_TYPE} on ${CMAKE_SYSTEM_NAME} Compiler-Id=${CMAKE_CXX_COMPILER_ID} WITH_ASAN=${WITH_ASAN} WITH_TSAN=${WITH_TSAN} COVERAGE=${COVERAGE} UNITTEST=${UNITTEST} Coverage=${COVERAGE} ~~~")

#-- -- -- -- - ASAN and TSAN options are not tested yet -- -- -- -- -- --
if(WITH_ASAN AND CMAKE_BUILD_TYPE STREQUAL Debug)
    if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=undefined -fsanitize-address-use-after-scope -fno-sanitize=alignment -fno-sanitize=vptr -DCDS_ADDRESS_SANITIZER_ENABLED -fno-omit-frame-pointer -fno-optimize-sibling-calls")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address,undefined -fsanitize-address-use-after-scope -DCDS_ADDRESS_SANITIZER_ENABLED -fno-omit-frame-pointer -fno-optimize-sibling-calls")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -fsanitize=undefined")
    else()
        message(ERROR "Compiler does not support AddressSanitizer")
    endif()
endif()

if(WITH_TSAN AND CMAKE_BUILD_TYPE STREQUAL Debug)
    if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -DCDS_THREAD_SANITIZER_ENABLED -fno-omit-frame-pointer")
        set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -DCDS_THREAD_SANITIZER_ENABLED -fno-omit-frame-pointer")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    else()
        message(ERROR "Compiler does not support ThreadSanitizer")
    endif()
endif()

if(DEFINED DEPS_DIR)
    message("Using ${DEPS_DIR} as the home for deps_prefix and deps_build")
else()
    set(DEPS_DIR ${CMAKE_CURRENT_SOURCE_DIR})
endif()
set(deps_prefix ${DEPS_DIR}/deps_prefix)
set(deps_build ${DEPS_DIR}/deps_build)
set(CMAKE_PREFIX_PATH ${deps_prefix})

if (COVERAGE)
    APPEND_COVERAGE_COMPILER_FLAGS()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-inline")
    set(COVERAGE_EXCLUDES
            'test/*'
            '${deps_build}/*'
            '${deps_prefix}/*'
	    '*usr/*'
            'node_modules/*'
            'development/*'
            '*gen_src/*'
            '*tests/*'
   	    '*fds/*'
       	    '*hash/*'
       	    '*common/*'
       	    '*http_server/*'
            '*proto/*'
            'doc/*'
            'cmake-build-*/*'
            )
endif()

set(ENV{PKG_CONFIG_PATH} ${deps_prefix}/lib/pkgconfig)
message(STATUS PKG_CONFIG_PATH=$ENV{PKG_CONFIG_PATH})
find_package(PkgConfig)
pkg_check_modules(DEPS protobuf>=3.11.2)
if(NOT DEPS_FOUND)
message(FATAL_ERROR "at least one required module was not found by pkg-config")
endif()

find_package(glog 0.4.0 REQUIRED CONFIG)
message(STATUS "glog Version=${glog_VERSION}")

find_package(flatbuffers REQUIRED CONFIG NAMES flatbuffers Flatbuffers)
message(STATUS "Flatbuffers Version=${flatbuffers_VERSION}")

find_package(Boost REQUIRED filesystem iostreams chrono)
# include_directories(${Boost_INCLUDE_DIRS})

find_program(
  CLANG_TIDY_EXE
  NAMES "clang-tidy"
  DOC "Path to clang-tidy executable"
  )
if(NOT CLANG_TIDY_EXE)
  message(STATUS "clang-tidy not found.")
else()
  message(STATUS "clang-tidy found: ${CLANG_TIDY_EXE}")
  set(DO_CLANG_TIDY "${CLANG_TIDY_EXE}" "-checks=*,-modernize-use-trailing-return-type,-clang-analyzer-alpha.*,clang-analyzer-*,clang-analyzer-cplusplus*")
endif()

if(CMAKE_BUILD_TYPE STREQUAL Debug)
    set(bld_type d)
    set(clickhouse_build_dir ${deps_build}/ClickHouse/cmake-build-debug)
else()
    set(clickhouse_build_dir ${deps_build}/ClickHouse/cmake-build)
endif()

#-Xclang-dump-deserialized-decls -Xclang-print-stats -v -H
add_compile_options( -fmessage-length=0 -fvisibility=default)
# -fvisibility-inlines-hidde

##

##To match the compilation flag in ClickHouse's top-level CMakeLists.txt, in order to address:
##"to make numeric_limits<__int128> works with GCC", as indicated in this top-level CMakeLists.txt.
##Otherwise, the compilation will fail for the include file that finally include "wide_integer.h"
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 8.0)
    add_compile_options(-std=gnu++2a)
    #when we switch to the above C++ standard compiler, the following compilation flag is removed,
    #to address the compilation error: unrecognized command line option ‘-Wno-deprecated-volatile’
    #add_compile_options(-Wno-deprecated-volatile)
endif()
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
   add_compile_options(-std=c++2a>)
   add_compile_options(-Wno-deprecated-volatile)
endif()

add_compile_options(-Wextra -Wall -Werror
    -Wno-unused-variable -Wno-unused-function -Wno-deprecated-declarations
    -Wno-unused-local-typedefs -Wno-unused-parameter -Wno-implicit-fallthrough 
    -Wno-array-bounds #adding this to fix protobuf compile errors (TODO fix this properly)
    -DRCU_SIGNAL
    $<$<CXX_COMPILER_ID:GNU>:-Wno-deprecated-copy>
    $<$<CXX_COMPILER_ID:GNU>:-Wno-expansion-to-defined>
    $<$<CXX_COMPILER_ID:GNU>:-Wno-unused-but-set-parameter>
    $<$<CXX_COMPILER_ID:AppleClang>:-Wno-unused-lambda-capture>
    # $<IF:$<NOT:$<CONFIG:Release>>,-g,>
    $<IF:$<CONFIG:Debug>,-DGSL_THROW_ON_CONTRACT_VIOLATION,-DGSL_UNENFORCED_ON_CONTRACT_VIOLATION>
    $<IF:$<CONFIG:Debug>,-O0,-O3>
    $<IF:$<CONFIG:Debug>,-D_DEBUG=1,-DNDEBUG=1>
    $<IF:$<CONFIG:Debug>,-DDEBUG_RCU,>
    $<IF:$<CONFIG:Debug>,-D_PRERELEASE=1,>
    $<IF:$<CONFIG:PreRelease>,-D_PRERELEASE=1,>
    $<$<CONFIG:Debug>:-fno-omit-frame-pointer>
 )

#
#Add some compile options not to treat warning as error
#This should be done only for g++ not Clang
#
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 8.0)
	add_compile_options(-Wno-ignored-qualifiers -Wno-class-memaccess -Wno-cast-function-type -Wno-attributes -Wno-type-limits)
endif()

add_definitions(-DUSE_GLOG)

file(GLOB_RECURSE all_src RELATIVE ${PROJECT_SOURCE_DIR} src/*.hpp src/*.h src/*.cc src/*.cpp)
source_group(TREE ${PROJECT_SOURCE_DIR} PREFIX "" FILES ${all_src})

set(SOURCE_FILES
    src/Aggregator/AggregatorLoader.cpp
    src/Aggregator/AggregatorLoaderManager.cpp
    src/Aggregator/BlockSupportedBuffer.cpp
    src/Aggregator/BlockSupportedBufferFlushTask.cpp
    src/Aggregator/ErrorCodes.cpp
    src/Aggregator/KafkaConnectorManager.cpp
    src/Aggregator/TableColumnsDescription.cpp
    src/Aggregator/SerializationHelper.cpp
    src/Aggregator/ProtobufBatchReader.cpp
    src/Aggregator/BlockAddMissingDefaults.cpp
    src/Aggregator/SSLEnabledApplication.cpp
    src/Aggregator/ServerStatusInspector.cpp
    src/Aggregator/SystemStatusTableExtractor.cpp
    src/Aggregator/LoaderOutputStreamLogging.cpp
    src/Aggregator/TableSchemaUpdateTracker.cpp
    src/Aggregator/ZooKeeperLock.cpp
    src/Aggregator/DistributedLoaderLock.cpp

    src/common/enum.hpp
    src/common/logging.hpp
    src/common/utils.cpp
    src/common/utils.cpp
    src/common/crashdump.hpp
    src/common/crashdump.cpp
    src/common/file_command_flags.hpp
    src/common/file_command_flags.cpp
    src/common/settings_factory.hpp
    src/common/settings_factory.cpp
    src/common/file_watcher.hpp
    src/common/file_watcher.cpp
    src/common/log_cleaner.hpp
    src/common/log_cleaner.cpp
    src/common/obj_life_counter.hpp
    src/common/coord_http_client.hpp
    src/common/coord_http_client.cpp

    src/common/hashing.cpp
    src/common/hash/MurmurHash3.cpp

    src/http_server/http_server.hpp
    src/http_server/http_server.cpp

    src/monitor_base/metrics_factory.cpp
    src/monitor_base/metrics_factory.hpp
    src/monitor_base/metrics.cpp
    src/monitor_base/metrics.hpp
    src/monitor_base/histogram_buckets.cpp
    src/monitor_base/histogram_buckets.hpp
    src/monitor_base/nudata_metric_exception.hpp
    
    src/monitor/application_metrics.cpp
    src/monitor/metrics_collector.cpp
    src/monitor/loader_metrics.cpp
    src/monitor/kafka_connector_metrics.cpp
    src/monitor/schema_tracking_metrics.cpp
    src/monitor/system_replicas_metrics.cpp
    src/monitor/system_tables_metrics.cpp
    src/monitor/server_status_metrics.cpp
    src/monitor/distributed_locking_metrics.cpp

    src/Serializable/ISerializableDataType.cpp
    src/Serializable/SerializableDataTypeNumberBase.cpp
    src/Serializable/SerializableDataTypesNumber.cpp
    src/Serializable/SerializableDataTypeFactory.cpp
    src/Serializable/SerializableDataTypeDate.cpp
    src/Serializable/SerializableDataTypeDateTime.cpp
    src/Serializable/SerializableDataTypeDateTime64.cpp
    src/Serializable/SerializableDataTypeString.cpp
    src/Serializable/SerializableDataTypeFixedString.cpp
    src/Serializable/SerializableDataTypeNullable.cpp
    src/Serializable/SerializableDataTypeLowCardinality.cpp
    src/Serializable/SerializableDataTypeArray.cpp

    src/Serializable/ProtobufReader.cpp

    src/KafkaConnector/SimpleBuffer.cpp
    src/KafkaConnector/SimpleFlushTask.cpp
    src/KafkaConnector/GlobalContext.cpp
    src/KafkaConnector/KafkaConnector.cpp
    src/KafkaConnector/KafkaConnectorParametersChecker.cpp
    src/KafkaConnector/Metadata.cpp
    src/KafkaConnector/MetadataVersion.cpp
    src/KafkaConnector/PartitionHandler.cpp
    src/KafkaConnector/RebalanceCb.cpp
    src/KafkaConnector/InvariantChecker.cpp
    src/KafkaConnector/Producer.cpp
    src/application.hpp
    src/application.cpp
    src/KafkaConnector/EventCb.cpp
)


# Create directory for generated code from .proto files
set(_gRPC_PROTO_GENS_DIR ${CMAKE_BINARY_DIR}/gen_src/proto)
file(MAKE_DIRECTORY ${_gRPC_PROTO_GENS_DIR})

set(_flatbuffers_gen_dir ${CMAKE_BINARY_DIR}/gen_src/flatbuffers)
file(MAKE_DIRECTORY ${_flatbuffers_gen_dir})

if(APPLE)
    include_directories(SYSTEM /usr/local/opt/openssl/include)
    include_directories(${deps_prefix}/include/breakpad_src Frameworks/Breakpad.framework/Headers)
    include_directories(SYSTEM ${deps_build}/ClickHouse/contrib/libcxx/include)
    add_compile_options(-nostdinc++)
    add_link_options(-nostdlib++)
elseif(CMAKE_SYSTEM_NAME STREQUAL Linux)
    include_directories(${deps_prefix}/include/breakpad)
endif()

include_directories(
    ${DEPS_STATIC_INCLUDE_DIRS}
    ${_gRPC_PROTO_GENS_DIR}
    ${_flatbuffers_gen_dir}

    ${deps_prefix}/include
    ${deps_prefix}/include/prometheus
    src
    ${deps_build}/ClickHouse/contrib/poco/Foundation/include
    ${deps_build}/ClickHouse/contrib/poco/XML/include
    ${deps_build}/ClickHouse/contrib/poco/Net/include
    ${deps_build}/ClickHouse/contrib/poco/Util/include
    ${deps_build}/ClickHouse/contrib/cityhash102/include
    ${deps_build}/ClickHouse/contrib/ryu
    ${deps_build}/ClickHouse/contrib/libpcg-random/include
    ${deps_build}/ClickHouse/contrib/fmtlib/include
    ${deps_build}/ClickHouse/contrib/dragonbox/include
    ${deps_build}/ClickHouse/contrib/miniselect/include
    ${deps_build}/ClickHouse/base
    ${deps_build}/ClickHouse/base/daemon/include
    ${deps_build}/ClickHouse/base/loggers
    ${deps_build}/ClickHouse/base/pcg-random
    ${deps_build}/ClickHouse/src
    ${clickhouse_build_dir}/src
    ${clickhouse_build_dir}/src/Core/include
)

include_directories(SYSTEM /usr/local/include)


add_library(nucolumnar_aggregator_lib OBJECT ${SOURCE_FILES})

if(CLANG_TIDY_EXE AND WITH_CLANG_TIDY)
  set_target_properties(nucolumnar_aggregator_lib PROPERTIES CXX_CLANG_TIDY "${DO_CLANG_TIDY}")
endif()

set(_FLATBUFFERS_IMPORT_DIR ${PROJECT_SOURCE_DIR}/flatbuffers)
flatbuffers_generate_cpp(embedded_t nucolumnar_aggregator_lib
    flatbuffers/nucolumnar_aggr_config.fbs
)

set(_PROTO_IMPORT_DIR ${PROJECT_SOURCE_DIR}/proto)

m_protobuf_generate(nucolumnar_aggregator_lib
        proto/nucolumnar/aggregator/v1/nucolumnaraggregator.proto
        proto/nucolumnar/datatypes/v1/columnartypes.proto
)
file(WRITE ${CMAKE_BINARY_DIR}/gen_src/version.cpp
    "const char* getVersion() { return \"${PROJECT_VERSION}\"; }"
    )
set_source_files_properties(${CMAKE_BINARY_DIR}/gen_src/version.cpp PROPERTIES GENERATED TRUE)

target_sources(nucolumnar_aggregator_lib PRIVATE
    ${CMAKE_BINARY_DIR}/gen_src/version.cpp
)

if(WITH_PCH)
   add_precompiled_header(nucolumnar_aggregator_lib precompiled_headers.hpp FORCEINCLUDE)
endif()


add_executable(${PROJECT_NAME} src/main.cpp $<TARGET_OBJECTS:nucolumnar_aggregator_lib>)

target_link_libraries(${PROJECT_NAME}
)

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions clickhouse_functions${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Dictionaries clickhouse_dictionaries${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Storages/System clickhouse_storages_system${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/AggregateFunctions clickhouse_aggregate_functions${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/TableFunctions clickhouse_table_functions${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/array clickhouse_functions_array${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/URL clickhouse_functions_url${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/GatherUtils clickhouse_functions_gatherutils${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/JSONPath clickhouse_functions_jsonpath${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src dbms${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Parsers/New clickhouse_parsers_new${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Parsers clickhouse_parsers${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src clickhouse_common_io${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src clickhouse_new_delete${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Dictionaries/Embedded clickhouse_dictionaries_embedded${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions clickhouse_functionsd${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Common/StringUtils string_utils${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Common/Config clickhouse_common_config${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Common/ZooKeeper clickhouse_common_zookeeper${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/consistent-hashing-sumbur consistent-hashing-sumbur${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/memcpy memcpy${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/consistent-hashing consistent-hashing${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/widechar_width widechar_width${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/mysqlxx mysqlxx${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/daemon daemon${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/loggers/loggers loggers${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/base/common common${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/Redis _poco_redis${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/Util _poco_util${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/XML _poco_xml${bld_type} _poco_xml_expat${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/JSON _poco_json${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/JSON _poco_json_pdjson${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/Net _poco_net${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/Net/SSL _poco_net_ssl${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/Crypto _poco_crypto${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/poco-cmake/Foundation _poco_foundation${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libfarmhash farmhash${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/h3-cmake h3${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/hyperscan-cmake hyperscan${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/murmurhash murmurhash${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libbtrie btrie${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libmetrohash metrohash${bld_type})

#add libdivide_impl.a
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/divide divide_impl${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/divide divide_impl_sse2${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/src/Functions/divide divide_impl_avx2${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/cctz-cmake cctz${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/re2_st re2_st${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libcpuid cpuid${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/cityhash102 cityhash${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/double-conversion-cmake double-conversion${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/re2 re2${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/simdjson-cmake simdjson${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/croaring-cmake roaring${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/ryu-cmake ryu${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/hyperscan/lib  hs${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/fastops-cmake fastops${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/base64-cmake base64${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/arrow-cmake parquet_static${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/arrow-cmake thrift_static${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/arrow-cmake arrow_static${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/snappy snappy${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/avro-cmake avrocpp${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/consistent-hashing-sumbur consistent-hashing-sumbur${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/consistent-hashing consistent-hashing${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/fmtlib-cmake fmt${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libcpuid-cmake cpuid${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/dragonbox-cmake dragonbox_to_chars${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/antlr4-runtime-cmake antlr4-runtime${bld_type})

#sasl and krb
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/cyrus-sasl-cmake sasl2${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/krb5-cmake krb5${bld_type})

#yaml
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/yaml-cpp-cmake yaml-cpp${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/nuraft-cmake nuraft${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/openldap-cmake ldap_r${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/openldap-cmake lber${bld_type})

AddStaticLibraries(${PROJECT_NAME} ${deps_prefix}/lib
    ${DEPS_STATIC_LIBRARIES}
    rdkafka++
    rdkafka
    flatbuffers
    glog
    gflags
    prometheus-cpp
    fswatch
    breakpad
    breakpad_client
)

AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/zstd-cmake zstd${bld_type})
AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/lz4-cmake lz4${bld_type})

AddStaticLibraries(${PROJECT_NAME} /usr/local/lib
    boost_system
    boost_thread-mt
    boost_thread
    boost_iostreams
    boost_filesystem
    boost_chrono
    boost_regex
    boost_program_options
    boost_context
)

AddStaticLibraries(${PROJECT_NAME} ${deps_prefix}/lib
    urcu
    urcu-signal
)

if(APPLE)
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-v,-U,_inside_main")
    find_library(CORE_FOUNDATION_LIBRARY CoreFoundation)
    find_library(CORE_SERVICES_LIBRARY CoreServices)
    AddStaticLibraries(${PROJECT_NAME} /usr/local/opt/openssl/lib ssl crypto)
    AddStaticLibraries(${PROJECT_NAME} /usr/local/opt/mysql@5.7/lib mysqlclient)
    AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/zlib-ng z${bld_type})
    AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libcxx-cmake cxx${bld_type})
    AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libcxxabi-cmake cxxabi${bld_type})

    add_library( breakpad_lib SHARED IMPORTED )
    set_property( TARGET breakpad_lib PROPERTY IMPORTED_LOCATION ${DEPS_DIR}/Frameworks/Breakpad.framework/Breakpad )
    target_link_libraries(${PROJECT_NAME} z dl breakpad_lib)

    message("Building without jemalloc")
    target_link_libraries(${PROJECT_NAME} ${CORE_FOUNDATION_LIBRARY} ${CORE_SERVICES_LIBRARY})

    add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
        COMMAND /Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/bin/dsymutil $<TARGET_FILE:${PROJECT_NAME}>
    )
    if (NOT (COVERAGE OR CMAKE_BUILD_TYPE STREQUAL Debug))
        add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
                COMMAND install_name_tool -change @executable_path/../Frameworks/Breakpad.framework/Versions/A/Breakpad
                ${DEPS_DIR}/Frameworks/Breakpad.framework/Versions/A/Breakpad  $<TARGET_FILE:${PROJECT_NAME}>
                COMMAND ${deps_prefix}/bin/dump_syms -g $<TARGET_FILE:${PROJECT_NAME}>.dSYM $<TARGET_FILE:${PROJECT_NAME}> > "$<TARGET_FILE:${PROJECT_NAME}>.sym" 2>/dev/null
                COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/CMakeScripts/breakpad.cmake $<TARGET_FILE:${PROJECT_NAME}>.sym
                )
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL Linux)
    AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/libunwind-cmake unwind${bld_type})
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -fuse-ld=gold")
    if(WITH_ASAN)
        # static version does not work under ASAN
        target_link_libraries(${PROJECT_NAME} jemalloc)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_LIB_JEMALLOC=1")
    else()
        AddStaticLibraries(${PROJECT_NAME} ${deps_prefix}/lib jemalloc)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_LIB_JEMALLOC=1")
    endif()
    AddSystemStaticLibraries(${PROJECT_NAME} mysqlclient ssl crypto pcre)
    AddStaticLibraries(${PROJECT_NAME} ${clickhouse_build_dir}/contrib/zlib-ng z${bld_type})
    AddCompilerStaticLibraries(${PROJECT_NAME} stdc++ stdc++fs backtrace filesystem)
    target_link_libraries(${PROJECT_NAME} m pthread rt dl)
    if (NOT (COVERAGE OR CMAKE_BUILD_TYPE STREQUAL Debug))
        add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD
            COMMAND ${deps_prefix}/bin/dump_syms $<TARGET_FILE:${PROJECT_NAME}> > "$<TARGET_FILE:${PROJECT_NAME}>.sym" 2>/dev/null
            COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/CMakeScripts/breakpad.cmake $<TARGET_FILE:${PROJECT_NAME}>.sym
            COMMAND objcopy --only-keep-debug $<TARGET_FILE:${PROJECT_NAME}> "$<TARGET_FILE:${PROJECT_NAME}>.debug"
            COMMAND strip --strip-debug $<TARGET_FILE:${PROJECT_NAME}>
            COMMAND objcopy --add-gnu-debuglink="$<TARGET_FILE:${PROJECT_NAME}>.debug" $<TARGET_FILE:${PROJECT_NAME}>
        )
    endif()
endif()

if( UNITTEST )
    enable_testing()
    add_subdirectory("tests")
endif()

if( DATAGENERATOR )
    add_subdirectory("dmgen")
endif()

if (COVERAGE)
  # Copy script files.
  file(COPY ${CMAKE_SOURCE_DIR}/run-tests/run_coverage_tests.sh
         DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()

if (COVERAGE)
  set(CODE_COVERAGE_DEPS
      test_serializer_all
      test_aggregator_all
      test_main_launcher
    )

    # lcov
    SETUP_TARGET_FOR_COVERAGE(
        NAME nucolumnar_aggr_cov
        EXECUTABLE ./run_coverage_tests.sh
        DEPENDENCIES ${CODE_COVERAGE_DEPS}
    )

    # gcov
    SETUP_TARGET_FOR_COVERAGE_COBERTURA(
        NAME nucolumnar_aggr_gcov
        EXECUTABLE ./run_coverage_tests.sh
        DEPENDENCIES ${CODE_COVERAGE_DEPS}
    )
endif()
