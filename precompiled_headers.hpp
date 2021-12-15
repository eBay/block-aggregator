
#ifdef __APPLE__
#include <ctime>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

#include <nlohmann/json.hpp>
#include <tclap/CmdLine.h>

// #include <Core/Defines.h>
// #include <IO/WriteHelpers.h>
// #include <IO/WriteBufferFromFileDescriptor.h>
// #include <IO/WriteBufferFromFile.h>
// #include <IO/ReadBufferFromMemory.h>
// #include <Common/NetException.h>
// #include <Common/Exception.h>
// #include <Parsers/parseQuery.h>
// #include <Parsers/ParserQuery.h>
// #include <Parsers/formatAST.h>
// #include <Parsers/ASTInsertQuery.h>
// #include <Storages/ColumnsDescription.h>
// #include <DataStreams/AddingDefaultBlockOutputStream.h>
// #include <DataStreams/AddingDefaultsBlockInputStream.h>
// #include <DataStreams/AsynchronousBlockInputStream.h>
// #include <DataStreams/InternalTextLogsRowOutputStream.h>

#include <glog/logging.h>

#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>

#include <unordered_map>
#include <string>
#include <queue>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
