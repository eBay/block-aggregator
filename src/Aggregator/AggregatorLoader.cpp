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

#include "Aggregator/AggregatorLoader.h"
#include "common/settings_factory.hpp"
#include "common/logging.hpp"

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/NetException.h>
#include <Common/Exception.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/ColumnsDescription.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>

#include <Common/InterruptListener.h>
#include <glog/logging.h>

#include <memory>
#include <tuple>
#include <sstream>

namespace nuclm {

namespace ErrorCodes {
extern const int FAILED_TO_CONNECT_TO_SERVER;
extern const int BAD_ARGUMENTS;
extern const int NO_DATA_TO_INSERT;
extern const int UNEXPECTED_PACKET_FROM_SERVER;
extern const int UNKNOWN_PACKET_FROM_SERVER;
} // namespace ErrorCodes

static Poco::Timespan timespan(uint32_t interval_ms) {
    uint32_t second = interval_ms / 1000;
    uint32_t remaining_ms = interval_ms - second * 1000;

    return Poco::Timespan(second, remaining_ms * 1000);
}

static DB::ASTPtr parseInsertQuery(const char*& pos, const char* end, bool allow_multi_statements) {
    DB::ParserQuery parser(end);
    DB::ASTPtr res;

    res = DB::parseQueryAndMovePosition(parser, pos, end, "", allow_multi_statements, 0, 0);

    {
        DB::WriteBufferFromOwnString buf;
        DB::formatAST(*res, buf, false, true);
        LOG_AGGRPROC(4) << "insert query query expression is: " << buf.str();
    }

    return res;
}

bool AggregatorLoader::init() {
    if (!initialized) {
        std::string server_name;
        std::string server_display_name;

        UInt64 server_revision = 0;
        UInt64 server_version_major = 0;
        UInt64 server_version_minor = 0;
        UInt64 server_version_patch = 0;

        try {
            // force connect for the retrieved pooled entry.
            connection_pool_entry = connection_pool->get(connection_parameters.timeouts, nullptr, true);

            connection_pool_entry->getServerVersion(connection_parameters.timeouts, server_name, server_version_major,
                                                    server_version_minor, server_version_patch, server_revision);

            server_version = DB::toString(server_version_major) + "." + DB::toString(server_version_minor) + "." +
                DB::toString(server_version_patch);

            if (server_display_name = connection_pool_entry->getServerDisplayName(connection_parameters.timeouts);
                server_display_name.empty()) {
                server_display_name = connection_parameters.host;
            }

            initialized = true;

            // server display name is the server name, for example,  "graphdb-1-2454883.lvs02.dev.ebayc3.com".
            LOG_AGGRPROC(3) << "Connected to " << server_name << " belonging to server: " << server_display_name
                            << " server version " << server_version << " revision " << server_revision << ".";
        } catch (DB::Exception& ex) {
            std::stringstream description;
            LOG(ERROR) << "Encountered Exception: " << ex.what() << ", " << ex.displayText();

            description << " Failed to connect to server with connection settings: " << connection_parameters
                        << " from a pooled connection";
            throw DB::Exception(description.str(), ErrorCodes::FAILED_TO_CONNECT_TO_SERVER);
        }
    }

    return initialized;
}

void AggregatorLoader::onException(const DB::Exception& e) {
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    if (std::string::npos != embedded_stack_trace_pos)
        text.resize(embedded_stack_trace_pos);

    LOG(ERROR) << "Received exception "
               << "Code: " << e.code() << ". " << text;

    // copy last exception is done in each of onException() caller.
    loader_state_machine->setExceptionEncountered();
    loader_state_machine->setLastErrorCode(e.code());
}

void AggregatorLoader::initLogsOutputStream() {
    if (!logs_out_stream) {
        logs_out_stream = std::make_shared<LoaderLogOutputStream>();
        logs_out_stream->initLogOutputStream();
    }
}

void AggregatorLoader::initBlockOutputStream(const DB::Block& block) {
    if (!block_out_stream) {
        block_out_stream = std::make_shared<LoaderBlockOutputStream>(context);
        block_out_stream->initBlockOutputStream(block);
    }
}

void AggregatorLoader::onData(DB::Block& block) {
    LOG_AGGRPROC(4) << "Start OnData event processing....";
    if (!block) {
        // Note: for select queries, such as describe tables, or describe table <table name>, the data block is followed
        // by an empty data block to signal the end of the query results.
        LOG_AGGRPROC(3) << "OnData identified that block received is empty, finish OnData event processing";
        return;
    } else {
        LOG_AGGRPROC(3) << "OnData identified that block received is not empty";
        std::string structure = block.dumpStructure();
        LOG_AGGRPROC(3) << "OnData shows structure dumped: " << structure;
    }

    processed_rows += block.rows();

    LOG_AGGRPROC(3) << "OnData identified that block received has number of rows: " << block.rows();

    initBlockOutputStream(block);

    /// The header block containing zero rows was used to initialize block_out_stream, do not output it.
    if (block.rows() != 0) {
        block_out_stream->write(block);
    }

    /// Received data block is immediately displayed to the log
    std::string str_out = block_out_stream->flush();
    LOG_AGGRPROC(4) << "OnData has data block flushed: " << str_out;

    LOG_AGGRPROC(3) << "Finish OnData event processing....";
}

void AggregatorLoader::onLogData(DB::Block& block) {
    initLogsOutputStream();
    logs_out_stream->write(block);
    std::string str_out = logs_out_stream->flush();
    LOG_AGGRPROC(4) << "Log Data flushed: " << str_out;
}

void AggregatorLoader::onTotals(DB::Block& block) {
    LOG_AGGRPROC(3) << "Enter OnTotal event processing....";
    initBlockOutputStream(block);
    block_out_stream->setTotals(block);

    LOG_AGGRPROC(3) << "Finish OnTotal event processing....";
}

void AggregatorLoader::onExtremes(DB::Block& block) {
    LOG_AGGRPROC(3) << "Enter OnExtremes event processing....";
    initBlockOutputStream(block);
    block_out_stream->setExtremes(block);

    LOG_AGGRPROC(3) << "Finish OnExtremes event processing....";
}

void AggregatorLoader::onProgress(const DB::Progress& value) {
    if (block_out_stream) {
        block_out_stream->onProgress(value);
    }
}

void AggregatorLoader::onProfileInfo(const DB::BlockStreamProfileInfo&) {}

// Flush all buffers
void AggregatorLoader::resetOutput() {
    block_out_stream.reset();
    logs_out_stream.reset();
}

void AggregatorLoader::onEndOfStream() {
    loader_state_machine->setEndStreamReached();

    LOG_AGGRPROC(4) << "Start OnEndOfStream ....";
    if (block_out_stream)
        block_out_stream->writeSuffix();

    if (logs_out_stream)
        logs_out_stream->writeSuffix();

    resetOutput();

    LOG_AGGRPROC(4) << "Finish OnEndOfStream ....";
}

// to retrieve all of the table names under the default database
bool AggregatorLoader::getDefinedTables(DB::Block& query_result) {
    loader_state_machine = std::make_shared<GetTableDefinitionStateMachine>();

    std::string get_table_definition_query = "show tables;";
    LOG_AGGRPROC(3) << "before execute table query to retrieve the defined tables for default database: ";
    std::string table_name = "default";
    executeTableQuery(table_name, get_table_definition_query, query_result);

    bool status = loader_state_machine->evaluate();
    LOG_AGGRPROC(3) << "after execute table query to retrieve all of the defined tables with status: " << status;
    if (status) {
        std::shared_ptr<GetTableDefinitionStateMachine> sm =
            std::static_pointer_cast<GetTableDefinitionStateMachine>(loader_state_machine);
        sm->loadQueryResult(query_result);
    }
    return status;
}

// to retrieve the sample block header that defines the table schema.
bool AggregatorLoader::getTableDefinition(const std::string& table_name, DB::Block& query_result) {
    if (!initialized) {
        init();
    }

    loader_state_machine = std::make_shared<GetTableDefinitionStateMachine>();

    std::string get_table_definition_query = "describe table " + table_name;
    LOG_AGGRPROC(3) << "before execute table query to retrieve table definition for table: " << table_name;
    executeTableQuery(table_name, get_table_definition_query, query_result);

    bool status = loader_state_machine->evaluate();
    LOG_AGGRPROC(3) << "after execute table query to retrieve table definition for table: " << table_name
                    << " with status: " << status;
    if (status) {
        std::shared_ptr<GetTableDefinitionStateMachine> sm =
            std::static_pointer_cast<GetTableDefinitionStateMachine>(loader_state_machine);
        sm->loadQueryResult(query_result);
    }
    return status;
}

bool AggregatorLoader::executeTableSelectQuery(const std::string& table_name, const std::string& query,
                                               DB::Block& query_result, int& error_code) {
    if (!initialized) {
        init();
    }

    loader_state_machine = std::make_shared<SelectQueryStateMachine>();

    LOG_AGGRPROC(3) << "before execute select query for table: " << table_name << " with query: " << query;
    executeTableQuery(table_name, query, query_result);

    bool status = loader_state_machine->evaluate();
    LOG_AGGRPROC(3) << "after execute select query for table: " << table_name << " with query: " << query
                    << " with status: " << status;
    if (status) {
        std::shared_ptr<SelectQueryStateMachine> sm =
            std::static_pointer_cast<SelectQueryStateMachine>(loader_state_machine);
        sm->loadQueryResult(query_result);
    }
    error_code = loader_state_machine->getLastErrorCode();
    return status;
}

bool AggregatorLoader::executeTableCreation(const std::string& table_name, const std::string& creation_query) {
    if (!initialized) {
        init();
    }

    loader_state_machine = std::make_shared<CreateDropTableStateMachine>();

    LOG_AGGRPROC(3) << "before execute create table query for table: " << table_name
                    << " with query: " << creation_query;
    DB::Block query_result; // no result returned actually.
    executeTableQuery(table_name, creation_query, query_result);

    bool status = loader_state_machine->evaluate();
    LOG_AGGRPROC(3) << "after execute create table query for table: " << table_name << " with query: " << creation_query
                    << " with status: " << status;

    // no actual query result returned.
    return status;
}

bool AggregatorLoader::executeTableDeletion(const std::string& table_name) {
    if (!initialized) {
        init();
    }

    loader_state_machine = std::make_shared<CreateDropTableStateMachine>();

    std::string drop_table_query = "drop table " + table_name;
    LOG_AGGRPROC(3) << "before execute drop table query for table: " << table_name
                    << " with query: " << drop_table_query;

    DB::Block query_result; // no result returned actually.
    executeTableQuery(table_name, drop_table_query, query_result);
    bool status = loader_state_machine->evaluate();
    LOG_AGGRPROC(3) << "after execute create table query for table: " << table_name
                    << " with query: " << drop_table_query << " with status: " << status;
    return status;
}

// to commonly shared execute queries
void AggregatorLoader::executeTableQuery(const std::string& table_name, const std::string& query,
                                         DB::Block& query_result) {
    bool result = false;
    const char* begin = query.data();
    DB::ASTPtr parsed_query = parseInsertQuery(begin, begin + query.size(), false);
    if (!parsed_query) {
        std::string err_msg = "can not parse query: " + query + " for table: " + table_name;
        LOG(ERROR) << "can not parse query: " << query << "for table: " << table_name;

        throw DB::Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
    }

    LOG_AGGRPROC(3) << "before sending empty query for table column definitions. ";
    // this is a single query, and thus no pending data for the last parameter.
    connection_pool_entry->sendQuery(connection_parameters.timeouts, query, query_id,
                                     DB::QueryProcessingStage::Complete, &context->getSettingsRef(), nullptr, false);

    LOG_AGGRPROC(3) << "after sending empty query for table column definitions. ";
    /// Receive description of table structure.
    receiveQueryResult(query);

    LOG_AGGRPROC(3) << "finish receiving table query result  " << result;
}

// to receive the query from  a simple insert query. The insert query carries data.
bool AggregatorLoader::load_buffer(const std::string& table_name, const std::string& query) {
    if (!initialized) {
        init();
    }

    loader_state_machine = std::make_shared<LoadInsertDataStateMachine>();
    bool result = false;
    const char* begin = query.data();
    DB::ASTPtr parsed_query = parseInsertQuery(begin, begin + query.size(), false);
    if (!parsed_query) {
        std::string err_msg = "can not parse query: " + query + " for table: " + table_name;
        LOG(ERROR) << "can not parse query: " << query << "for table: " << table_name;

        throw DB::Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
    }

    /// Send part of query without data, because data will be sent separately.
    /// Warning what does parsed_insert_query.data - query.data() result to?
    const auto& parsed_insert_query = parsed_query->as<DB::ASTInsertQuery&>();
    std::string query_without_data =
        parsed_insert_query.data ? query.substr(0, parsed_insert_query.data - query.data()) : query;

    if (!parsed_insert_query.data) {
        throw DB::Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);
    }

    // this is a single query, and thus no pending data for the last parameter.
    connection_pool_entry->sendQuery(connection_parameters.timeouts, query_without_data, query_id,
                                     DB::QueryProcessingStage::Complete, &context->getSettingsRef(), nullptr, false);

    /// Receive description of table structure.
    DB::Block sample;
    DB::ColumnsDescription columns_description;
    if (receiveSampleBlock(sample, columns_description)) {
        /// If structure was received (thus, server has not thrown an exception),
        /// send our data with that structure.
        LOG_AGGRPROC(3) << "finish receiving sample block and column description";

        DB::ReadBufferFromMemory data_in(parsed_insert_query.data, parsed_insert_query.end - parsed_insert_query.data);
        sendData(sample, columns_description, data_in, parsed_query);
        LOG_AGGRPROC(3) << "finish send data in the insert query";

        result = receiveEndOfQuery();
    } else {
        LOG(ERROR) << "Failed to retrieve sample block";
        return false;
    }

    result = loader_state_machine->evaluate();
    return result;
}

// load data via insert query, the insert query does not carry data. The data is in the block.
bool AggregatorLoader::load_buffer(const std::string& table_name, const std::string& query, const DB::Block& block,
                                   int& error_code) {
    if (!initialized) {
        init();
    }

    loader_state_machine = std::make_shared<LoadInsertDataStateMachine>();

    bool result = false;
    LOG_AGGRPROC(3) << " loader received insert query: " << query << " for table: " << table_name;

    const char* begin = query.data();
    DB::ASTPtr parsed_query = parseInsertQuery(begin, begin + query.size(), false);

    if (!parsed_query) {
        std::string err_msg = "can not parse query: " + query + " for table: " + table_name;
        LOG(ERROR) << "Can not parse query: " << query << " for table: " << table_name;

        throw DB::Exception(err_msg, ErrorCodes::BAD_ARGUMENTS);
    }

    /// Send part of query without data, because data will be sent separately.
    /// Warning what does parsed_insert_query.data - query.data() result to?
    const auto& parsed_insert_query = parsed_query->as<DB::ASTInsertQuery&>();
    std::string query_without_data =
        parsed_insert_query.data ? query.substr(0, parsed_insert_query.data - query.data()) : query;

    // NOTE: this is OK, as we have the block to contain the actual data, query is just to have the server to return
    // table definition as part of the protocol.
    // if (!parsed_insert_query.data) {
    //    LOG(INFO) << "no data to insert, that is OK, as we have the block..... ";
    // throw DB::Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);
    //}

    LOG_AGGRPROC(3) << "Sending empty query for column definitions";

    // this is a single query, and thus no pending data for the last parameter.
    connection_pool_entry->sendQuery(connection_parameters.timeouts, query_without_data, query_id,
                                     DB::QueryProcessingStage::Complete, &context->getSettingsRef(), nullptr, false);

    LOG_AGGRPROC(3) << "Finish sending empty query and receiving column definitions";

    /// Receive description of table structure.
    DB::Block sample;
    DB::ColumnsDescription columns_description;
    if (receiveSampleBlock(sample, columns_description)) {
        LOG_AGGRPROC(3) << "Sending data block ...";
        connection_pool_entry->sendData(block);

        processed_rows += block.rows();
        LOG_AGGRPROC(4) << "Finished sending data block";

        connection_pool_entry->sendData(DB::Block());
        LOG_AGGRPROC(4) << "Finish sending empty block as end of block insertion";

        result = receiveEndOfQuery();
        if (!result) {
            LOG(ERROR) << "Failed to receive end of query";
        }
        result = loader_state_machine->evaluate();
        error_code = loader_state_machine->getLastErrorCode();
        if (result) {
            LOG_AGGRPROC(3) << "Finished sending data block";
            return true;
        } else {
            LOG(ERROR) << "Failed to sending data block, error code: " << error_code;
            return false;
        }
    } else {
        LOG(ERROR) << "Failed to retrieve sample block";
        error_code = loader_state_machine->getLastErrorCode();
        return false;
    }
}

void AggregatorLoader::sendDataFrom(DB::ReadBuffer& buf, const DB::Block& sample,
                                    const DB::ColumnsDescription& columns_description, DB::ASTPtr parsed_query) {
    std::string current_format = "Values";

    /// Data format can be specified in the INSERT query.
    if (const auto* insert = parsed_query->as<DB::ASTInsertQuery>()) {
        if (!insert->format.empty())
            current_format = insert->format;
    }

    LOG_AGGRPROC(4) << "current format is: " << current_format;
    size_t insert_format_max_block_size = context->getSettingsRef().max_insert_block_size;
    DB::BlockInputStreamPtr block_input =
        context->getInputFormat(current_format, buf, sample, insert_format_max_block_size);

    if (columns_description.hasDefaults()) {
        block_input = std::make_shared<DB::AddingDefaultsBlockInputStream>(block_input, columns_description, context);
    }

    DB::BlockInputStreamPtr async_block_input = std::make_shared<DB::AsynchronousBlockInputStream>(block_input);

    async_block_input->readPrefix();

    while (true) {
        DB::Block block = async_block_input->read();
        connection_pool_entry->sendData(block);
        processed_rows += block.rows();

        /// Check if server send Log packet
        auto packet_type = connection_pool_entry->checkPacket();
        if (packet_type && *packet_type == DB::Protocol::Server::Log)
            receiveAndProcessPacket();

        if (!block)
            break;
    }

    async_block_input->readSuffix();

    LOG_AGGRPROC(3) << "Send blocks with total rows: " << processed_rows;
}

void AggregatorLoader::sendData(DB::Block& sample, const DB::ColumnsDescription& columns_description,
                                DB::ReadBuffer& buffer, DB::ASTPtr parsed_query) {
    sendDataFrom(buffer, sample, columns_description, parsed_query);
}

/// Process Log packets, exit when receive Exception or EndOfStream
bool AggregatorLoader::receiveEndOfQuery() {
    LOG_AGGRPROC(4) << "Receiving end of query";
    while (true) {
        DB::Packet packet = connection_pool_entry->receivePacket();

        switch (packet.type) {
        case DB::Protocol::Server::EndOfStream:
            LOG_AGGRPROC(4) << "Received EndOfStream";
            onEndOfStream();
            return true;

        case DB::Protocol::Server::Exception:
            LOG_AGGRPROC(4) << "Received Exception";
            onException(*packet.exception);
            loader_state_machine->setLastException(packet.exception);
            return false;

        case DB::Protocol::Server::Log:
            LOG_AGGRPROC(4) << "Received Log";
            onLogData(packet.block);
            break;

        default:
            LOG_AGGRPROC(4) << "Received invalid type of packet " << DB::Protocol::Server::toString(packet.type);
            throw DB::NetException("Unexpected packet from server (expected Exception, EndOfStream or Log, got " +
                                       std::string(DB::Protocol::Server::toString(packet.type)) + ")",
                                   ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
        }
    }
}

void AggregatorLoader::receiveQueryResult(const std::string& query) {
    LOG_AGGRPROC(3) << "Receiving query result ...";
    DB::InterruptListener interrupt_listener;
    bool cancelled = false;

    // TODO: get the poll_interval from commandline.
    const auto receive_timeout = connection_parameters.timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1000000; /// in microseconds
    constexpr size_t min_poll_interval = 5000;        /// in microseconds
    const size_t poll_interval =
        std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));

    while (true) {
        Stopwatch receive_watch(CLOCK_MONOTONIC_COARSE);

        while (true) {
            /// Has the Ctrl+C been pressed and thus the query should be cancelled?
            /// If this is the case, inform the server about it and receive the remaining packets
            /// to avoid losing sync.
            if (!cancelled) {
                auto cancelQuery = [&] {
                    connection_pool_entry->sendCancel();
                    cancelled = true;
                    loader_state_machine->setOperationCancelled();
                    /// Pressing Ctrl+C twice results in shut down.
                    interrupt_listener.unblock();
                };

                if (interrupt_listener.check()) {
                    cancelQuery();
                } else {
                    double elapsed = receive_watch.elapsedSeconds();
                    if (elapsed > receive_timeout.totalSeconds()) {
                        LOG(WARNING) << "Timeout exceeded while receiving data from server."
                                     << " Waited for " << static_cast<size_t>(elapsed) << " seconds,"
                                     << " timeout is " << receive_timeout.totalSeconds() << " seconds.";

                        cancelQuery();
                    }
                }
            }

            /// Poll for changes after a cancellation check, otherwise it never reached
            /// because of progress updates from server.
            if (connection_pool_entry->poll(poll_interval))
                break;
        }

        if (!receiveAndProcessPacket())
            break;
    }

    if (cancelled) {
        LOG(ERROR) << "Query: " << query << " was cancelled"; // need to say how long has ben waiting.
    }

    LOG_AGGRPROC(3) << "Finished receiving query result";
}

/// Receive the block that serves as an example of the structure of table where data will be inserted.
bool AggregatorLoader::receiveSampleBlock(DB::Block& out, DB::ColumnsDescription& columns_description) {
    LOG_AGGRPROC(4) << "In Aggregator Loader, enter receiveSampleBlock....";
    std::shared_ptr<LoadInsertDataStateMachine> sm =
        std::static_pointer_cast<LoadInsertDataStateMachine>(loader_state_machine);

    while (true) {
        DB::Packet packet = connection_pool_entry->receivePacket();

        LOG_AGGRPROC(4) << "After connection received a packet from server";

        switch (packet.type) {
        case DB::Protocol::Server::EndOfStream:
            LOG_AGGRPROC(3) << "SampleBlock: receiving end of stream";
            onEndOfStream();
            return true;

        case DB::Protocol::Server::Data:
            LOG_AGGRPROC(3) << "SampleBlock: receiving Data";

            out = packet.block;
            sm->setSampleHeaderReceived();
            return true;

        case DB::Protocol::Server::Exception:
            LOG_AGGRPROC(3) << "SampleBlock: receiving Exception";

            onException(*packet.exception);
            loader_state_machine->setLastException(packet.exception);
            return false;

        case DB::Protocol::Server::Log:
            LOG_AGGRPROC(3) << "SampleBlock: receiving Log";

            onLogData(packet.block);
            break;

        case DB::Protocol::Server::TableColumns:
            columns_description = DB::ColumnsDescription::parse(packet.multistring_message[1]);
            LOG_AGGRPROC(3) << "SampleBlock: receiving TableColumns.";

            // Should only mark definitions received when fully succeeded.
            if (receiveSampleBlock(out, columns_description)) {
                sm->setColumnDefinitionsReceived(true);
                return true;
            } else {
                sm->setColumnDefinitionsReceived(false);
                return false;
            }
        default: {
            LOG_AGGRPROC(3) << "SampleBlock: receiving unexpected packet from server."
                            << std::string(DB::Protocol::Server::toString(packet.type));

            throw DB::NetException("Unexpected packet from server (expected Data, Exception or Log, got " +
                                       std::string(DB::Protocol::Server::toString(packet.type)) + ")",
                                   ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
        }
        }
    }
}

/**
 * It turns on that for a table definition, we will receive three data blocks.
 *
    I0207 15:33:09.788295  2044 AggregatorLoader.cpp:323]  In AggregatorLoader enter OnData event processing....
    I0207 15:33:09.788322  2044 AggregatorLoader.cpp:329] in AggregatorLoader, OnData identified that block received is
 not empty I0207 15:33:09.788354  2044 AggregatorLoader.cpp:331] In AggregatorLoader, OnData shows structure dumped:
 name String String(size = 0), type String String(size = 0), default_type String String(size = 0), default_expression
 String String(size = 0), comment String String(size = 0), codec_expression String String(size = 0), ttl_expression
 String String(size = 0) I0207 15:33:09.788370  2044 AggregatorLoader.cpp:336] in AggregatorLoader, OnData identified
 that block received has number of rows: 0 I0207 15:33:09.788522  2044 AggregatorLoader.cpp:348]  In AggregatorLoader
 exit OnData event processing.... I0207 15:33:09.788594  2044 AggregatorLoader.cpp:323]  In AggregatorLoader enter
 OnData event processing.... I0207 15:33:09.788605  2044 AggregatorLoader.cpp:329] in AggregatorLoader, OnData
 identified that block received is not empty I0207 15:33:09.788620  2044 AggregatorLoader.cpp:331] In AggregatorLoader,
 OnData shows structure dumped: name String String(size = 3), type String String(size = 3), default_type String
 String(size = 3), default_expression String String(size = 3), comment String String(size = 3), codec_expression String
 String(size = 3), ttl_expression String String(size = 3) I0207 15:33:09.788631  2044 AggregatorLoader.cpp:336] in
 AggregatorLoader, OnData identified that block received has number of rows: 3 Count	UInt64 Host	String Colo
 String I0207 15:33:09.788695  2044 AggregatorLoader.cpp:348]  In AggregatorLoader exit OnData event processing....
    I0207 15:33:09.788720  2044 AggregatorLoader.cpp:323]  In AggregatorLoader enter OnData event processing....
    E0207 15:33:09.788729  2044 AggregatorLoader.cpp:325] in AggregatorLoader, OnData identified that block received is
 empty, exist OnData event processing

    Corresponding, we need to have query_block_received to be true, only when the rows are not empty.


    and for table query with select count (*) from table <table name>

    I0208 15:18:05.722509 25925 AggregatorLoader.cpp:352] in AggregatorLoader, OnData identified that block received is
 not empty I0208 15:18:05.722561 25925 AggregatorLoader.cpp:354] In AggregatorLoader, OnData shows structure dumped:
 count() UInt64 UInt64(size = 0) I0208 15:18:05.722573 25925 AggregatorLoader.cpp:359] in AggregatorLoader, OnData
 identified that block received has number of rows: 0 I0208 15:18:05.722738 25925 AggregatorLoader.cpp:371]  In
 AggregatorLoader exit OnData event processing.... I0208 15:18:05.722788 25925 AggregatorLoader.cpp:346]  In
 AggregatorLoader enter OnData event processing.... I0208 15:18:05.722800 25925 AggregatorLoader.cpp:352] in
 AggregatorLoader, OnData identified that block received is not empty I0208 15:18:05.722812 25925
 AggregatorLoader.cpp:354] In AggregatorLoader, OnData shows structure dumped: count() UInt64 UInt64(size = 1) I0208
 15:18:05.722821 25925 AggregatorLoader.cpp:359] in AggregatorLoader, OnData identified that block received has number
 of rows: 1 2320 I0208 15:18:05.722868 25925 AggregatorLoader.cpp:371]  In AggregatorLoader exit OnData event
 processing.... I0208 15:18:05.722919 25925 AggregatorLoader.cpp:346]  In AggregatorLoader enter OnData event
 processing.... E0208 15:18:05.722931 25925 AggregatorLoader.cpp:348] in AggregatorLoader, OnData identified that block
 received is empty, exist OnData event processing
 */
void AggregatorLoader::holdData(DB::Block& block) { loader_state_machine->loadData(block); }

// we can have more debugging information to be
bool AggregatorLoader::receiveAndProcessPacket() {
    DB::Packet packet = connection_pool_entry->receivePacket();

    switch (packet.type) {
    case DB::Protocol::Server::Data:
        onData(packet.block);
        holdData(packet.block);
        return true;

    case DB::Protocol::Server::Progress:
        onProgress(packet.progress);
        return true;

    case DB::Protocol::Server::ProfileInfo:
        onProfileInfo(packet.profile_info);
        return true;

    case DB::Protocol::Server::Totals:
        onTotals(packet.block);
        return true;

    case DB::Protocol::Server::Extremes:
        onExtremes(packet.block);
        return true;

    case DB::Protocol::Server::Exception:
        onException(*packet.exception);
        loader_state_machine->setLastException(packet.exception);
        return false;

    case DB::Protocol::Server::Log:
        onLogData(packet.block);
        return true;

    case DB::Protocol::Server::EndOfStream:
        onEndOfStream();
        return false;

    default:
        throw DB::Exception("Unknown packet from server", ErrorCodes::UNKNOWN_PACKET_FROM_SERVER);
    }
}

} // namespace nuclm
