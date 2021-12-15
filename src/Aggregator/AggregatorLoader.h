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

#pragma once

#ifdef __APPLE__
#include <ctime>
#define CLOCK_MONOTONIC_COARSE CLOCK_MONOTONIC
#endif

#include <Aggregator/DBConnectionParameters.h>
#include <Aggregator/LoaderConnectionPool.h>
#include <Aggregator/LoaderOutputStreamLogging.h>

#include <Client/Connection.h>
#include <Storages/ColumnsDescription.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Core/Protocol.h>
#include <IO/ConnectionTimeouts.h>

#include <common/logging.hpp>

namespace nuclm {

/**
 * The loader's execution states when performing insert queries with data,  direct block insertion and other queries
 * that include table select queries, table create/drop queries.
 */
class AggregatorLoaderStateMachine {
  public:
    AggregatorLoaderStateMachine() :
            got_exception{false},
            last_exception{nullptr},
            operation_cancelled{false},
            end_of_stream_reached{false},
            final_evaluation_result{false},
            error_code{0} {}

    virtual ~AggregatorLoaderStateMachine() {}

    void setExceptionEncountered() { got_exception = true; }

    void setLastException(std::unique_ptr<DB::Exception>& last_exception_) {
        last_exception = std::move(last_exception_);
    }

    void setOperationCancelled() { operation_cancelled = true; }
    void setEndStreamReached() { end_of_stream_reached = true; }

    void setLastErrorCode(int errcode) { error_code = errcode; }

    int getLastErrorCode() { return error_code; }

    bool getFinalEvaluationResult() { return final_evaluation_result; }

    virtual bool evaluate() = 0;
    virtual void loadData(DB::Block& block) = 0;

  protected:
    bool got_exception;
    std::unique_ptr<DB::Exception> last_exception;
    bool operation_cancelled;
    // no query result return, for block insertion and for table creation and for table drop.
    bool end_of_stream_reached;
    // the final evaluation result
    bool final_evaluation_result;
    // error code.
    int error_code;
};

class LoadInsertDataStateMachine : public AggregatorLoaderStateMachine {
  public:
    LoadInsertDataStateMachine() : received_sample_header{false}, received_column_definitions{false} {}
    ~LoadInsertDataStateMachine() = default;

    bool evaluate() override {
        final_evaluation_result = !got_exception && !operation_cancelled && received_sample_header &&
            received_column_definitions && end_of_stream_reached;
        return final_evaluation_result;
    }
    void loadData(DB::Block& block) override {
        // do nothing. we do not have output data for loading.
    }

    void setSampleHeaderReceived(bool receivedSuccess = true) { received_sample_header = receivedSuccess; }
    void setColumnDefinitionsReceived(bool receivedSuccess = true) { received_column_definitions = receivedSuccess; }

  private:
    bool received_sample_header;
    bool received_column_definitions;
};

class GetTableDefinitionStateMachine : public AggregatorLoaderStateMachine {
  public:
    GetTableDefinitionStateMachine() : received_sample_header{false}, received_table_definition{false} {}
    ~GetTableDefinitionStateMachine() = default;

    bool evaluate() override {
        final_evaluation_result = !got_exception && !operation_cancelled && received_sample_header &&
            received_table_definition && end_of_stream_reached;
        return final_evaluation_result;
    }

    void loadData(DB::Block& block) override {
        if (block) {
            // receive sample header as the first message
            if (!received_sample_header) {
                received_sample_header = true;
                sample_header.swap(block);
            } else {
                // then receive table definition as the second message.
                received_table_definition = true;
                table_definition.swap(block);
            }
        }
    }

    // transfer the result to the caller.
    void loadQueryResult(DB::Block& query_result) { query_result.swap(table_definition); }

    // for  debugging purpose.
    void loadSampleHeader(DB::Block& header) { header.swap(sample_header); }

  private:
    bool received_sample_header;
    bool received_table_definition;

    DB::Block sample_header;
    DB::Block table_definition;
};

class SelectQueryStateMachine : public AggregatorLoaderStateMachine {
  public:
    SelectQueryStateMachine() : received_sample_header{false}, received_query_result{false}, first_query_block{true} {}
    ~SelectQueryStateMachine() = default;

    bool evaluate() override {
        final_evaluation_result = !got_exception && !operation_cancelled && received_sample_header &&
            received_query_result && end_of_stream_reached;
        return final_evaluation_result;
    }

    void loadData(DB::Block& block) override {
        if (block) {
            if (!received_sample_header) {
                received_sample_header = true;
                sample_header.swap(block);
            } else {
                // then receive query result. It can have rows.size() to be 0.
                received_query_result = true;

                // need to differentiate the first time merging vs. the subsequent merging.
                if (first_query_block) {
                    query_result.swap(block);
                    first_query_block = false;
                } else {
                    // query_result.swap(block);
                    // there could be multiple data blocks as from the server, the parts can be multiple of them.
                    DB::MutableColumns current_columns = block.mutateColumns();
                    DB::MutableColumns block_columns = query_result.mutateColumns();
                    size_t number_of_columns = block_columns.size();
                    for (size_t column_index = 0; column_index < number_of_columns; column_index++) {
                        block_columns[column_index]->insertRangeFrom(*current_columns[column_index], 0,
                                                                     current_columns[column_index]->size());
                    }
                    /// construct the new blocks for query result .
                    query_result.setColumns(std::move(block_columns));
                }
            }
        }
    }

    void loadQueryResult(DB::Block& result) { result.swap(query_result); }

    // for debugging purpose
    void loadSampleHeader(DB::Block& header) { header.swap(sample_header); }

  private:
    bool received_sample_header;
    bool received_query_result;
    bool first_query_block;

    DB::Block sample_header;
    DB::Block query_result;
};

class CreateDropTableStateMachine : public AggregatorLoaderStateMachine {
  public:
    CreateDropTableStateMachine() {}
    ~CreateDropTableStateMachine() = default;

    bool evaluate() override {
        final_evaluation_result = !got_exception && !operation_cancelled && end_of_stream_reached;
        return final_evaluation_result;
    }

    void loadData(DB::Block& block) override {
        // do nothing, we will not come here.
    }
};

/**
 * The loader to load the constructed Block to the specified database server (most likely the colocated datbase server)
 */
class AggregatorLoader {

  public:
    using LoaderQueryEvaluationFunc = std::function<bool()>;

  public:
    AggregatorLoader(DB::ContextMutablePtr context_, std::shared_ptr<LoaderConnectionPool> connection_pool_,
                     const DatabaseConnectionParameters& connection_parameters_) :
            context(context_),
            connection_pool{connection_pool_},
            connection_parameters(connection_parameters_),
            initialized{false} {}

    ~AggregatorLoader() { LOG_AGGRPROC(3) << "AggregatorLoader shutdown"; }
    bool init();
    bool shutdown();
    bool run();

    // this is more for testing purpose, to load to the table from a insert query.
    bool load_buffer(const std::string& table_name, const std::string& query);

    // this is the actual loading of a block constructed.
    bool load_buffer(const std::string& table_name, const std::string& query, const DB::Block& block, int& error_code);

    bool load_buffer(const std::string& table_name, const std::string& query, const DB::Block& block) {
        int error_code = 0;
        return load_buffer(table_name, query, block, error_code);
    }

    // to retrieve the table definition
    bool getTableDefinition(const std::string& table_name, DB::Block& query_result);

    // to retrieve all of the table definition from a specified database--at this time, we use default database.
    bool getDefinedTables(DB::Block& query_result);

    // to execute query that is not about table insertion.
    bool executeTableSelectQuery(const std::string& table_name, const std::string& query, DB::Block& query_result,
                                 int& error_code);

    bool executeTableSelectQuery(const std::string& table_name, const std::string& query, DB::Block& query_result) {
        int error_code;
        return executeTableSelectQuery(table_name, query, query_result, error_code);
    };

    // to execute query that is about create/drop tables.
    bool executeTableCreation(const std::string& table_name, const std::string& creation_query);
    bool executeTableDeletion(const std::string& table_name);

    // to expose aggregator state (for debugging purpose)
    std::shared_ptr<AggregatorLoaderStateMachine> getLoaderStateMachine() const { return loader_state_machine; }

  private:
    void executeTableQuery(const std::string& table_name, const std::string& query, DB::Block& query_result);

    void receiveQueryResult(const std::string& query);

    // helper functions that we need to test out
    bool receiveSampleBlock(DB::Block& out, DB::ColumnsDescription& columns_description);
    void sendData(DB::Block& sample, const DB::ColumnsDescription& columns_description, DB::ReadBuffer& buffer,
                  DB::ASTPtr parsed_query);
    void sendDataFrom(DB::ReadBuffer& buf, const DB::Block& sample, const DB::ColumnsDescription& columns_description,
                      DB::ASTPtr parsed_query);
    bool receiveAndProcessPacket();
    bool receiveEndOfQuery();

    void initLogsOutputStream();
    void initBlockOutputStream(const DB::Block& block);

    void onData(DB::Block& block);
    void onLogData(DB::Block& block);
    void onTotals(DB::Block& block);
    void onExtremes(DB::Block& block);
    void onProgress(const DB::Progress& value);
    void onEndOfStream();
    void onProfileInfo(const DB::BlockStreamProfileInfo&);
    void onException(const DB::Exception& e);

    void resetOutput();

    void holdData(DB::Block& block);

  private:
    // Global Context
    DB::ContextMutablePtr context;

    std::shared_ptr<LoaderConnectionPool> connection_pool;
    // holding the connection for the lifetime of the Aggregator Loader.
    DB::ConnectionPool::Entry connection_pool_entry;
    const DatabaseConnectionParameters& connection_parameters;
    bool initialized;

    /// ToDo: what does the use of the query id?
    std::string query_id;

    // Server version
    std::string server_version;

    // total processed rows
    size_t processed_rows = 0;

    // block output stream
    LoaderBlockOutputStreamPtr block_out_stream;

    // log output stream
    LoaderLogOutputStreamPtr logs_out_stream;

    // to hold the execute state
    std::shared_ptr<AggregatorLoaderStateMachine> loader_state_machine;
};

} // namespace nuclm
