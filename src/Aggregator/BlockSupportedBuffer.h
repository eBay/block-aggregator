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

#include <Aggregator/AggregatorLoaderManager.h>
#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>

#include <KafkaConnector/Buffer.h>
#include <KafkaConnector/KafkaConnector.h>

#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include "TableSchemaUpdateTracker.h"

#include <climits>
#include <memory>

namespace nuclm {

/**
 * A generic buffer that can be extended to have different way to contain data to be processed for a table. The
 * buffer belongs to a table and has its own partition id.
 */
class BlockSupportedBuffer : public kafka::Buffer {

  public:
    BlockSupportedBuffer(const AggregatorLoaderManager& loader_manager_, int partition_, const std::string& table_,
                         uint32_t batch_size_, uint32_t batch_timeout_, DB::ContextMutablePtr context_,
                         kafka::KafkaConnector* kafka_connector_) :
            kafka::Buffer(partition_, table_, batch_size_, batch_timeout_),
            loader_manager(loader_manager_),
            table_definition(loader_manager_.getTableColumnsDefinition(table_)),
            full_columns_definition(table_definition.getFullColumnTypesAndNamesDefinitionCache()),
            block_holder(SerializationHelper::getBlockDefinition(full_columns_definition)),
            total_message_bytes_size{},
            total_block_bytes_size{},
            total_rows_count{},
            maximum_stream_offset{},
            total_offset_difference{},
            minmax_msg_timestamp{std::make_pair(LONG_MAX, LONG_MIN)},
            context(context_),
            kafka_connector(kafka_connector_),
            schema_update_tracker{
                std::make_shared<TableSchemaUpdateTracker>(table_, table_definition, loader_manager)} {
        assigned_buffer_id = buffer_id++;
    }

    ~BlockSupportedBuffer() override = default;

    bool append(const char* data, size_t data_size, int64_t offset, int64_t timestamp) override;

    kafka::FlushTaskPtr flush() override;

    bool flushable() override;

    bool empty() override;

  private:
    const AggregatorLoaderManager& loader_manager;
    TableColumnsDescription table_definition; // make a local copy instead to support dynamic schema update.
    ColumnTypesAndNamesTableDefinition full_columns_definition;
    DB::Block block_holder; // the actual block, initialized as the block definition

    size_t total_message_bytes_size;
    size_t total_block_bytes_size;
    size_t total_rows_count;
    int64_t maximum_stream_offset;
    int64_t total_offset_difference;

    // to capture the maximum and minimum message time stamp that are in this block
    // first element is min and second element is max
    std::pair<int64_t, int64_t> minmax_msg_timestamp;

    DB::ContextMutablePtr context;

    static std::atomic<unsigned long> buffer_id;
    std::atomic<unsigned long> assigned_buffer_id;

    kafka::KafkaConnector* kafka_connector;
    TableSchemaUpdateTrackerPtr schema_update_tracker;

    void update_maxmin_msg_timestamp(int64_t timestamp);
};

} // namespace nuclm
