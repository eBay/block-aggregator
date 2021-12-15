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

#include "Aggregator/BlockSupportedBuffer.h"
#include "Aggregator/BlockSupportedBufferFlushTask.h"
#include "Aggregator/SerializationHelper.h"
#include "monitor/metrics_collector.hpp"
#include "common/settings_factory.hpp"

#include "common/logging.hpp"
#include <string>

namespace nuclm {

// the following function returns the Epoc time in nano seconds.
static int64_t now() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::atomic<unsigned long> BlockSupportedBuffer::buffer_id{0};

bool BlockSupportedBuffer::append(const char* data, size_t data_size, int64_t offset, int64_t timestamp) {
    std::shared_ptr<LoaderMetrics> loader_metrics = MetricsCollector::instance().getLoaderMetrics();
    loader_metrics->total_batched_kafka_messages_received_metrics->labels({{"table", table_definition.getTableName()}})
        .increment();
    loader_metrics->total_bytes_from_batched_kafka_messages_received_metrics
        ->labels({{"table", table_definition.getTableName()}})
        .increment(data_size);

    // keep track of the state
    if (begin_ == kafka::Metadata::EARLIEST_OFFSET) {
        begin_ = offset;
        flushedAt = now();
    }
    end_ = offset;

    bool result = false;
    try {
        std::string serialized_message(data, data_size); // NOTE: the data is copied to the message.
        // need to have per-message-serialization definition and then merging to the block_holder.

        LOG_AGGRPROC(4) << "right before loading message into a block for table: " << table_definition.getTableName()
                        << " with size: " << data_size;
        ProtobufBatchReader batchReader(serialized_message, schema_update_tracker, block_holder, context);
        result = batchReader.read();
        if (result) {
            update_maxmin_msg_timestamp(timestamp);

            LOG_AGGRPROC(4) << "loaded message into a block for table: " << table_definition.getTableName();
            size_t total_number_of_rows_so_far = block_holder.rows();
            LOG_AGGRPROC(4) << " in buffer: " << assigned_buffer_id << " in partition: " << partitionId
                            << " total number of rows in block holder: " << total_number_of_rows_so_far
                            << " minimum timestamp in buffer: " << minmax_msg_timestamp.first
                            << " maximum timestamp in buffer: " << minmax_msg_timestamp.second;

            std::string names_holder = block_holder.dumpNames();
            LOG_AGGRPROC(4) << " in buffer: " << assigned_buffer_id << " in partition: " << partitionId
                            << " column names dumped in block holder : " << names_holder;

            std::string structure = block_holder.dumpStructure();
            LOG_AGGRPROC(4) << " in buffer: " << assigned_buffer_id << " in partition: " << partitionId
                            << " structure dumped in block holder: " << structure;

            total_message_bytes_size += data_size;
            maximum_stream_offset = offset;

            total_block_bytes_size += batchReader.getBytesProcessed();
            total_rows_count += batchReader.getRowsProcessed();
            total_offset_difference = end_ - begin_;

            // still, the data size is with respect to the original message
            loader_metrics->total_bytes_from_batched_kafka_messages_processed_metrics
                ->labels({{"table", table_definition.getTableName()}})
                .increment(data_size);
            // and the bytes that are stored by the buffer, which can be smaller than the original message size
            loader_metrics->total_bytes_from_batched_kafka_messages_stored_metrics
                ->labels({{"table", table_definition.getTableName()}})
                .increment(batchReader.getBytesProcessed());
            // and the rows stored.
            loader_metrics->total_rows_from_batched_kafka_messages_stored_metrics
                ->labels({{"table", table_definition.getTableName()}})
                .increment(batchReader.getRowsProcessed());

        } else {
            LOG(WARNING) << "failed to load message into a block for table: " << table_definition.getTableName();
        }
    } catch (...) {
        result = false;
        auto code = DB::getCurrentExceptionCode();
        std::string err_msg = "BlockSupportedBuffer cannot convert received kafka message into a block";
        LOG(ERROR) << err_msg << " with exception return code: " << code
                   << ", exception: " << DB::getCurrentExceptionMessage(true);
    }

    if (!result) {
        loader_metrics->messages_failed_to_be_processed_total->labels({{"table", table}}).increment();
        loader_metrics->bytes_failed_to_be_processed_total->labels({{"table", table}}).increment(data_size);
    }
    return result;
}

kafka::FlushTaskPtr BlockSupportedBuffer::flush() {
    LOG_AGGRPROC(4) << "BlockSupportedBuffer flush entering at buffer (id): " << assigned_buffer_id;
    kafka::FlushTaskPtr task = nullptr;
    if (block_holder.rows() > 0) {
        const TableColumnsDescription& latest_table_definition = schema_update_tracker->getLatestSchema();
        task = std::make_shared<BlockSupportedBufferFlushTask>(
            partitionId, table, begin_, end_, block_holder, total_block_bytes_size, total_rows_count,
            minmax_msg_timestamp, latest_table_definition.getFullColumnTypesAndNamesDefinitionCache(), loader_manager,
            context, kafka_connector);
        block_holder.clear(); // it has been transferred to buffer in flush task.
        // then we still need to give it the header definition
        // TODO: Maybe unnecessary, as BlockSupportedBufferFlushTask will do swap.
        block_holder = SerializationHelper::getBlockDefinition(
            latest_table_definition.getFullColumnTypesAndNamesDefinitionCache());
        LOG_AGGRPROC(4) << "Buffer with id: " << assigned_buffer_id.load(std::memory_order_relaxed) << ",  " << table
                        << "[" << begin_ << "," << end_
                        << "]: Flushing non-empty buffer. After block holder being cleared, block "
                        << "number of block row (with header definition): " << block_holder.rows();
    } else {
        LOG_AGGRPROC(4) << "Buffer with id: " << assigned_buffer_id.load(std::memory_order_relaxed) << ",  " << table
                        << "[" << begin_ << "," << end_ << "]: Intend to Flush empty buffer. no Future task generated";
    }

    begin_ = kafka::Metadata::EARLIEST_OFFSET;
    flushedAt = now();

    LOG_AGGRPROC(4) << "BlockSupportedBuffer flush exiting at buffer (id):  " << assigned_buffer_id;
    return task;
}

bool BlockSupportedBuffer::flushable() {
    auto t_now = now();

    auto max_allowed_block_size_in_bytes =
        with_settings([this](SETTINGS s) { return s.config.aggregatorLoader.max_allowed_block_size_in_bytes; });

    auto max_allowed_block_size_in_rows =
        with_settings([this](SETTINGS s) { return s.config.aggregatorLoader.max_allowed_block_size_in_rows; });

    // LOG_AGGRPROC(5) << "aggregator loader max_allowed_block_size_in_bytes: " << max_allowed_block_size_in_bytes;
    // LOG_AGGRPROC(5)<< "aggregator loader max_allowed_block_size_in_rows: " << max_allowed_block_size_in_rows;
    // LOG_AGGRPROC(5) << "aggregator batch time (in ms): " << batchTimeout;

    return (block_holder.allocatedBytes() > max_allowed_block_size_in_bytes) ||
        (block_holder.rows() > max_allowed_block_size_in_rows) ||
        (t_now - flushedAt > batchTimeout * 1000000); // TODO: Potential problem for complex unit test.
}

bool BlockSupportedBuffer::empty() { return (block_holder.rows() == 0); }

// update the time-stamp only for rows that can be de-serialized correctly
void BlockSupportedBuffer::update_maxmin_msg_timestamp(int64_t timestamp) {
    if (minmax_msg_timestamp.first > timestamp) {
        minmax_msg_timestamp.first = timestamp;
    }

    if (minmax_msg_timestamp.second < timestamp) {
        minmax_msg_timestamp.second = timestamp;
    }
}
} // namespace nuclm
