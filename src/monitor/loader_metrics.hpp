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

#include <monitor_base/metrics.hpp>
#include <monitor_base/metrics_factory.hpp>

namespace nuclm {

struct LoaderMetrics {
    static const std::string ConnectionTo_DB_Metric_Name;
    static const std::string NumberOfTablesRetrievedFrom_DB_Metric_Name;
    static const std::string NumberOfBatchedMessagesReceived_Metric_Name;
    static const std::string TotalBytesFromBatchedMessagesReceived_Metric_Name;
    static const std::string TotalBytesFromBatchedMessagesProcessed_Metric_Name;
    static const std::string TotalBytesFromBatchedMessagesStored_Metric_Name;
    static const std::string TotalRowsFromBatchedMessagesStored_Metric_Name;
    static const std::string TotalBlocksFromBatchedMessagesStored_Metric_Name;
    static const std::string TotalBlocksFromBatchedMessagesPersisted_Metric_Name;

    static const std::string BytesInBlocksFromBatchedMessages_Metric_Name;
    static const std::string RowsInBlocksFromBatchedMessages_Metric_Name;

    static const std::string BlockLoadingTime_Metric_Name;
    static const std::string BlockLoadingNumberOfRetries_Metric_Name;

    static const std::string NumberOfColumnsInTables_Metric_Name;

    // error on block persistence
    static const std::string NumberOfBlocksFailedToBePersisted_Metric_Name;
    // error on processing messages;
    static const std::string NumberOfBatchedMessagesFailedToBeProcessed_Metric_Name;
    static const std::string BytesOfBatchedMessagesFailedToBeProcessed_Metric_Name;
    static const std::string BytesOfBlocksFailedToBePersisted_Metric_Name;
    // clickhouse errors (with error code) passed back to the aggregator
    static const std::string NumberOfErrorsPassedFromClickHouseServer_Metric_Name;

    // Lag time reported by the Aggregator for the message time stamp at the NuColumnar Service, up to the time when
    // a block gets received successfully by the ClickHouse shard
    static const std::string EndToEndLagTimeObservedAtLoader_Metric_Name;

    monitor::MetricFamily<monitor::_gauge>* connection_to_db_metrics;
    monitor::MetricFamily<monitor::_gauge>* number_of_tables_retrieved_from_db_metrics;
    monitor::MetricFamily<monitor::_counter>* total_batched_kafka_messages_received_metrics;
    monitor::MetricFamily<monitor::_counter>* total_bytes_from_batched_kafka_messages_received_metrics;

    // the bytes are counted with respect to the received kafka message
    monitor::MetricFamily<monitor::_counter>* total_bytes_from_batched_kafka_messages_processed_metrics;

    // the bytes are counted with respect to the received kafka message
    monitor::MetricFamily<monitor::_counter>* total_bytes_from_batched_kafka_messages_stored_metrics;

    // the rows stored by the buffers, which should be smaller than the received kafka message sizes
    monitor::MetricFamily<monitor::_counter>* total_rows_from_batched_kafka_messages_stored_metrics;

    // the blocks stored by the buffers.
    monitor::MetricFamily<monitor::_counter>* total_blocks_from_batched_kafka_messages_stored_metrics;

    // the blocks persisted to the backend store
    monitor::MetricFamily<monitor::_counter>* total_blocks_from_batched_kafka_messages_persisted_metrics;

    // the byte sizes distributed in the stored blocks
    monitor::MetricFamily<monitor::_histogram>* bytes_in_blocks_from_batched_kafka_messages_stored_metrics;

    // the row sizes distributed in the stored blocks
    monitor::MetricFamily<monitor::_histogram>* rows_in_blocks_from_batched_kafka_messages_stored_metrics;

    // loading time per block
    monitor::MetricFamily<monitor::_histogram>* block_loading_time_metrics;
    // number of retries to load a block
    monitor::MetricFamily<monitor::_histogram>* block_loading_number_of_retries_metrics;

    // number of columns in a table
    monitor::MetricFamily<monitor::_gauge>* number_of_columns_in_tables_metrics;

    // failure on blocks to be persisted
    monitor::MetricFamily<monitor::_counter>* blocks_failed_to_be_persisted_total;

    // failure on messages failed to be processed
    monitor::MetricFamily<monitor::_counter>* messages_failed_to_be_processed_total;
    // failure on total bytes failed to be processed before getting to the loader for persistence
    monitor::MetricFamily<monitor::_counter>* bytes_failed_to_be_processed_total;
    // failure on total bytes in blocks that failed to be processed at the loader
    monitor::MetricFamily<monitor::_counter>* bytes_of_blocks_failed_to_be_persisted_total;

    // clickhouse errors (with error code) passed back to the aggregator
    monitor::MetricFamily<monitor::_counter>* errors_passed_from_clickhouse_total;

    // end-to-end lag time
    monitor::MetricFamily<monitor::_histogram>* end_to_end_lag_time_at_loader;

    LoaderMetrics(monitor::NuDataMetricsFactory& factory);
};

} // namespace nuclm
