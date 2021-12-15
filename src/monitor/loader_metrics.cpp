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

#include "loader_metrics.hpp"

namespace nuclm {

const std::string LoaderMetrics::ConnectionTo_DB_Metric_Name = "nucolumnar_aggregator_connection_to_database_ok";
const std::string LoaderMetrics::NumberOfTablesRetrievedFrom_DB_Metric_Name =
    "nucolumnar_aggregator_number_of_table_definitions_retrieved";
const std::string LoaderMetrics::NumberOfBatchedMessagesReceived_Metric_Name =
    "nucolumnar_aggregator_batched_messages_received_total";
const std::string LoaderMetrics::TotalBytesFromBatchedMessagesReceived_Metric_Name =
    "nucolumnar_aggregator_batched_messages_received_bytes_total";
const std::string LoaderMetrics::TotalBytesFromBatchedMessagesProcessed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_processed_bytes_total";
const std::string LoaderMetrics::TotalBytesFromBatchedMessagesStored_Metric_Name =
    "nucolumnar_aggregator_batched_messages_stored_bytes_total";
const std::string LoaderMetrics::TotalRowsFromBatchedMessagesStored_Metric_Name =
    "nucolumnar_aggregator_batched_messages_stored_rows_total";
const std::string LoaderMetrics::TotalBlocksFromBatchedMessagesStored_Metric_Name =
    "nucolumnar_aggregator_batched_messages_stored_blocks_total";
const std::string LoaderMetrics::TotalBlocksFromBatchedMessagesPersisted_Metric_Name =
    "nucolumnar_aggregator_batched_messages_persisted_blocks_total";

const std::string LoaderMetrics::BytesInBlocksFromBatchedMessages_Metric_Name =
    "nucolumnar_aggregator_bytes_in_stored_blocks";
const std::string LoaderMetrics::RowsInBlocksFromBatchedMessages_Metric_Name =
    "nucolumnar_aggregator_rows_in_stored_blocks";

const std::string LoaderMetrics::BlockLoadingTime_Metric_Name =
    "nucolumnar_aggregator_block_loading_time_in_microseconds";
const std::string LoaderMetrics::BlockLoadingNumberOfRetries_Metric_Name =
    "nucolumnar_aggregator_block_loading_number_of_retries";

const std::string LoaderMetrics::NumberOfColumnsInTables_Metric_Name =
    "nucolumnar_aggregator_number_of_columns_in_tables";

const std::string LoaderMetrics::NumberOfBlocksFailedToBePersisted_Metric_Name =
    "nucolumnar_aggregator_blocks_failed_to_be_persisted_total";
const std::string LoaderMetrics::NumberOfBatchedMessagesFailedToBeProcessed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_failed_processing_total";
const std::string LoaderMetrics::BytesOfBatchedMessagesFailedToBeProcessed_Metric_Name =
    "nucolumnar_aggregator_batched_messages_failed_processing_bytes_total";
const std::string LoaderMetrics::BytesOfBlocksFailedToBePersisted_Metric_Name =
    "nucolumnar_aggregator_blocks_failed_to_be_persisted_bytes_total";
const std::string LoaderMetrics::NumberOfErrorsPassedFromClickHouseServer_Metric_Name =
    "nucolumnar_aggregator_errors_from_clickhouse_total";

const std::string LoaderMetrics::EndToEndLagTimeObservedAtLoader_Metric_Name =
    "nucolumnar_aggregator_end_to_end_lag_time_at_loader";

LoaderMetrics::LoaderMetrics(monitor::NuDataMetricsFactory& factory) {
    // metric: ConnectionTo_DB_Metric_Name
    connection_to_db_metrics = &factory.registerMetric<monitor::_gauge>(
        ConnectionTo_DB_Metric_Name, "nucolumnar aggregator connection to backend is ok", {"database"});

    // metric: NumberOfTablesRetrievedFrom_DB_Metric_Name
    number_of_tables_retrieved_from_db_metrics = &factory.registerMetric<monitor::_gauge>(
        NumberOfTablesRetrievedFrom_DB_Metric_Name, "number of table definitions retrieved from backend database",
        {"database"});

    // metric: NumberOfBatchedMessageReceived_Metric_Name
    // NOTE: need to have partition and topic to be part of the labels as well.
    total_batched_kafka_messages_received_metrics = &factory.registerMetric<monitor::_counter>(
        NumberOfBatchedMessagesReceived_Metric_Name, "total number of batched kafka messages received by the buffers",
        {"table"});

    // metric:  TotalBytesFromBatchedMessageReceived_Metric_Name
    total_bytes_from_batched_kafka_messages_received_metrics = &factory.registerMetric<monitor::_counter>(
        TotalBytesFromBatchedMessagesReceived_Metric_Name,
        "total bytes from batched kafka messages received by the buffers", {"table"});

    // metric: TotalBytesFromBatchedMessageProcessed_Metric_Name
    total_bytes_from_batched_kafka_messages_processed_metrics = &factory.registerMetric<monitor::_counter>(
        TotalBytesFromBatchedMessagesProcessed_Metric_Name,
        "total bytes from batched kafka messages processed by the buffers", {"table"});

    // metric: TotalBytesFromBatchedMessageStored_Metric_Name
    total_bytes_from_batched_kafka_messages_stored_metrics = &factory.registerMetric<monitor::_counter>(
        TotalBytesFromBatchedMessagesStored_Metric_Name,
        "total bytes from batched kafka messages stored by the buffers in blocks", {"table"});

    // metric: TotalRowsFromBatchedMessageStored_Metric_Name;
    total_rows_from_batched_kafka_messages_stored_metrics = &factory.registerMetric<monitor::_counter>(
        TotalRowsFromBatchedMessagesStored_Metric_Name,
        "total rows from batched kafka messages stored by the buffers in blocks", {"table"});

    // metric: TotalBlocksFromBatchedMessagesStored_Metric_Name
    // this metric reflects the blocks constructed from the buffer and now are ready to be sent to the backend server.
    // it can still fail to be persisted.
    total_blocks_from_batched_kafka_messages_stored_metrics = &factory.registerMetric<monitor::_counter>(
        TotalBlocksFromBatchedMessagesStored_Metric_Name,
        "total blocks from batched kafka messages stored by the buffers", {"table"});

    // metric: TotalBlocksFromBatchedMessagesPersisted_Metric_Name
    total_blocks_from_batched_kafka_messages_persisted_metrics = &factory.registerMetric<monitor::_counter>(
        TotalBlocksFromBatchedMessagesPersisted_Metric_Name,
        "total blocks from batched kafka messages persisted to the backend database", {"table"});

    // metric: BytesInBlocksFromBatchedMessages_Metric_Name
    bytes_in_blocks_from_batched_kafka_messages_stored_metrics = &factory.registerMetric<monitor::_histogram>(
        BytesInBlocksFromBatchedMessages_Metric_Name, "bytes from batched kafka messages in the stored blocks",
        {"table"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: RowsInBlocksFromBatchedMessages_Metric_Name
    rows_in_blocks_from_batched_kafka_messages_stored_metrics = &factory.registerMetric<monitor::_histogram>(
        RowsInBlocksFromBatchedMessages_Metric_Name, "total rows from batched kafka messages in the stored blocks",
        {"table"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: BlockLoadingTime_Metric_Name, > 20 seconds, thus choose exponential-of-2 buckets instead.
    block_loading_time_metrics = &factory.registerMetric<monitor::_histogram>(
        BlockLoadingTime_Metric_Name, "block loading time from buffer to backend database in microseconds", {"table"},
        monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: BlockLoadingNumberOfRetries_Metric_Name
    block_loading_number_of_retries_metrics = &factory.registerMetric<monitor::_histogram>(
        BlockLoadingNumberOfRetries_Metric_Name, "block loading number of retries from buffer to backend database",
        {"table"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);

    // metric: NumberOfColumnsInTables_Metric_Name
    number_of_columns_in_tables_metrics = &factory.registerMetric<monitor::_gauge>(
        NumberOfColumnsInTables_Metric_Name, "number of columns in a defined table from backend database", {"table"});

    // metric: NumberOfBlocksFailedToBePersisted_Metric_Name
    blocks_failed_to_be_persisted_total = &factory.registerMetric<monitor::_counter>(
        NumberOfBlocksFailedToBePersisted_Metric_Name,
        "total number of blocks failed to be persisted to backend database", {"table"});

    // metric: NumberOfBatchedMessagesFailedToBeProcessed_Metric_Name
    messages_failed_to_be_processed_total = &factory.registerMetric<monitor::_counter>(
        NumberOfBatchedMessagesFailedToBeProcessed_Metric_Name,
        "total number of batched messages failed to be processed by buffers", {"table"});

    // metric: BytesOfBatchedMessagesFailedToBeProcessed_Metric_Name;
    bytes_failed_to_be_processed_total = &factory.registerMetric<monitor::_counter>(
        BytesOfBatchedMessagesFailedToBeProcessed_Metric_Name,
        "total byes for batched messages failed to be processed by buffers", {"table"});

    // metric: BytesOfBatchedMessagesFailedToBePersisted_Metric_Name;
    bytes_of_blocks_failed_to_be_persisted_total = &factory.registerMetric<monitor::_counter>(
        BytesOfBlocksFailedToBePersisted_Metric_Name, "total bytes in blocks failed to be loaded to backend database",
        {"table"});

    // metric: NumberOfErrorsPassedFromClickHouseServer_Metric_Name
    errors_passed_from_clickhouse_total = &factory.registerMetric<monitor::_counter>(
        NumberOfErrorsPassedFromClickHouseServer_Metric_Name, "errors on block processing passed back from clickhouse",
        {"table", "error_code"});

    // metric: LoaderMetrics::EndToEndLagTimeObservedAtLoader_Metric_Name. It can be > 100 seconds based on the
    // estimation, and thus we need to use the exponential-of-two buckets in histogram.
    end_to_end_lag_time_at_loader = &factory.registerMetric<monitor::_histogram>(
        EndToEndLagTimeObservedAtLoader_Metric_Name, "total end-to-end lag time observed at the loader in milliseconds",
        {"table"}, monitor::HistogramBuckets::ExponentialOfTwoBuckets);
}
} // namespace nuclm
