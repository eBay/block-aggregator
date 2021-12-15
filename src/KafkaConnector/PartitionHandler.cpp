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

#include "PartitionHandler.h"
#include "KafkaConnector.h"
#include "RebalanceCb.h"
#include "GlobalContext.h"

#include "Metadata.h"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"
#include "monitor/metrics_collector.hpp"

#include "global.h"
#include "nlohmann/json.hpp"

#include <sstream>
#include <chrono>
#include <string>
#include <thread>

#ifdef _PRERELEASE
#include <flip/flip.hpp>
#endif

#define PART_ID(id) "KafkaConnector [" << kafkaConnector->getId() << "]: Partition [" << id << "]: "

namespace kafka {

std::string PartitionHandler::print_toppar_list(const rd_kafka_topic_partition_list_t* list) {
    std::stringstream s;
    s << "List count " << list->cnt << std::endl;
    for (int i = 0; i < list->cnt; i++) {
        const rd_kafka_topic_partition_t* a = &list->elems[i];
        s << a->topic << " [" << a->partition << "] @ " << a->offset << ": (" << a->metadata_size << ") \""
          << (const char*)a->metadata << "\"" << std::endl;
    }

    return s.str();
}

PartitionHandler::PartitionHandler(const std::string& topic_, int partition_, int64_t offset_, std::string& metadata_,
                                   KafkaConnector* kafkaConnector_) :
        topic(topic_), partitionId(partition_), kafkaConnector(kafkaConnector_) {
    savedMetadata.deserialize(metadata_);
    previousMetadata = metadata_;

    pos = offset_;
    end = savedMetadata.max();   // end is for all tables
    begin = savedMetadata.min(); // begin is for all tables, should equal to offset_;

    LOG_KAFKA(1) << PART_ID(partitionId) << "Initializing handler for topic: " << topic_
                 << " with metadata = " << metadata_ << " offset = " << offset_ << ", range = [" << begin << "," << end
                 << "]";
    // ToDo: set replica_id of the metadata here
    auto replica_id = with_settings([](SETTINGS s) { return s.config.databaseServer.replica_id; });
    status.setReplicaId(replica_id);

    // Adding buffers of the metadata
    auto tables = savedMetadata.getTables();
    for (auto& table : tables) {
        CHECK(GlobalContext::instance().getBufferFactory() != nullptr)
            << "Partition Handler can not retrieve Buffer Factory.";

        with_settings([this, &table](SETTINGS s) {
            try {
                buffers[table] = GlobalContext::instance().getBufferFactory()->createBuffer(
                    partitionId, table, s.config.kafka.consumerConf.buffer_batch_processing_size,
                    s.config.kafka.consumerConf.buffer_batch_processing_timeout_ms, kafkaConnector);
                buffers[table]->setCount(savedMetadata.getOffset(table).count);
            } catch (...) {
                LOG(ERROR) << PART_ID(partitionId) << "Exception during creating buffer for table: " << table
                           << " when initializing PartitionHandler.";
                buffers[table] = nullptr;
            }
        });

        CHECK(buffers[table] != nullptr) << "Buffer for table: " << table << " can not be created";
    }

    PartitionState state;
    if (pos <= end) {
        state = REPLAY;
    } else {
        state = CONSUME;
    }

    if (kafkaConnector_->isInConsumeMode()) {
        LOG_KAFKA(1) << PART_ID(partitionId) << "In Partition Handler for topic: " << topic_
                     << " forced to be consumer mode";
        state = CONSUME;
    } else {
        LOG_KAFKA(3) << PART_ID(partitionId) << "In Partition Handler for topic: " << topic_
                     << " no forcing to be consumer mode"
                     << " current state is: " << state;
    }
    status.init(&this->savedMetadata, offset_, state);
    LOG_KAFKA(1) << PART_ID(partitionId) << "Initialized as " << (status.getState() == REPLAY ? "REPLAY" : "CONSUME")
                 << " mode, offsets: [" << begin << ", " << end << "]. Current pos: " << pos;
}

KafkaConnectorError PartitionHandler::commitMetadata(const rd_kafka_topic_partition_list_t* toppar_to_commit,
                                                     rd_kafka_t* rk) {
    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();
    auto var_zone = with_settings([this](SETTINGS s) {
        size_t idx = kafkaConnector->getId();
        auto& var = s.config.kafka.configVariants[idx];
        return var.zone;
    });

    auto max_number_of_kafka_commit_metadata_retries = with_settings(
        [this](SETTINGS s) { return s.config.kafka.consumerConf.max_number_of_kafka_commit_metadata_retries; });

    auto kafka_commit_metadata_max_retry_delay_ms = with_settings(
        [this](SETTINGS s) { return s.config.kafka.consumerConf.kafka_commit_metadata_max_retry_delay_ms; });
    auto kafka_commit_metadata_initial_retry_delay_ms = with_settings(
        [this](SETTINGS s) { return s.config.kafka.consumerConf.kafka_commit_metadata_initial_retry_delay_ms; });

    // Check the current metadata to make sure it has not changed by another aggregator
    auto committed_toppar = RebalanceHandler::get_committed_metadata(toppar_to_commit, rk);
    if (committed_toppar != nullptr) {
        std::string current_metadata((char*)committed_toppar->elems[0].metadata,
                                     committed_toppar->elems[0].metadata_size);
        if (previousMetadata != current_metadata) {
            LOG(ERROR) << PART_ID(partitionId)
                       << "Unexpected metadata found on the partition. Probably it is changed by another aggregator."
                       << "\nExpected metadata: " << previousMetadata << "\nCurrent metadata: " << current_metadata;
            // Use metrics to capture: current kafka connector is the owner and no one else should have modified the
            // metadata.
            kafkaconnector_metrics->metadata_committed_by_different_connector_total
                ->labels({{"on_topic", topic}, {"on_zone", var_zone}})
                .increment();
        } else {
            LOG_KAFKA(3) << PART_ID(partitionId) << "Metadata is expected: "
                         << "\nExpected metadata: " << previousMetadata << "\nCurrent metadata: " << current_metadata;
        }
        rd_kafka_topic_partition_list_destroy(committed_toppar);
    }

    KafkaConnectorError result = KafkaConnectorError::NO_ERROR;
    rd_kafka_resp_err_t err;

    std::chrono::time_point<std::chrono::high_resolution_clock> offset_commit_start =
        std::chrono::high_resolution_clock::now();
    for (uint32_t retry = 0; retry < max_number_of_kafka_commit_metadata_retries; retry++) {
        if (retry > 0) {
            int delay_time = std::min(retry * kafka_commit_metadata_initial_retry_delay_ms,
                                      kafka_commit_metadata_max_retry_delay_ms);
            LOG(ERROR) << PART_ID(partitionId) << "Retry to commit metadata after " << delay_time << " (ms).";
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_time));
        }
        err = rd_kafka_commit(rk, toppar_to_commit, 0);
#ifdef _PRERELEASE
        if (flip::Flip::instance().test_flip("[kafka-commit-error]")) {
            LOG(INFO) << PART_ID(partitionId) << "[kafka-commit-error]: simulate kafka commit error";
            err = rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_LISTENER_NOT_FOUND;
        }
#endif
        if (err) {
            LOG(ERROR) << PART_ID(partitionId) << "Error in committing metadata: " << rd_kafka_err2str(err)
                       << " for metadata: " << print_toppar_list(toppar_to_commit);
            kafkaconnector_metrics->commit_offset_by_kafka_connectors_failed_total
                ->labels(
                    {{"on_topic", topic}, {"on_zone", var_zone}, {"error_code", std::to_string(static_cast<int>(err))}})
                .increment();
            result = KafkaConnectorError::KAFKA_ERROR;
        } else {
            LOG_KAFKA(1) << PART_ID(partitionId) << "Committed metadata: " << print_toppar_list(toppar_to_commit);
            kafkaconnector_metrics->committed_offset_by_kafka_connector
                ->labels({{"on_topic", topic}, {"on_zone", var_zone}})
                .update(toppar_to_commit->elems[0].offset);
            break;
        }
    }
    std::chrono::time_point<std::chrono::high_resolution_clock> offset_commit_end =
        std::chrono::high_resolution_clock::now();
    uint64_t time_diff =
        std::chrono::duration_cast<std::chrono::microseconds>(offset_commit_end.time_since_epoch()).count() -
        std::chrono::duration_cast<std::chrono::microseconds>(offset_commit_start.time_since_epoch()).count();
    kafkaconnector_metrics->commit_time_for_offset_commit_by_kafka_connectors
        ->labels({{"on_topic", topic}, {"on_zone", var_zone}})
        .observe(time_diff);

    if (result == KafkaConnectorError::NO_ERROR) {
        previousMetadata = "";
        for (size_t i = 0; i < toppar_to_commit->elems[0].metadata_size; i++) {
            previousMetadata.push_back(((char*)toppar_to_commit->elems[0].metadata)[i]);
        }
    }
    if (result != KafkaConnectorError::NO_ERROR)
        LOG(ERROR) << PART_ID(partitionId)
                   << "Number of retries exceeded the maximum number of retries. Rerturning failure now.";
    return result;
}

/**
 * It picks a buffer from a table queue. It tries the queue in circular manner.
 * It returns nullptr if now the table has a buffer ready for flush.
 */
KafkaConnectorError PartitionHandler::consume(RdKafka::Message* msg) {
    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();

    auto [identified_zone, identified_topic] = with_settings([this](SETTINGS s) {
        size_t idx = kafkaConnector->getId();
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });

    LOG_KAFKA(4) << PART_ID(partitionId) << "Consuming message [" << msg->offset() << "] in state "
                 << (status.getState() == REPLAY ? "REPLAY" : "CONSUME");
    // Currently if the kakfa message header format is wrong, we return NO_ERROR as we expect that this should not
    // happen.
    if (!msg->headers() || msg->headers()->get("table").size() == 0) {
        LOG(ERROR) << PART_ID(partitionId) << "Table is not set as the header for message [" << msg->offset()
                   << "]. This message is in bad format and will be ignored.";
        // To have metric to monitor this message in wrong format situation
        kafkaconnector_metrics->kafka_message_in_wrong_format_total
            ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
            .increment();
        return NO_ERROR;
    }
    // Checking the offset
    auto last_known_offset = status.getLastKnownOffset();
    // Abnormal condition 1: Message offset = 0 is an abnormal situation.
    if (last_known_offset > 0 && msg->offset() == 0) {
        LOG(WARNING) << PART_ID(partitionId)
                     << "Kafka offset got reset. All exisiting buffers will flush. The new message will be consumed. "
                        "offset before reset: "
                     << last_known_offset;
        kafkaconnector_metrics->kafka_offset_reset_total
            ->labels({{"on_topic", identified_topic},
                      {"partition", std::to_string(partitionId)},
                      {"on_zone", identified_zone}})
            .increment();
        flushAll();
        // NOTE: it seems that we need to change the associated buffer's "end" to be -1 for this situation of kafka
        // message offset being reset; Otherwise, buffer's end will keep the largest "end" ever-increasing as message
        // consumption continues with higher and higher message offsets. And in the method "append" of this class,  due
        // to the "append" logic, all messages that have offset smaller than the current "end" will get rejected and
        // thus no message replay will happen in this rare abnormal case.

        // Furthermore, this buffer "end" to be set to -1 is not needed for the re-wind (the next condition being
        // checked), as skipping the messages that have offset lower than the buffer's "end" offset is actually correct.
        status.clearMetadata();
        status.setState(CONSUME);
    } else if (msg->offset() <= last_known_offset) {
        // Abnormal condition 2: kafka message rewound
        LOG(WARNING) << PART_ID(partitionId)
                     << "Kafka offset got rewound. Message will be ignored. last known offset: " << last_known_offset
                     << " new offset: " << msg->offset();
        kafkaconnector_metrics->kafka_offset_went_backward_total
            ->labels({{"on_topic", identified_topic},
                      {"partition", std::to_string(partitionId)},
                      {"on_zone", identified_zone}})
            .increment();
        return NO_ERROR; // message rejected to be consumed.
    } else if (msg->offset() > last_known_offset + 1) {
        // Abnormal condition 3:  kafka message offset jumps dis-continuously
        LOG(WARNING) << PART_ID(partitionId)
                     << "Kafka offset is larger than what expected. Message will be still consumed. last known offset: "
                     << last_known_offset << " new offset: " << msg->offset();
        kafkaconnector_metrics->kafka_offset_larger_than_expected_total
            ->labels({{"on_topic", identified_topic},
                      {"partition", std::to_string(partitionId)},
                      {"on_zone", identified_zone}})
            .increment();
    }

    // Message accepted to be consumed, so we continue with the rest.
    status.setLastKnownOffset(msg->offset());

    std::string table = (char*)(msg->headers()->get("table")[0].value());

    /**
     * When reference is -1, it is either at the beginning of the whole system being just started, or we have flushed
     * everything (due to periodic force flush in version=1 protocol, which we don’t have in our code yet).
     * In any case, when reference = -1, all buffers must be empty. Because at current version=0 protocol, there is
     * no reference information committed to metadata. So after rebalancing and the partition handler gets re-created,
     * when the execution flow comes to the following line, the reference is always -1. Therefore reference = -1
     * indicates that rebalancing of the partition has just happened, in the case of version = 0 protocol that we are
     * currently using.
     */
    if (status.getReference() == -1) {
        // Setting the reference to be the current message offset. Since in current Version = 0, reference will not be
        // persisted for saved metadata.  Thus next time the new partition handler gets created, the reference is reset
        // to -1 again.
        status.setReference(msg->offset());
        // Note that all buffers are created at the Partition Handler constructor based on the tables specified in the
        // saved-metadata.
        for (auto buffer : buffers) {
            buffer.second->setCount(0);

            // Because this is the beginning of the partition handler's execution (due to reference = -1), we should not
            // have the situation that the buffer is not empty.
            if (!buffer.second->empty()) {
                LOG(ERROR) << PART_ID(partitionId)
                           << "Reference is -1 while buffer is not empty. Table= " << buffer.first;
                kafkaconnector_metrics->partition_handler_reference_wrong_total
                    ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                    .increment();
            }
        }
    }

    /**
     * We check to see if we have allocated buffer for the table to which this message belong to. This table is new from
     * the current message, not the tables identified in the saved metadata from which this Partition Handler is
     * created.
     */
    auto it = buffers.find(table);
    if (it == buffers.end()) {
        CHECK(GlobalContext::instance().getBufferFactory() != nullptr)
            << "Partition Handler can not retrieve Buffer Factory.";
        try {
            with_settings([this, &table](SETTINGS s) {
                buffers[table] = GlobalContext::instance().getBufferFactory()->createBuffer(
                    partitionId, table, s.config.kafka.consumerConf.buffer_batch_processing_size,
                    s.config.kafka.consumerConf.buffer_batch_processing_timeout_ms, kafkaConnector);
            });
            // metadata records (begin, end, count)
            buffers[table]->setCount(0);
        } catch (...) {
            LOG(ERROR) << PART_ID(partitionId) << "Exception during creating buffer for table: " << table
                       << ". when message offset reaches: " << msg->offset();
            buffers[table] = nullptr;
        }

        // If the buffer can not be created from above, then CHECK will fail and the whole process exits.
        CHECK(buffers[table] != nullptr) << "Buffer for table: " << table << " can not be created ";
    }

    /**
     * The current metadata on Kafka is with version 0 that does not have reference and count, we should have set
     reference above,
     * so we count from there by incrementing the count.

     * If the current metadata on Kafka is of version 1 or greater, then we have reference and count on it. In this
     case,
     * when we are reproducing the last block in the REPLAY mode, we don't increment the count and we simply use the
     count
     * recorded on the saved metadata. After reproducing the last block, we start counting. Thus, we increment the count
     * only when offset > savedMetadata.getOffset(table).end.
    */
    int count = 0;
    if (savedMetadata.getReference() == -1) { // metadata version 0
        count = buffers[table]->count() + 1;
    } else {
        // In the following condition, this table is still under REPLAY mode. Therefore the count has been included
        // in the saved metadata, and count does not get advanced.
        if (msg->offset() <= savedMetadata.getOffset(table).end) {
            count = savedMetadata.getOffset(table).count;
        } else {
            // In the following condition, this table is not in the REPLAY mode any more, and thus we use the regular
            // counting method.
            count = buffers[table]->count() + 1;
        }
    }
    buffers[table]->setCount(count);
    if (status.getState() == REPLAY) {
        pos = msg->offset();
        if (pos > end) {
            /*At this special condition, we need to check whether it should be an issue when pos > end, for all of the
            tables, *not just the current table.
            *
            *Examples:
            *1) Metadata = table1,6,5,table2,6,5 ; //indicating both tables do not have anything to replay at all.
            *end = 5
            *pos = 6 => offset > end, but since savedMetadata.emptyIgnoreSpecials() = true, we don't have issue (because
            * I am at pos > end) .

            *2) Metadata = table1,6,5,table2,3,5
            *end = 5
            *pos = 6 => offset > end, but since savedMetadata.emptyIgnoreSpecials() = false (table 2 is not special;
            * special is defined "begin = end + 1"), we have issue.
            */
            // savedMetadata gets constantly updated by removing the table entries when the table replay finishes.
            // The following function check: As long as there is at least on table with begin <= end, then we are still
            // in the REPLAY mode. But that contradicts the above condition (explained in the in-line comment of the
            // following function): pos > end, as in this case, we should complete the REPLAy mode and switch to the
            // CONSUME mode.
            if (!savedMetadata.emptyIgnoreSpecials()) {
                LOG(WARNING) << PART_ID(partitionId)
                             << "Based on metadata, some tables need replay, but REPLAY is going to be skipped! "
                                "probably due to Kafka retention."
                             << pos << ", current end: " << end;

                kafkaconnector_metrics->kafka_connector_replay_being_skipped_total
                    ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                    .increment();

            } else {
                // This is what we expect that when pos > end, there is no saved metadata table having begin <= end (
                // following the implementation of savedMetadata.emptyIgnoreSpecials) So we are are good to switch to
                // the CONSUME mode.
                LOG_KAFKA(1) << PART_ID(partitionId) << "Changing to CONSUME mode. pos:" << pos << " end:" << end;
            }

            // For all of the tables, even though some tables have already finished replay earlier, the last table that
            // finishes the replay is at pos = end.
            status.setState(CONSUME);
            if (!append(table, (const char*)msg->payload(), msg->len(), msg->offset(), msg->timestamp().timestamp)) {
                return MSG_ERROR;
            }
            kafkaconnector_metrics->batched_messages_replayed_by_kafka_connectors_total
                ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                .increment();
            kafkaconnector_metrics->batched_messages_replayed_by_kafka_connectors_bytes_total
                ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                .increment(msg->len());

            return NO_ERROR;
        } else if (pos < begin) {
            // We are in the abnormal condition that message offset gets rewound.
            LOG(WARNING) << PART_ID(partitionId) << "Received abnormal msg " << pos << " earlier than committed offset"
                         << begin << ", kafka broker error or retention, auto reset as earliest to avoid data loss";

            kafkaconnector_metrics->kafka_connector_abnormal_message_received_total
                ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                .increment();

            // The solution is for us to immediately go to the consume mode since this condition violates what is
            // expected in the normal situation.
            status.setState(CONSUME);
            if (!append(table, (const char*)msg->payload(), msg->len(), msg->offset(), msg->timestamp().timestamp)) {
                return MSG_ERROR;
            }

            kafkaconnector_metrics->batched_messages_replayed_by_kafka_connectors_total
                ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                .increment();
            kafkaconnector_metrics->batched_messages_replayed_by_kafka_connectors_bytes_total
                ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                .increment(msg->len());

            return NO_ERROR;
        }

        auto offset = savedMetadata.getOffset(table);
        // -1 means for this particular table, no metadata has been recorded at all. It could be that the replay of this
        // table is done, and the table entry gets removed from the savedMetadata, and thus the above getOffset(.)
        // return -1 when table can not be found any more.
        if (offset.begin != -1) { // table has metadata
            // The condition that the message offset passed the "end" offset of the table. So for this table, it is
            // already in the CONSUME mode. In fact, when it comes here, all tables should be already in the CONSUME
            // mode.
            if (msg->offset() > offset.end) {
                LOG_KAFKA(4) << PART_ID(partitionId) << "Appending message [" << msg->offset()
                             << "] in consume mode for table (" << table << ")";
                if (!append(table, (const char*)msg->payload(), msg->len(), msg->offset(),
                            msg->timestamp().timestamp)) {
                    return MSG_ERROR;
                }
                // Because we have the other saved-metadata removed for the situation when offset = end, so this entire
                // if-condition block should never come in the normal situation.
                savedMetadata.remove(table);
            } else if (offset.begin <= offset.end) {
                // The following condition is normal.
                if (msg->offset() >= offset.begin) {
                    // The following records the first message at the REPLAY mode
                    if (msg->offset() == offset.begin) {
                        LOG_KAFKA(4) << PART_ID(partitionId) << "Enter table [" << table << "] replay batch, start ["
                                     << offset.begin << "]"
                                     << " end[ " << offset.end << "]";
                    }
                    if (!append(table, (const char*)msg->payload(), msg->len(), msg->offset(),
                                msg->timestamp().timestamp)) {
                        return MSG_ERROR;
                    }
                    LOG_KAFKA(4) << PART_ID(partitionId) << "buffer for [" << table
                                 << "] begin: " << buffers[table]->begin() << " end: " << buffers[table]->end();
                } else {
                    /* We ignore the message as it is older than the begin of the last batch for this table. That can
                     * happen because the entire replay starts with "min" of all of the tables. And thus for a
                     * particular table, its saved "begin" can be larger than the message starting from "min" for all
                     * the tables. For example, The entire messages stored in Kafka is the following : 1 2 3 4 5 6 7 8 9
                     * 10 table1 involves offset:  1,3,6: suppose last batch: [1-6] table2 involves offset: 2,4,5,7,10:
                     * suppose last batch: [4-10], without offset 2. Metadata: table1, 1,6, table2, 4,10 We start
                     * replaying from 1 (min(1,4)).Msg at Offset 2 gets covered in this range, and thus will be read But
                     * we will ignore offset = 2 here at table 2.
                     */
                }

                // This situation means that this message is the last message to be replayed for this table.
                if (msg->offset() == offset.end) {

                    LOG_KAFKA(1) << PART_ID(partitionId)
                                 << " reconstruction of the previous buffer is done. Flushing buffer now.";
                    LOG_KAFKA(2) << PART_ID(partitionId) << "Updating replay batch for table: " << table
                                 << " begin: " << buffers[table]->begin() << " end: " << buffers[table]->end();
                    status.updateReplayedBatch(table, buffers[table]->begin(), buffers[table]->end(),
                                               buffers[table]->count());
                    LOG_KAFKA(2) << PART_ID(partitionId) << "Replay batches: " << status.getSerializedReplayedBatches();

                    // Update state information (in particular, count, which matters here in) to the in-memory status
                    // Actually, there is no begin and end that need to be updated as it is still in the REPLAY mode.
                    status.updateMetadata(table, buffers[table]->begin(), buffers[table]->end(),
                                          buffers[table]->count());

                    auto task = buffers[table]->flush();
                    auto table_task = activeTasks.find(table);
                    // Note the following situation should not happen in the normal situation when the partition handler
                    // is at the end of the REPLAY mode and only one task per table will ever be launched at this time.
                    if (table_task != activeTasks.end() && !table_task->second->isDone()) {
                        LOG(WARNING) << PART_ID(partitionId)
                                     << "flush task should have been finished but it is not, table " << table;

                        // Having table as a metric dimension
                        kafkaconnector_metrics->kafka_connector_flush_task_not_finished_total
                            ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}, {"table", table}})
                            .increment();
                    }
                    activeTasks[table] = task;
                    task->start();               // Only one task per table in the REPLAY mode can happen.
                    savedMetadata.remove(table); // Remove the entry from the metadata tracker.
                    // The following condition happens when all of the tables are removed from saved-metadata after
                    // being replayed. Thus, the whole partition handler now moves to the CONSUME mode.
                    if (savedMetadata.empty()) {
                        // If there are no traffic after we finish replay, we should change status to CONSUME to
                        // let next round checkBuffers to update lastCommittedOffset
                        LOG_KAFKA(1) << PART_ID(partitionId) << "Changing to CONSUME mode. pos:" << pos
                                     << " end:" << end;
                        status.setState(CONSUME);
                    } else {
                        LOG_KAFKA(1) << PART_ID(partitionId) << "In REPLAY mode, after removing table " << table
                                     << ", left metadata is " << savedMetadata.serialize() << ". pos:" << pos
                                     << " end:" << end;
                    }
                }
            } else {
                LOG_KAFKA(4) << PART_ID(partitionId) << "Message " << msg->offset() << " is not consumed for table ["
                             << table << "], as this messages is already applied by the store.";
            }
        } else {
            // Do not have any metadata, directly go to the Consume mode for this table.
            LOG_KAFKA(4) << PART_ID(partitionId) << "Appending message [" << msg->offset()
                         << "] in consume mode for table (" << table
                         << ") that either does not have metadata or the reconstruction of its previous batch is done";
            if (!append(table, (const char*)msg->payload(), msg->len(), msg->offset(), msg->timestamp().timestamp)) {
                return MSG_ERROR;
            }
        }
    } else { // We are at the Consume mode
        if (!append(table, (const char*)msg->payload(), msg->len(), msg->offset(), msg->timestamp().timestamp)) {
            return MSG_ERROR;
        }
    }
    return NO_ERROR;
} // namespace kafka

KafkaConnectorError PartitionHandler::checkBuffers(rd_kafka_t* rk) {
    KafkaConnectorError result = KafkaConnectorError::NO_ERROR;

    //_log_info(logger, "entering check buffer for Partition [%d]", partitionId);
    LOG_KAFKA(3) << PART_ID(partitionId) << "checking buffer, offset: [" << status.getMinOffset() << ","
                 << status.getMaxOffset() << "]";
    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();
    auto [identified_zone, identified_topic] = with_settings([this](SETTINGS s) {
        size_t idx = kafkaConnector->getId();
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });
    std::chrono::time_point<std::chrono::high_resolution_clock> check_buffers_start =
        std::chrono::high_resolution_clock::now();

    int64_t currentLastOffset = status.getLastKnownOffset();
    std::vector<FlushTaskPtr> new_tasks;
    bool commitRequired = false;
    for (auto& entry : buffers) {
        auto table = entry.first;
        auto buffer = entry.second;
        if (!buffer->flushable())
            continue;
        // We do not want to create new metadata to the Broker to represent the state that
        // the buffer is empty, and begin = end +1 has been still there since some earlier, say 1 hour ago.
        // if this state lasts 1 hour, then only one marker for the whole 1-hour duration will be recorded to the
        // metadata. For example, in the case of unit test Jenkins jobs that only runs every day at midnight and for the
        // whole day, there will not be metadata update at all. So  it is waste the effort to commit to the Broker for
        // the entire day of no progress via the special-marker.  In summary, we do not want to commit when not
        // necessary. Just let it pass through via "continue".
        if (buffer->empty() && status.getOffset(table).begin == currentLastOffset + 1 &&
            status.getOffset(table).end == currentLastOffset) {
            continue;
        }
        // _log_debug(logger, "Partition [%d]: Buffer {%s[%ld, %ld]} is flushable. ", partitionId, table.c_str(),
        //            buffer->begin(), buffer->end());
        LOG_KAFKA(3) << PART_ID(partitionId) << "Buffer {" << table << "[" << buffer->begin() << "," << buffer->end()
                     << "]} is flushable.";
        auto task_it = activeTasks.find(table);
        // We have a pending task that has been launched, and we need to wait for it to finish.
        if (task_it != activeTasks.end()) {
            if (task_it->second == nullptr) {
                LOG_KAFKA(1) << PART_ID(partitionId) << "encounter task to be nullptr on table: " << table;
            } else {
                if (!task_it->second->isDone()) {
                    LOG_KAFKA(3) << PART_ID(partitionId) << "waiting for the previous task on table: " << table;
                }
                auto blockWait_result = task_it->second->blockWait();
#ifdef _PRERELEASE
                if (flip::Flip::instance().test_flip("[block-wait-return-false]")) {
                    LOG(INFO) << "[block-wait-return-false]: simulate aggregator to clickhouse blockWait error";
                    blockWait_result = false;
                }
#endif
                if (!blockWait_result) {
                    kafkaconnector_metrics->task_block_wait_failed_in_kafka_connectors
                        ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                        .increment();

                    LOG(ERROR) << PART_ID(partitionId) << "Flush task failed on table " << table << ", return CH_ERROR";
                    result = KafkaConnectorError::CH_ERROR;
                    commitRequired = false;
                    break; // Return error for any flush failure.
                }

                // Note: when block waiting fails, the entire kafka-connector main loop will be reset, including
                // removing all of the partition handlers, and thus all associated tasks will be aborted any way. So in
                // the above block the flow exits due to block-wait failure.
                activeTasks.erase(task_it);
            }
        }

        commitRequired = true;
        if (buffer->empty()) {
            // At the first time the buffer is empty, the special marker is written down. If the buffer continues to
            // be empty in the next check buffer, there is no actual status update happened  on this metadata, even
            // though the following method is still invoked (invoked, but no actual value update), because the earlier
            // decision logic has already conveyed "buffer is empty and begin = end + 1".
            status.updateMetadata(table, currentLastOffset + 1, currentLastOffset, buffer->count());
            // ToDo check buffer flush of Xinglong's code
            LOG_KAFKA(4) << PART_ID(partitionId) << "Buffer is empty but flushable for table: " << table;
            auto task = buffer->flush();
            if (task != nullptr) {
                LOG(WARNING) << "Empty task should be nullptr";
                new_tasks.push_back(task);
                activeTasks[table] = task;
            }
        } else {
            status.updateMetadata(table, buffer->begin(), buffer->end(), buffer->count());
            auto task = buffer->flush();
            if (task != nullptr) {
                new_tasks.push_back(task);
                activeTasks[table] = task;
            }
        }
    }

    // If commit required, perform metadata commit first, and when commit succeeds, then launch the pending tasks
    if (commitRequired) {
        auto toppar_list = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(toppar_list, topic.c_str(), partitionId);
        toppar_list->elems[0].offset = status.getMinOffset(); // Commit offset.
        toppar_list->elems[0].metadata = strdup(status.getSerializedMetadata().c_str());
        toppar_list->elems[0].metadata_size = strlen((char*)toppar_list->elems[0].metadata);

        LOG_KAFKA(3) << PART_ID(partitionId) << "Kafka committing metadata offset: [" << status.getMinOffset() << ","
                     << status.getMaxOffset() << "]";

        if (commitMetadata(toppar_list, rk) != KafkaConnectorError::NO_ERROR) {
            LOG(ERROR) << PART_ID(partitionId) << "Metadata commit failed, should reconsume data block from kafka";
            kafkaconnector_metrics->kafka_commit_metadata_finally_failed_total
                ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
                .increment();
            result = KafkaConnectorError::KAFKA_ERROR;
        } else {
            for (auto& task : new_tasks) {
                if (task != nullptr) {
                    task->start();
                }
            }
            // The following is only for debugging/testing purpose.
            status.updateLastCommittedMetadataAndOffset();
        }
        rd_kafka_topic_partition_list_destroy(toppar_list);
    }
    std::chrono::time_point<std::chrono::high_resolution_clock> check_buffers_end =
        std::chrono::high_resolution_clock::now();
    uint64_t time_diff =
        std::chrono::duration_cast<std::chrono::microseconds>(check_buffers_end.time_since_epoch()).count() -
        std::chrono::duration_cast<std::chrono::microseconds>(check_buffers_start.time_since_epoch()).count();
    kafkaconnector_metrics->all_tasks_completion_waittime_by_kafka_connectors
        ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}})
        .observe(time_diff);

    return result;
}

/**
 If append returns false, the failure will propagate to kafka-connector main processing loop, and force the main
 processing loop to exit and thus discontinue  message consumption. As a result, kafka re-partition could happen and a
 different replica may pick up the message to repeat the processing (a cross-replica retry loop). Even if the partition
 does not happen and the same replica pick up this partition, it will go through the Partition Handler initialization
 again.
 */
bool PartitionHandler::append(std::string table, const char* data, size_t data_size, int64_t offset,
                              int64_t msg_timestamp) {
    std::shared_ptr<nuclm::KafkaConnectorMetrics> kafkaconnector_metrics =
        nuclm::MetricsCollector::instance().getKafkaConnectorMetrics();
    auto [identified_zone, identified_topic] = with_settings([this](SETTINGS s) {
        size_t idx = kafkaConnector->getId();
        auto& var = s.config.kafka.configVariants[idx];
        return std::make_tuple(var.zone, var.topic);
    });

    LOG_KAFKA(4) << PART_ID(partitionId) << "append msg " << offset << " for table buf: " << table
                 << " time stamp: " << msg_timestamp << "msg lag time (ms) : "
                 << (std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count() -
                     msg_timestamp);

    /**
     * The buffers[table]->end() indicates the last messages appended to the buffer. Note: end is defaulted to -1.
     * When offset is less than or equal to this end, then this means Kafka message got rewound or is duplicated.
     * This should not happen in the normal situation.
     */
    if (offset > buffers[table]->end()) {
        if (buffers[table]->append(data, data_size, offset, msg_timestamp)) {
            LOG_KAFKA(5) << PART_ID(partitionId) << "appended msg: " << offset;

            // Important: if the table is missing on the metadata, add it to the metadata.
            // The following updateMetadata invocation ensures that the first time the message for a table gets
            // consumed, we will record this special marker to indicate that "at least this is the message that we
            // should start for replaying; not any message earlier". otherwise, the replay can go to a higher offset due
            // to the other table, and therefore replay will miss some messages that belong to this table.
            if (status.getOffset(table).begin == -1) {
                status.updateMetadata(table, offset, offset - 1, 0);
            }
            return true;
        } else {
            // Failure will propagate to kafka main processing loop.
            LOG(ERROR) << PART_ID(partitionId) << "failed to append msg: " << offset << " to table: " << table;
            return false;
        }
    } else {
        LOG(WARNING) << PART_ID(partitionId)
                     << "Kafka message has rewound. This should not happen. Thus, ignoring the message at " << offset;

        // Having table to be a metric dimension
        kafkaconnector_metrics->kafka_broker_message_rewound_total
            ->labels({{"on_topic", identified_topic}, {"on_zone", identified_zone}, {"table", table}})
            .increment();

        return true;
    }
}

// This method only handles the situation when the whole message offset gets reset to 0.
void PartitionHandler::flushAll() {
    for (auto buffer : buffers) {
        auto task = buffer.second->flush();
        activeTasks[buffer.first] = task;
        task->start();
    }
}
} // namespace kafka
