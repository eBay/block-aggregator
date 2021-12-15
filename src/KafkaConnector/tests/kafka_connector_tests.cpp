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

#include "../Producer.h"
#include "KafkaConnector/SimpleFlushTask.h"
// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "boost/date_time/time_defs.hpp"
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include "test_common.h"
#include "KafkaConnector/GlobalContext.h"
#include "KafkaConnector/SimpleBuffer.h"
#include "KafkaConnector/SimpleBufferFactory.h"
#include "KafkaConnector/SimpleThreadPool.h"
#include "KafkaConnector/KafkaConnector.h"
#include "librdkafka/rdkafkacpp.h"
#include "KafkaConnector/FileWriter.h"

#include <boost/filesystem.hpp>
namespace filesystem = boost::filesystem;

#include <algorithm>
#include <fstream>
#include <glog/logging.h>
#include <unordered_map>

#include <thread>
#include <chrono>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

const std::string TEST_CONFIG_FILE_PATH_ENV_VAR = "TEST_CONFIG_FILE_PATH";

static std::string getConfigFilePath(const std::string& config_file) {
    const char* env_p = std::getenv(TEST_CONFIG_FILE_PATH_ENV_VAR.c_str());
    if (env_p == nullptr) {
        LOG(ERROR) << "cannot find  TEST_CONFIG_FILE_PATH environment variable....exit test execution...";
        exit(-1);
    }

    std::string path(env_p);
    path.append("/").append(config_file);

    return path;
}

static std::string extract(const std::string& host_with_endpoint) {
    std::size_t found = host_with_endpoint.find(":");
    if (found != std::string::npos) {
        std::string result = host_with_endpoint.substr(0, found);
        return result;
    } else {
        LOG(ERROR) << "cannot extract host name from: " << host_with_endpoint;
        exit(-3);
    }
}
std::pair<std::shared_ptr<std::thread>, std::shared_ptr<Producer>>
runProducer(int num_of_messages, int num_of_tables, int message_size, int num_of_partitions, bool create, int delay) {
    auto kafka_consumer_params = SETTINGS_PARAM(config.kafka.consumerConf);
    // pick the first configuration variant
    auto kafka_consumer_configuration_params = SETTINGS_PARAM(config.kafka.configVariants[0]);

    std::string server_lists = kafka_consumer_configuration_params.hosts;
    std::string topic = kafka_consumer_configuration_params.topic;

    std::string first_kafka_endpoint_with_port = server_lists; // in the format of: localhost:9092.
    std::string first_kafka_endpoint_host_name = extract(first_kafka_endpoint_with_port);

    std::shared_ptr<Producer> producer;
    if (create) {
        producer = std::make_shared<Producer>(server_lists, topic, num_of_messages, num_of_tables, message_size, delay,
                                              create, first_kafka_endpoint_host_name + ":2181", num_of_partitions);
    } else {
        producer = std::make_shared<Producer>(server_lists, topic, num_of_messages, num_of_tables, message_size, delay);
    }
    return {std::make_shared<std::thread>([=]() { producer->run(); }), producer};
}

// int runProducer(int num_of_messages, int num_of_tables, int message_size, int num_of_partitions, bool create) {
//     auto kafka_consumer_params = SETTINGS_PARAM(config.kafka.consumerConf);
//     // pick the first configuration variant
//     auto kafka_consumer_configuration_params = SETTINGS_PARAM(config.kafka.configVariants[0]);

//     std::string server_lists = kafka_consumer_configuration_params.hosts;
//     std::string topic = kafka_consumer_configuration_params.topic;
//     std::string command = "./producer " + server_lists + " " + topic + " " + std::to_string(num_of_messages) + " " +
//         std::to_string(num_of_tables) + " " + std::to_string(message_size);

//     std::string first_kafka_endpoint_with_port = server_lists; // in the format of: localhost:9092.
//     std::string first_kafka_endpoint_host_name = extract(first_kafka_endpoint_with_port);

//     LOG(INFO) << " The first kafka endpoint host name extracted is: " << first_kafka_endpoint_host_name;
//     //:2181 is for zookeeper, we assume that the same kafka cluster also is configured with zookeeper.
//     if (create)
//         command += +" --create " + first_kafka_endpoint_host_name + ":2181 " + std::to_string(num_of_partitions);

//     LOG(INFO) << "issue command to run producer is: " << command;

//     return system(command.c_str());
// >>>>>>> master
// }

struct Offset {
    int begin;
    int end;
};

struct Batch {
    int partition;
    std::string table;
    Offset offset;
};

Batch readBatch(std::string& line, std::ifstream& infile, int& num_of_message_out) {
    int batch_size = stoi(line.substr(line.find(">") + 1));
    std::getline(infile, line);
    int partition_id = stoi(line.substr(line.find("=") + 1));
    std::getline(infile, line);
    std::string table = line.substr(line.find("=") + 1);

    int begin = -1;
    int end = -1;
    for (int i = 0; i < batch_size; i++) {
        std::getline(infile, line);
        int offset = stoi(line.substr(0, line.find(":")));
        if (begin == -1)
            begin = offset;
        end = offset;
        num_of_message_out++;
    }
    Batch result = {partition_id, table, {begin, end}};
    return result;
}

/**
 * Returns the last batch for each partition.
 * @param content
 * @return
 */
std::unordered_map<int, std::unordered_map<std::string, Offset>> getLastBatches(std::string filename,
                                                                                int& num_of_message_out) {
    std::unordered_map<int, std::unordered_map<std::string, Offset>> result;
    std::ifstream infile(filename);
    std::string line;
    num_of_message_out = 0;
    while (std::getline(infile, line)) {
        if (line.find("new batch>") != std::string::npos) {
            auto batch = readBatch(line, infile, num_of_message_out);
            result[batch.partition][batch.table] = batch.offset;
        }
    }
    return result;
}

std::unordered_map<int, std::unordered_map<std::string, Offset>> getFirstBatches(std::string filename,
                                                                                 int& num_of_message_out) {
    std::unordered_map<int, std::unordered_map<std::string, Offset>> result;
    std::ifstream infile(filename);
    std::string line;
    num_of_message_out = 0;
    while (std::getline(infile, line)) {
        if (line.find("new batch>") != std::string::npos) {
            auto batch = readBatch(line, infile, num_of_message_out);
            auto it = result.find(batch.partition);
            if (it != result.end()) {
                if (it->second.find(batch.table) == it->second.end())
                    result[batch.partition][batch.table] = batch.offset;
            } else
                result[batch.partition][batch.table] = batch.offset;
        }
    }
    return result;
}

/**the total number of messages in the content should equal to the num_of_messages.
 * If last_batches is not empty, the first batch for each partition should be exactly the same the batch in the
 * last_batches.
 * @param content
 * @param num_of_messages
 * @param last_batches
 */

int checkBatchesContent(std::string& filename, int num_of_messages,
                        std::unordered_map<int, std::unordered_map<std::string, Offset>> last_batches = {}) {
    int message_num;
    LOG_KAFKA(2) << "Checking content of the output file: " << filename;
    auto first_batches = getFirstBatches(filename, message_num);
    CHK_EQ(num_of_messages, message_num);

    if (!last_batches.empty()) {
        for (auto p_it = last_batches.begin(); p_it != last_batches.end(); p_it++) {
            for (auto t_it = p_it->second.begin(); t_it != p_it->second.end(); t_it++) {
                auto partition = p_it->first;
                auto table = t_it->first;
                CHK_EQ(last_batches[partition][table].begin, t_it->second.begin);
                CHK_EQ(last_batches[partition][table].end, t_it->second.end);
            }
        }
    }
    return 0;
}

RdKafka::Conf* createConfig() {
    auto kafka_consumer_params = SETTINGS_PARAM(config.kafka.consumerConf);
    // pick the first configuration variant
    auto kafka_consumer_configuration_params = SETTINGS_PARAM(config.kafka.configVariants[0]);
    std::string errstr;
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string end_point_with_port = kafka_consumer_configuration_params.hosts;
    if (conf->set("bootstrap.servers", end_point_with_port, errstr) != RdKafka::Conf::CONF_OK) {
        LOG(ERROR) << "Failed to set broker address: " << errstr << std::endl;
        exit(1);
    } else {
        LOG(INFO) << "Kafka configuration successfully set broker address: " << end_point_with_port;
    }

    std::string groupid = "nucolumnar_aggregator." + kafka_consumer_configuration_params.variantName + "_" +
        kafka_consumer_configuration_params.zone + "_" + kafka_consumer_configuration_params.topic;
    if (conf->set("group.id", groupid, errstr) != RdKafka::Conf::CONF_OK) {
        LOG(ERROR) << "Failed to set group.id: " << errstr << std::endl;
        exit(1);
    } else {
        LOG(INFO) << "Kafka configuration successfully set group.id: " << groupid;
    }

    // disabling the auto-commit
    std::string enable_auto_commit_result = kafka_consumer_params.enable_auto_commit ? "true" : "false";
    if (conf->set("enable.auto.commit", enable_auto_commit_result, errstr) != RdKafka::Conf::CONF_OK) {
        LOG(ERROR) << "Failed to disable auto commit: " << errstr << std::endl;
        exit(1);
    } else {
        LOG(INFO) << "Kafka configuration successfully set the flag:  enable_auto_commit_result to be: "
                  << enable_auto_commit_result;
    }

    std::string auto_offset_reset = kafka_consumer_params.auto_offset_reset;
    if (conf->set("auto.offset.reset", auto_offset_reset, errstr) != RdKafka::Conf::CONF_OK) {
        LOG(ERROR) << "Failed to set auto.offset.reset: " << errstr << std::endl;
        exit(1);
    } else {
        LOG(INFO) << "Kafka configuration successfully set the flag: auto_offset_reset to be: " << auto_offset_reset;
    }
    return conf;
}

int checkCompleteness(int num_of_messages, int num_of_partitions) {
    LOG_KAFKA(2) << "Checking completeness using invariant checker";
    int max_end = 0;
    for (int p = 0; p < num_of_partitions; p++) {
        auto p_max_end = kafka::SimpleFlushTask::invariantChecker.getMaxEnd(p);
        max_end += p_max_end + 1;
    }

    CHK_EQ(num_of_messages, max_end);
    return 0;
}

void sleepSec(int seconds) {
    LOG(INFO) << "Sleeping for " << seconds << "seconds...";
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
}
int happy_path(int num_of_messages, int num_of_tables, int message_size, int num_of_partitions, int producer_delay,
               std::string output_file) {

    kafka::SimpleFlushTask::clearInvariantChecker();
    // clearing the output
    std::ofstream ofs;
    ofs.open(output_file, std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    // call runProducer to create the topic and put messages
    auto t = runProducer(num_of_messages, num_of_tables, message_size, num_of_partitions, true, producer_delay);
    // Create Kafka Config
    auto conf = createConfig();

    std::string rm_command = "rm kafka_connector_test.log";
    int status = system(rm_command.c_str());
    if (status != 0) {
        std::cerr << "command: " << rm_command << "failed with status code: " << status << std::endl;
    }

    // create your KafkaConnector object to listen to the topic
    std::shared_ptr<kafka::GenericThreadPool> thread_pool = std::make_shared<kafka::SimpleThreadPool>();
    thread_pool->init();

    // _log_info(logger, "waiting for the producer to finish");
    LOG(INFO) << "Waiting for the producer to finish.";
    t.first->join();
    LOG(INFO) << "Producer thread joined";

    kafka::KafkaConnector kafkaConnector(0, conf, thread_pool, nullptr);
    kafkaConnector.start();

    sleepSec(10);
    kafkaConnector.stop();
    // CHK_EQ(true, kafkaConnector.wait());
    kafkaConnector.wait();
    thread_pool->shutdown();

    // _log_info(logger, "stopping FileWriter.");
    LOG(INFO) << "Stopping FileWriter.";
    FileWriter::getInstance()->stop();
    LOG(INFO) << "FileWriter stopped.";

    std::unordered_map<int, std::unordered_map<std::string, Offset>> ignore;
    // if (checkBatchesContent(output_file, num_of_messages, ignore))
    //     return -1;
    if (checkCompleteness(num_of_messages, num_of_partitions))
        return -1;
    return 0;
}

int replay_test(int num_of_messages, int num_of_tables, int message_size, int num_of_partitions, int producer_delay,
                std::string output_file) {
    kafka::SimpleFlushTask::clearInvariantChecker();
    // clearing the output
    std::ofstream ofs;
    ofs.open(output_file, std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    // pick the first configuration variant
    auto kafka_consumer_configuration_params = SETTINGS_PARAM(config.kafka.configVariants[0]);

    // call runProducer to create the topic and put messages
    auto t = runProducer(num_of_messages, num_of_tables, message_size, num_of_partitions, true, producer_delay);
    // Creat Kafka Config
    auto conf = createConfig();

    std::string rm_command = "rm kafka_connector_test.log";
    int status = system(rm_command.c_str());
    if (status != 0) {
        LOG(ERROR) << "command: " << rm_command << "failed with status code: " << status << std::endl;
    }

    // create a simple thread pool
    std::shared_ptr<kafka::GenericThreadPool> thread_pool = std::make_shared<kafka::SimpleThreadPool>();
    thread_pool->init();

    // create your KafkaConnector object to listen to the topic

    std::this_thread::sleep_for(std::chrono::seconds(10)); // waiting for intialization
    kafka::KafkaConnector kafkaConnector(0, conf, thread_pool, nullptr);
    kafkaConnector.start();

    LOG(INFO) << "Waiting for the producer to finish.";
    t.first->join();
    LOG(INFO) << "Producer thread joined";

    std::this_thread::sleep_for(std::chrono::seconds(10)); // some additional wait

    LOG(INFO) << "Stopping kafkaConnector.";
    kafkaConnector.stop();
    // CHK_EQ(true, kafkaConnector.wait());

    auto check_result = checkBatchesContent(output_file, num_of_messages, {});
    if (check_result != 0)
        return check_result;

    // cleaning output file
    ofs.open(output_file, std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    // putting some new data, but we don't remove the previous data this time.
    LOG(INFO) << "Running producer again.";
    t = runProducer(num_of_messages, num_of_tables, message_size, num_of_partitions, false, producer_delay);

    LOG(INFO) << "Starting kafkaConnector again.";
    kafkaConnector.start();

    LOG(INFO) << "Waiting for the producer to finish.";
    t.first->join();
    LOG(INFO) << "Producer thread joined";

    std::this_thread::sleep_for(std::chrono::seconds(10));
    LOG(INFO) << "Stopping kafkaConnector again.";
    kafkaConnector.stop();
    // CHK_EQ(true, kafkaConnector.wait());

    kafkaConnector.wait();

    thread_pool->shutdown();

    LOG(INFO) << "Stopping FileWriter.";
    FileWriter::getInstance()->stop();
    LOG(INFO) << "FileWriter stopped.";

    if (checkCompleteness(2 * num_of_messages, num_of_partitions))
        return -1;
    return 0;
}

int replay_test_with_storage_failure(int num_of_messages, int num_of_tables, int message_size, int num_of_partitions,
                                     int producer_delay, std::string output_file) {
    kafka::SimpleFlushTask::clearInvariantChecker();

    // clearing the output
    std::ofstream ofs;
    ofs.open(output_file, std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    // call runProducer to create the topic and put messages
    auto t = runProducer(num_of_messages, num_of_tables, message_size, num_of_partitions, true, producer_delay);
    // Creat Kafka Config
    auto conf = createConfig();

    std::string rm_command = "rm kafka_connector_test.log";
    int status = system(rm_command.c_str());
    if (status != 0) {
        std::cerr << "command: " << rm_command << "failed with status code: " << status << std::endl;
    }

    // create a simple thread pool
    std::shared_ptr<kafka::GenericThreadPool> thread_pool = std::make_shared<kafka::SimpleThreadPool>();
    thread_pool->init();

    // create your KafkaConnector object to listen to the topic
    std::this_thread::sleep_for(std::chrono::seconds(10));
    kafka::KafkaConnector kafkaConnector(0, conf, thread_pool, nullptr);

    kafkaConnector.start();

    std::this_thread::sleep_for(std::chrono::seconds(3));
    // _log_info(logger, "-------------Simulating Storage Failure----------------");
    kafka::SimpleFlushTask::stopAcks();
    // CHK_EQ(false, kafkaConnector.wait());

    // _log_info(logger, "-------------Running KafkaConnector Again----------------");
    kafka::SimpleFlushTask::startAcks();
    // cleaing output file
    ofs.open(output_file, std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    kafkaConnector.start();

    LOG(INFO) << "Waiting for the producer to finish.";
    t.first->join();
    LOG(INFO) << "Producer thread joined";

    // waiting longer enough to consumer all message
    std::this_thread::sleep_for(std::chrono::seconds(10));
    kafkaConnector.stop();
    // CHK_EQ(true, kafkaConnector.wait());
    kafkaConnector.wait();
    thread_pool->shutdown();

    LOG(INFO) << "Stopping FileWriter.";
    FileWriter::getInstance()->stop();
    LOG(INFO) << "FileWriter stopped.";

    if (checkCompleteness(num_of_messages, num_of_partitions))
        return -1;
    return 0;
}

int partition_reassignment_test(int num_of_messages, int num_of_tables, int message_size, int num_of_partitions,
                                int producer_delay, std::string output_file) {
    kafka::SimpleFlushTask::clearInvariantChecker();
    // clearing the output
    std::ofstream ofs;
    ofs.open(output_file, std::ofstream::out | std::ofstream::trunc);
    ofs.close();

    // call runProducer to create the topic and put messages
    auto t = runProducer(num_of_messages, num_of_tables, message_size, num_of_partitions, true, producer_delay);
    // Creat Kafka Config
    auto conf1 = createConfig();
    auto conf2 = createConfig();

    std::string rm_command = "rm kafka_connector_test.log";
    int status = system(rm_command.c_str());
    if (status != 0) {
        std::cerr << "command: " << rm_command << "failed with status code: " << status << std::endl;
    }

    // create a simple thread pool
    std::shared_ptr<kafka::GenericThreadPool> thread_pool = std::make_shared<kafka::SimpleThreadPool>();
    thread_pool->init();

    // create two kafkaconnectors
    sleepSec(10);
    // _log_info(logger, "Running KafkaConnector 1");
    LOG(INFO) << "Running KafkaConnector 0.";
    kafka::KafkaConnector kafkaConnector0(0, conf1, thread_pool, nullptr);
    kafkaConnector0.start();

    sleepSec(10);

    // _log_info(logger, "Running KafkaConnector 2");
    LOG(INFO) << "Running KafkaConnector 1.";
    kafka::KafkaConnector kafkaConnector1(1, conf2, thread_pool, nullptr);
    kafkaConnector1.start();

    sleepSec(10);
    // _log_info(logger, "Stopping KafkaConnector 1");
    LOG(INFO) << "Stopping KafkaConnector 0.";
    kafkaConnector0.stop();
    kafkaConnector0.wait();

    sleepSec(15);

    // _log_info(logger, "Starting KafkaConnector 1 again");
    LOG(INFO) << "Starting KafkaConnector 0 again.";
    kafkaConnector0.start();
    // _log_info(logger, "KafkaConnector 1 started.");
    LOG(INFO) << "KafkaConnector 0 started.";

    LOG(INFO) << "Waiting for the producer to finish.";
    t.first->join();
    LOG(INFO) << "Producer thread joined";

    sleepSec(10);
    // waiting longer enough to consumer all message
    kafkaConnector0.stop();
    // CHK_EQ(true, kafkaConnector0.wait());
    kafkaConnector0.wait();

    kafkaConnector1.stop();
    // CHK_EQ(true, kafkaConnector1.wait());
    kafkaConnector1.wait();

    thread_pool->shutdown();

    LOG(INFO) << "Stopping FileWriter.";
    FileWriter::getInstance()->stop();
    LOG(INFO) << "FileWriter stopped.";

    if (checkCompleteness(num_of_messages, num_of_partitions))
        return -1;
    return 0;
}

static void init_kafka_environment() {
    std::string path = getConfigFilePath("my_simple_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;
    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.
    kafka::GlobalContext::instance().setBufferFactory(std::make_shared<kafka::SimpleBufferFactory>());
}

// invocation: kafka_connector_tests --end_point <chosen endpoint>
int main(int argc, char** argv) {
    // to globally initialize glog
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    std::string log_dir = get_cwd_path("logs/");

    if (!filesystem::exists(log_dir) && !filesystem::create_directories(log_dir)) {
        std::cerr << "Failed to create log dir: " << log_dir << std::endl;
        std::exit(-1);
    } else {
        std::cout << "Log Directory: " << log_dir << std::endl;
    }

    FOREACH_VMODULE(VMODULE_INITIALIZE_MODULE)
    // set log destination
    for (auto i = 0; i < google::NUM_SEVERITIES; i++) {
        std::string logpaths = log_dir + std::string("/NuColumnarAggregator.log.") + google::LogSeverityNames[i] + ".";
        google::SetLogDestination(i, logpaths.c_str());
    }

    TestSuite ts(argc, argv);
    init_kafka_environment();

    ts.doTest("happy_path_2_table_1_partition", happy_path, 100, 2, 10, 1, 0, "batches.txt");
    ts.doTest("happy_path_2_table_2_partition", happy_path, 100, 2, 10, 2, 0, "batches.txt");
    ts.doTest("replay_test_1_table_1_partition", replay_test, 100, 1, 10, 1, 10, "batches.txt");
    ts.doTest("replay_test_3_table_1_partition", replay_test, 100, 3, 10, 1, 10, "batches.txt");
    ts.doTest("replay_test_2_table_2_partition", replay_test, 100, 2, 10, 2, 10, "batches.txt");
    ts.doTest("replay_test_with_storage_failure_1_table_1_partition", replay_test_with_storage_failure, 100, 1, 50, 1,
              0, "batches.txt");
    ts.doTest("replay_test_with_storage_failure_2_table_1_partition", replay_test_with_storage_failure, 100, 2, 50, 1,
              0, "batches.txt");
    ts.doTest("replay_test_with_storage_failure_2_table_2_partition", replay_test_with_storage_failure, 100, 2, 50, 2,
              0, "batches.txt");
    ts.doTest("partition_reassignment_test", partition_reassignment_test, 500, 3, 10, 1, 100, "batches.txt");
    ts.doTest("partition_reassignment_test", partition_reassignment_test, 500, 3, 10, 2, 100, "batches.txt");
    ts.doTest("partition_reassignment_test", partition_reassignment_test, 500, 3, 10, 3, 100, "batches.txt");
    ts.doTest("partition_reassignment_test", partition_reassignment_test, 500, 3, 10, 4, 100, "batches.txt");
    return 0;
}
