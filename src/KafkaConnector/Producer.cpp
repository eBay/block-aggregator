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

#include "Producer.h"

#include <librdkafka/rdkafkacpp.h>
#include <iostream>
#include <random>
#include <string>
#include <algorithm>
#include <stdlib.h>
#include <chrono>
#include <thread>

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
  public:
    void dr_cb(RdKafka::Message& message) {
        if (message.err())
            std::cerr << "% Message delivery failed: " << message.errstr() << std::endl;
        else
            std::cerr << "% Message delivered to topic " << message.topic_name() << " [" << message.partition()
                      << "] at offset " << message.offset() << std::endl;
    }
};

std::string random_string(int max_length) {
    std::string possible_characters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dist(0, possible_characters.size() - 1);
    std::string ret = "";
    for (int i = 0; i < max_length; i++) {
        int random_index = dist(engine); // get index between 0 and possible_characters.size()-1
        ret += possible_characters[random_index];
    }
    return ret;
}

bool Producer::run() {
    if (create_topic) {
        // create Kafka topic here. Delete and re-create it, if it already exists!
        printf("Creating Kafka topic %s... (kafka-topics.sh must be in your PATH)\n", topic.c_str());
        std::string command =
            "cd $KAFKAHOME/bin; ./kafka-topics.sh --delete --zookeeper " + zookeeper + " --topic " + topic;
        int status = system(command.c_str());

        if (status != 0) {
            std::cerr << "command: " << command << "failed with status code: " << status << std::endl;
        }
        command = "cd $KAFKAHOME/bin; ./kafka-topics.sh --create --zookeeper " + zookeeper +
            " --replication-factor 1 --partitions " + std::to_string(num_of_partitions) + " --topic " + topic;
        status = system(command.c_str());
        if (status != 0) {
            std::cerr << "command: " << command << "failed with status code: " << status << std::endl;
        }

    } else {
        printf("Assuming Kafka topic %s already exists. If it does not exists use --create flag at the end of "
               "parameters list\n",
               topic.c_str());
    }

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    std::string errstr;

    // Building the config
    if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return false;
    }

    auto ex_dr_cb = new ExampleDeliveryReportCb();
    if (conf->set("dr_cb", ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        return false;
    }

    RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return false;
    }

    delete conf;

    std::vector<std::string> tableNames;
    for (int i = 0; i < num_of_tables; i++) {
        tableNames.push_back("table" + std::to_string(i));
    }

    // For the number of message generate <headers, message>
    std::vector<std::pair<RdKafka::Headers*, std::string>> messages;
    for (int i = 0; i < num_of_messages; i++) {
        auto random_index = rand() % tableNames.size();
        std::string table = tableNames[random_index];
        std::string data = random_string(message_size);
        RdKafka::Headers* headers = RdKafka::Headers::create();
        headers->add("table", table);
        messages.emplace_back(headers, data);
    }

    for (auto& message : messages) {
        auto hdr = message.first->get("table");
        //        printf("Table: %s\n", (char *) hdr[0].value());

    retry:
        RdKafka::ErrorCode err = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
                                                   const_cast<char*>(message.second.c_str()), message.second.size(),
                                                   NULL, 0, 0, message.first, NULL);

        //            delete message.first;
        if (err != RdKafka::ERR_NO_ERROR) {
            std::cerr << "Failed to produce to topic " << topic << ": " << RdKafka::err2str(err) << std::endl;
            delete message.first;

            if (err == RdKafka::ERR__QUEUE_FULL) {
                producer->poll(1000 /*block for max 1000ms*/);
                goto retry;
            }

        } else {
            // commenting for now
            //            std::cout << "Enqueued message (" << message.second.size() << " bytes) " <<
            //                      "for topic " << topic << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    }
    producer->poll(0);
    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000 /* wait for max 10 seconds */);

    if (producer->outq_len() > 0)
        std::cerr << "% " << producer->outq_len() << " message(s) were not delivered" << std::endl;
    delete ex_dr_cb;
    delete producer;
    return true;
}

// int main(int argc, char** argv) {
//     if (argc < 6 || (argc > 6 && argc != 9)) {
//         std::cerr << "Usage: " << argv[0]
//                   << " <brokers> <topic> <num_of_messages> <num_of_tables> <message_size> [--create <zookeeper> "
//                      "<number_of_partitions>]\n";
//         exit(1);
//     }

//     std::string brokers = argv[1];
//     std::string topic = argv[2];
//     int num_of_messages = std::stoi(argv[3]);
//     int num_of_tables = std::stoi(argv[4]);
//     int message_size = std::stoi(argv[5]);

//     bool create_topic = false;
//     std::string zookeeper;
//     int num_of_partitions = 0;
//     if (argc > 6) {
//         create_topic = true;
//         zookeeper = argv[7];
//         num_of_partitions = std::stoi(argv[8]);
//     }
//     Producer producer(brokers, topic, num_of_messages, num_of_tables, message_size, create_topic, zookeeper,
//                       num_of_partitions);
//     return !producer.run();
// }
