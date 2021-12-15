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

// NOTE: The following two header files are necessary to invoke the three required macros to initialize the
// required static variables:
//   THREAD_BUFFER_INIT;
//   FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
//   RCU_REGISTER_CTL;
#include "libutils/fds/thread/thread_buffer.hpp"
#include "common/logging.hpp"
#include "common/settings_factory.hpp"

#include <Aggregator/SerializationHelper.h>
#include <Aggregator/ProtobufBatchReader.h>

#include <nucolumnar/aggregator/v1/nucolumnaraggregator.pb.h>
#include <nucolumnar/datatypes/v1/columnartypes.pb.h>

#include <librdkafka/rdkafkacpp.h>
#include <glog/logging.h>

#include <iostream>
#include <random>
#include <string>
#include <algorithm>
#include <stdlib.h>

// NOTE: required for static variable initialization for ThreadRegistry and URCU defined in libutils.
THREAD_BUFFER_INIT;
// We need to extern declare all the modules, so that registered modules are usable.
FOREACH_VMODULE(VMODULE_DECLARE_MODULE);
RCU_REGISTER_CTL;

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

static int random_value() {
    int max_value = 10000000;
    std::random_device rd;
    std::mt19937 engine(rd());
    std::uniform_int_distribution<> dist(0, max_value - 1);
    std::string ret = "";
    int random_value = dist(engine); // get index between 0 and possible_characters.size()-1
    return random_value;
}

std::string generate_message(const std::string& shard, const std::string& table, const std::string sql) {
    // for deterministic testing, having this to be the deterministic value.
    int counter = random_value();
    // int counter = 10090999;
    LOG(INFO) << "random string starts with counter: " << counter;

    nucolumnar::aggregator::v1::DataBindingList bindingList;
    nucolumnar::aggregator::v1::SQLBatchRequest sqlBatchRequest;
    sqlBatchRequest.set_shard(shard);
    sqlBatchRequest.set_table(table);

    nucolumnar::aggregator::v1::SqlWithBatchBindings* sqlWithBatchBindings =
        sqlBatchRequest.mutable_nucolumnarencoding();
    sqlWithBatchBindings->set_sql(sql);
    sqlWithBatchBindings->mutable_batch_bindings();

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(7123456 + counter);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("nudata-nudata-abc-7");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("nudata-nudata-xyz-7");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        // value 4, array of array of UInt32.
        {
            // fill out Counters1: Array(Array(UInt32))
            long start_uint_value = rand() % 1000000;
            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
            nucolumnar::datatypes::v1::ListValueP* array_value = val4->mutable_list_value();
            {
                // each array value is also an array, with three elements
                nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                    array_value->add_value()->mutable_list_value();
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            }
            {
                // each array value is also an array, with two elements
                nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                    array_value->add_value()->mutable_list_value();
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            }

            // add val4
            nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
            pval4->CopyFrom(*val4);
        }
    }
    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(6123456 + counter);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("nudata-nudata-abc-16");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("nudata-nudata-xyz-16");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        // value 4, array of array of UInt32.
        {
            // fill out Counters1: Array(Array(UInt32))
            long start_uint_value = rand() % 1000000;
            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
            nucolumnar::datatypes::v1::ListValueP* array_value = val4->mutable_list_value();
            {
                // each array value is also an array, with three elements
                nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                    array_value->add_value()->mutable_list_value();
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            }
            {
                // each array value is also an array, with two elements
                nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                    array_value->add_value()->mutable_list_value();
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            }

            // add val1
            nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
            pval4->CopyFrom(*val4);
        }
    }

    {
        nucolumnar::aggregator::v1::DataBindingList* dataBindingList = sqlWithBatchBindings->add_batch_bindings();
        // value 1
        nucolumnar::datatypes::v1::ValueP* val1 = bindingList.add_values();
        val1->set_long_value(5123456 + counter);
        // value 2
        nucolumnar::datatypes::v1::ValueP* val2 = bindingList.add_values();
        val2->set_string_value("nudata-nudata-abc-15");
        // value 3
        nucolumnar::datatypes::v1::ValueP* val3 = bindingList.add_values();
        val3->set_string_value("nudata-nudata-xyz-15");

        nucolumnar::datatypes::v1::ValueP* pval1 = dataBindingList->add_values();
        pval1->CopyFrom(*val1);

        nucolumnar::datatypes::v1::ValueP* pval2 = dataBindingList->add_values();
        pval2->CopyFrom(*val2);

        nucolumnar::datatypes::v1::ValueP* pval3 = dataBindingList->add_values();
        pval3->CopyFrom(*val3);

        // value 4, array of array of UInt32.
        {
            // fill out Counters1: Array(Array(UInt32))
            long start_uint_value = rand() % 1000000;
            nucolumnar::datatypes::v1::ValueP* val4 = bindingList.add_values();
            nucolumnar::datatypes::v1::ListValueP* array_value = val4->mutable_list_value();
            {
                // each array value is also an array, with three elements
                nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                    array_value->add_value()->mutable_list_value();
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            }
            {
                // each array value is also an array, with two elements
                nucolumnar::datatypes::v1::ListValueP* array_value_nested_array =
                    array_value->add_value()->mutable_list_value();
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
                array_value_nested_array->add_value()->set_uint_value(start_uint_value++);
            }

            // add val4
            nucolumnar::datatypes::v1::ValueP* pval4 = dataBindingList->add_values();
            pval4->CopyFrom(*val4);
        }
    }

    std::string serializedSqlBatchRequestInString = sqlBatchRequest.SerializeAsString();
    return serializedSqlBatchRequestInString;
}

int main(int argc, char** argv) {
    if (argc < 5 || (argc > 5 && argc != 8)) {
        std::cerr
            << "Usage: " << argv[0]
            << " <brokers> <topic> <num_of_messages> <num_of_tables>  [--create <zookeeper> <number_of_partitions>]\n";
        exit(1);
    }

    srand(time(NULL)); // create a random seed, when to use rand()

    std::string brokers = argv[1];
    std::string topic = argv[2];
    int num_of_messages = std::stoi(argv[3]);
    int num_of_tables = std::stoi(argv[4]);

    if (num_of_tables != 1) {
        std::cerr << "Only one table is supported currently\n";
        exit(1);
    }

    bool create_topic = false;
    std::string zookeeper;
    std::string num_of_partitions;
    if (argc > 5) {
        create_topic = true;
        zookeeper = argv[6];
        num_of_partitions = argv[7];
    }

    if (create_topic) {
        // create Kafka topic here. Delete and re-create it, if it already exists!
        printf("Creating Kafka topic %s... (kafka-topics.sh must be in your PATH)\n", topic.c_str());
        std::string command = "kafka-topics.sh --delete --zookeeper " + zookeeper + " --topic " + topic;
        int status = system(command.c_str());
        if (status != 0) {
            std::cerr << "command: " << command << "failed with status code: " << status << std::endl;
        }
        command = "kafka-topics.sh --create --zookeeper " + zookeeper + " --replication-factor 1 --partitions " +
            num_of_partitions + " --topic " + topic;
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
        exit(1);
    }

    auto ex_dr_cb = new ExampleDeliveryReportCb();
    if (conf->set("dr_cb", ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;

    std::vector<std::string> tableNames;
    { tableNames.push_back("simple_event_68"); }

    // For the number of message generate <headers, message>
    std::vector<std::pair<RdKafka::Headers*, std::string>> messages;
    for (int i = 0; i < num_of_messages; i++) {
        // auto random_index = rand() % tableNames.size();
        int random_index = 0;
        std::string table = tableNames[random_index];
        std::string shard_id = "1111";
        std::string insert_query =
            "insert into simple_event_68 (`Count`, `Host`, `Colo`, `Counters` ) VALUES (?, ?, ?, ?)";
        // std::string generate_message(const std::string& shard, const std::string& table, const std::string sql )
        std::string data = generate_message(shard_id, table, insert_query);
        RdKafka::Headers* headers = RdKafka::Headers::create();
        headers->add("table", table);
        messages.emplace_back(headers, data);
    }

    for (auto& message : messages) {
        auto hdr = message.first->get("table");
        printf("Table: %s\n", (char*)hdr[0].value());

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
            std::cout << "Enqueued message (" << message.second.size() << " bytes) "
                      << "for topic " << topic << std::endl;
        }
    }
    producer->poll(0);
    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000 /* wait for max 10 seconds */);

    if (producer->outq_len() > 0)
        std::cerr << "% " << producer->outq_len() << " message(s) were not delivered" << std::endl;
    delete producer;
}
