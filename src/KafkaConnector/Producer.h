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

#include <string>

class Producer {
  private:
    std::string brokers;
    std::string topic;
    int num_of_messages;
    int num_of_tables;
    int message_size;
    int delay;
    bool create_topic;
    std::string zookeeper;
    int num_of_partitions;

  public:
    Producer(std::string brokers_, std::string topic_, int num_of_messages_, int num_of_tables_, int message_size_,
             int delay_, bool create_topic_ = false, std::string zookeeper_ = "", int num_of_partitions_ = 1) :
            brokers(brokers_),
            topic(topic_),
            num_of_messages(num_of_messages_),
            num_of_tables(num_of_tables_),
            message_size(message_size_),
            delay(delay_),
            create_topic(create_topic_),
            zookeeper(zookeeper_),
            num_of_partitions(num_of_partitions_) {}
    bool run();
};
