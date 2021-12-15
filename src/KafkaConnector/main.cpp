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

#include "GlobalContext.h"
#include "SimpleBufferFactory.h"
#include "FileWriter.h"

#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <future>
#include <random>
#include <string>
#include <algorithm>

class Offset {
  public:
    int64_t begin;
    int64_t end;
};

const char separator = ',';
std::string serialize(std::unordered_map<std::string, Offset>& offsets) {
    std::ostringstream ss;
    for (auto& offset : offsets) {
        ss << offset.first << separator << offset.second.begin << separator << offset.second.end << separator;
    }
    return ss.str();
}

void deserialize(const char* meta, std::unordered_map<std::string, Offset>& offsets) {
    std::stringstream s_stream(meta); // create string stream from the string
    while (s_stream.good()) {
        std::string table;
        getline(s_stream, table, separator);
        if (table.empty())
            break;

        std::string begin_str;
        getline(s_stream, begin_str, separator);

        std::string end_str;
        getline(s_stream, end_str, separator);

        offsets[table].begin = std::stol(begin_str);
        offsets[table].end = std::stol(end_str);
    }
}

int64_t min(std::unordered_map<std::string, Offset>& offsets) {
    int64_t min_ = INT64_MAX;
    for (auto& offset : offsets) {
        min_ = std::min(min_, offset.second.begin);
    }
    if (min_ == INT64_MAX)
        min_ = -1l;
    return min_;
}

int64_t max(std::unordered_map<std::string, Offset>& offsets) {
    int64_t max_ = -1l;
    for (auto& offset : offsets) {
        max_ = std::max(max_, offset.second.end);
    }
    return max_;
}

bool writeToFile(std::string file_name) {
    std::fstream fs;
    fs.open("cache.txt", std::ios::out | std::ios::app);
    if (!fs.good())
        return false;
    fs << "Hello" << std::endl;
    fs.close();
    return true;
}

std::string generate(int max_length) {
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

int main(int argc, char** argv) {
    auto randomStr = generate(10);
    std::cout << generate(3) << "\t" << generate(10);

    kafka::GlobalContext::instance().setBufferFactory(std::make_shared<kafka::SimpleBufferFactory>());

    //    FileWriter::getInstance("text.txt")->start();
    //    FileWriter::getInstance()->insert({"testText1"});
    //    FileWriter::getInstance()->insert({"testText2"});
    //    FileWriter::getInstance()->insert({"testText3"});
    //    FileWriter::getInstance()->insert({"testText4"});
    //    auto id = FileWriter::getInstance()->insert({"testText5", "testText6"});
    //    FileWriter::getInstance()->wait(id);
    //    FileWriter::getInstance()->stop();

    //    //std::cout<< kafka::KAFKA_BATCH_SIZE << std::endl;
    //    kafka::Buffer buffer("table");
    //    std::ostringstream ss;
    //    std::vector<std::string> vec({"table1","2","3","table2","1","10"});
    //    std::string separator;
    //    for (auto x : vec){
    //        ss << separator << x;
    //        separator = ",";
    //    }
    //    std::string output = ss.str();
    //    std::cout << output << std::endl;
    //
    //    const char* meta = output.c_str();
    //    std::unordered_map<std::string, Offset> myMap;
    //
    //    std::stringstream s_stream(meta); //create string stream from the string
    //    while(s_stream.good()) {
    //        std::string table;
    //        getline(s_stream, table, ',');
    //
    //        std::string begin_str;
    //        getline(s_stream, begin_str, ',');
    //
    //        std::string end_str;
    //        getline(s_stream, end_str, ',');
    //
    //        myMap[table].begin = std::stol(begin_str);
    //        myMap[table].end = std::stol(end_str);
    //    }
    //    auto serialized_str = serialize (myMap);
    //    std::cout << "after serialization: " <<  serialized_str << std::endl;
    //
    //    std::unordered_map<std::string, Offset> out;
    //    deserialize(serialized_str.c_str(), out);
    //    std::cout << "after deserialized a serialized and serialized again!: " <<  serialize (out) << std::endl;
    //
    //    std::cout << "min: " << min(out) <<  " max: " << max(out) << std::endl;
    return 0;
}
