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
#include <queue>
#include <thread>
#include <chrono>
#include <iostream>
#include <fstream>
#include <atomic>
#include <mutex>
#include <condition_variable>

#include "common/logging.hpp"

class FileWriter {
  private:
    std::atomic<int64_t> lastWritten;
    std::atomic<bool> running;
    std::queue<std::vector<std::string>> batchesQueue; // each entry is a batch of messages
    std::mutex queueMtx;
    std::condition_variable queueCv;

    std::thread* writerThread;
    std::fstream fs;

    void writerThreadFunction() {
        while (running) {
            std::unique_lock<std::mutex> lck(queueMtx);
            if (!batchesQueue.empty()) {
                auto f = batchesQueue.front();
                batchesQueue.pop();
                lck.unlock();
                fs << "new batch>" << f.size() - 1 << std::endl;
                int i = 0;
                for (auto& message : f) {
                    fs << message << std::endl;
                    i++;
                }
                lastWritten++;
                LOG_KAFKA(2) << "Batch wrote to filestream, batch size: " << f.size();
            }
            queueCv.notify_all();
        }
    }

    FileWriter(const std::string& file_name) : lastWritten(-1), running(false) {
        fs.open(file_name, std::ios::out | std::ios::app);
        if (!fs.good())
            std::cerr << "Cannot open file: " << file_name << std::endl;
    }

    static FileWriter* instance;

  public:
    static void stop() {
        instance->stop_();
        delete instance;
        instance = nullptr;
    }

    static FileWriter* getInstance(const std::string& file_name = "") {
        if (!instance) {
            instance = new FileWriter(file_name);
            instance->start();
        }
        return instance;
    }

    void start() {
        running = true;
        writerThread = new std::thread(&FileWriter::writerThreadFunction, this);
    }

    void stop_() {
        running = false;
        {
            std::unique_lock<std::mutex> lck(queueMtx);
            queueCv.wait(lck, [&] { return batchesQueue.empty(); });
        }
        if (writerThread && writerThread->joinable())
            writerThread->join();
        delete writerThread;
        if (fs.is_open())
            fs.close();
        lastWritten = -1;
        running = false;
    }

    int64_t insert(const std::vector<std::string>& batch) {
        std::unique_lock<std::mutex> lck(queueMtx);
        batchesQueue.push(batch);
        return (int)batchesQueue.size() - 1;
    }

    void wait(int64_t id) {
        while (lastWritten < id) {
            LOG_KAFKA(2) << "FileWrite: waiting... id=" << id << ", lastWritten=" << lastWritten;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
};
