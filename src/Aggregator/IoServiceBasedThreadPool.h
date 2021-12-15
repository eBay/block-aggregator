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

#include <KafkaConnector/GenericThreadPool.h>

#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

#include <glog/logging.h>

#include <memory>
#include <atomic>

namespace nuclm {

class IOServiceBasedThreadPool : public kafka::GenericThreadPool {
  public:
    IOServiceBasedThreadPool(boost::asio::io_service* io_service_, size_t thread_num_) :
            io_service(io_service_), number_of_thread(thread_num_), running(true) {}

    void init() override {
        pwork.reset(new boost::asio::io_service::work(*io_service));

        for (size_t i = 0; i < number_of_thread; i++) {
            thread_pool.create_thread([this, i]() {
                // It's for kafka consumer only, so name it
                std::string name = "KConsumer-" + std::to_string(i);
#ifdef __APPLE__
                pthread_setname_np(name.c_str());
#else
                pthread_setname_np(pthread_self(), name.c_str());
#endif /* __APPLE__ */
                LOG(INFO) << "Kafka consumer thread " << i << " started";
                io_service->run();
                LOG(INFO) << "Kafka consumer thread " << i << " exited";
            });
        }

        LOG(INFO) << "io-service-based thread pool started with number of threads: " << number_of_thread;
    }

    // to invoke: pool.enqueue(boost::protect (boost::bind(&apache::MyClient::ping, _obj));
    void enqueue(const kafka::GenericThreadPool::TaskFunc& func) override {
        if (running) {
            io_service->post(
                boost::bind(&IOServiceBasedThreadPool::execute<kafka::GenericThreadPool::TaskFunc>, this, func));
        }
    }

    void wait_on_termination() {
        try {
            thread_pool.join_all(); // wait for all completion.
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception: " << e.what() << " in io-service thread pool shutdown";
        }
    }

    void shutdown() override {
        running = false;

        // need to be able to shutdown the work while it is running on the IO quickly by passing some signal.
        pwork.reset(); // NOTE: what is the semantics? stop all.
        io_service->stop();

        try {
            thread_pool.join_all(); // wait for all completion.
        } catch (const std::exception& e) {
            LOG(ERROR) << "Exception: " << e.what() << " in io-service thread pool shutdown";
        }

        LOG(INFO) << "io-service thread pool is shutdown";
    }

    ~IOServiceBasedThreadPool() = default;

  private:
    // this will be executed when one of the threads is available.
    template <typename TFunc> void execute(const TFunc& func) { func(); }

  private:
    boost::asio::io_service* io_service;
    boost::thread_group thread_pool;
    std::shared_ptr<boost::asio::io_service::work> pwork; // to stop the thread pool from existing
    size_t number_of_thread;
    std::atomic<bool> running;
};

} // namespace nuclm
