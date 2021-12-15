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

#include "test_common.h"

#include "KafkaConnector/GlobalContext.h"
#include "KafkaConnector/SimpleBuffer.h"
#include "KafkaConnector/SimpleBufferFactory.h"
#include "KafkaConnector/KafkaConnector.h"
#include "KafkaConnector/SimpleThreadPool.h"

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
        LOG(ERROR) << "Failed to disable auto commit: " << errstr << std::endl;
        exit(1);
    } else {
        LOG(INFO) << "Kafka configuration successfully set the flag: auto_offset_reset to be: " << auto_offset_reset;
    }
    return conf;
}

int basic_test() {
    std::string path = getConfigFilePath("my_simple_aggregator_config.json");
    LOG(INFO) << " JSON configuration file path is: " << path;

    SETTINGS_FACTORY.load(path); // force to load the configuration setting as the global instance.

    auto kafka_consumer_params = SETTINGS_PARAM(config.kafka.consumerConf);

    kafka::GlobalContext::instance().setBufferFactory(std::make_shared<kafka::SimpleBufferFactory>());

    // create your KafkaConnector object to listen to the topic
    std::shared_ptr<kafka::GenericThreadPool> thread_pool = std::make_shared<kafka::SimpleThreadPool>();
    thread_pool->init();

    auto conf = createConfig();
    kafka::KafkaConnector kafkaConnector(0, conf, thread_pool, nullptr);
    kafkaConnector.start();

    auto buffer = kafka::GlobalContext::instance().getBufferFactory()->createBuffer(
        0, "myTable", kafka_consumer_params.buffer_batch_processing_size,
        kafka_consumer_params.buffer_batch_processing_timeout_ms, &kafkaConnector);
    buffer->flush();
    CHK_EQ(false, buffer->flushable());
    // need to be sighly bigger than the specified time-out value above.
    std::this_thread::sleep_for(std::chrono::milliseconds(3050));
    CHK_EQ(true, buffer->flushable());

    buffer->flush();
    CHK_EQ(false, buffer->flushable());
    for (int64_t i = 0; i < (int64_t)kafka_consumer_params.buffer_batch_processing_size; i++) {
        int64_t timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        buffer->append("garbage data", strlen("garbage data"), i, timestamp);
        buffer->setCount(buffer->count() + 1);
    }
    CHK_EQ(true, buffer->flushable());
    CHK_EQ(0, buffer->begin());
    CHK_EQ((int64_t)kafka_consumer_params.buffer_batch_processing_size - 1, buffer->end());

    kafkaConnector.stop();
    thread_pool->shutdown();

    return 0;
}

int main(int argc, char** argv) {
    // to globally initialize glog
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    TestSuite ts(argc, argv);
    ts.doTest("basic_test", basic_test);
    return 0;
}
