# Block Aggregator

Block Aggregator is a data loader that subscribes to Kafka topics, aggregates the Kafka messages into blocks that follow the Clickhouse’s table schemas, and then inserts the blocks into ClickHouse. Block Aggregator provides exactly-once delivery guarantee to load data from Kafka to ClickHouse. Block Aggregator utilizes Kafka’s metadata to keep track of blocks that are intended to send to ClickHouse, and later uses this metadata information to deterministically re-produce ClickHouse blocks for re-tries in case of failures. The identical blocks are guaranteed to be deduplicated by ClickHouse. 

Please refer to [the article](https://tech.ebayinc.com/engineering/block-aggregator-real-time-data-ingestion-from-kafka-to-clickhouse-with-deterministic-retries/) and [the presentation](https://www.youtube.com/watch?v=4p5mo6IiVHE) for more detail. 


##  Features 

* No data loss/duplication when loading data from Kafka to ClickHouse
* Support multi-shard and multi-datacenter ClickHouse deployment model
* Support Kafka message consumption from multiple Kafka clusters
* Loading of multiple ClickHouse tables from one Kafka topic with multiple Kafka partitions
* Monitoring with over one hundred metrics:
     * Kafka message processing rate
     * Block insertion rate and failure rate
     * Block size distribution
     * Block loading time distribution
     * Kafka metadata commit time and failure rate
     * Whether abnormal message consumption behaviors happened (such as message offset re-wound or skipped)

## Supported Platforms

* Ubuntu (tested on 18.04) 

The Mac environment can support build and run Block Aggregator on its earlier version but not on the current version.

## How to Build 

The build enviroment requires Utuntu 18.04.

At the top directory of the repo, follow these steps:

#### Step 1: Install External dependencies

The dependencies include cmake 3.16, gcc 10.3 and Boost Library 1.75.0. 

```shell script
./external-deps-ubuntu.sh
```

#### Step 2: Build and install dependent library modules

This includes building the dependent libraries from the source code, including protobuf, flatbuffer,  librdkafka and ClickHouse. The current ClickHouse version chosen is v21.8.3.44-lts.

```shell script
./deps.sh
```

**Notes**: building ClickHouse from source code may take more than two hours and the build of ClickHouse includes both release build and debug build. Thus if no need to build Block Aggregator's debug build and only the ClickHouse release build is needed, then in ``deps.sh``, change the following function:

```shell script
ClickHouse () {
    ClickHouse_ 'Debug' true
    ClickHouse_ 'Release'
}
```
to become:

```shell script
ClickHouse () {
    ClickHouse_ 'Release' true
}
```
#### Step 3: Bootstrap

This is to generate the cmake related build scripts

```shell script
rm -rf ./cmake-build-*
./bootstrap.sh 
```

If to build unit tests in addition, then run the following command:

```shell script
rm -rf ./cmake-build-*
./bootstrap.sh -DUNITTEST=ON
```

#### Step 4: Build 

To build in release mode:

```shell script
cmake --build ./cmake-build-release -- -j8 VERBOSE=1 
```

Or, to build the debug version: 

```shell script
cmake --build ./cmake-build-debug -- -j8 VERBOSE=1
```

The built application is located at `cmake-build-release` (or `cmake-build-debug`), named `NuColumnarAggr`. 

If we issue earlier with `./bootstrap.sh -DUNITTEST=ON`, then we can build both the application and the unit test suites. In the release mode, we can invoke:

```shell script
cmake --build ./cmake-build-release -- -j8 VERBOSE=1 install
```

or in the debug mode:

```shell script
cmake --build ./cmake-build-debug -- -j8 VERBOSE=1 install
```

All of the unit test suites related executables are built and installed under the directory: `run-test/deployed`. 

**Notes**: In the debug mode, building the application executable or each of the test executables can take  more than 15 minutes, because linking to the debug version of ClickHouse libraries is slow. Future release will consolidate the current test suites into small number of test executables. 

## Run Unit Tests


#### Step 1: Test Framework Preparation on Kafka, ZooKeeper and ClickHouse


We need to make sure that a Kafka process, a ZooKeeper process, and a ClickHouse server with ClickHouse version 21.8, are accessible from a Linux-based test environment.

The Kafaka process and the ZooKeeper process can be set up by download a kafka binary distribution, such as [kafka_2.11-2.1.1.tgz] (https://kafka.apache.org/downloads).

The ClickHouse server can be set up via [the ClickHouse installation guide] (https://clickhouse.com/docs/en/getting-started/install/)

The IP address information related to the Kafka process, the ZooKeeper process and the ClickHouse server process need to be updated to the configuration files located under `run-tests/conf`

* example_aggregator_config.xml
* example_aggregator_config_with_tls.json
* example_aggregator_config_for_distributed_locking.json


#### Step 2: Loading Table Schema for Testing Related Tables 
 
Follow the instructions given in [the readme file](./run-tests/metadata/README.md) to load the table schema for all of the testing related tables into the ClickHouse server process.

#### Step 3: Run Unit Tests

Invoke the following command to run all of the unit tests: 

```shell script
cd ./run-tests; . ./set_env.sh; ./runtests.sh 
```


## Integration Tests

An integration test example to launch one Block Aggregator instance and consume Kafka message batches can be found under the directory `run-tests/scripts/simple_kafka_producer`. Please refer to the steps in [the readme file] (./run-tests/scripts/simple_kafka_produce/readme.txt) to invoke the scripts and check the loaded rows in ClickHouse.

## Docker Build

Follow the instructions given in [the readme file](./docker/README.md) to build the docker image.


## Contributing to This Project

We welcome contributions. If you find any bugs, potential flaws and edge cases, improvements, new feature suggestions or discussions, please submit issues or pull requests.


## Contact 


* Jun Li (<junli5@ebay.com>)


## License Information 


Copyright 2020-2021 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## 3rd Party Code 

1. URL: https://github.com/ClickHouse/ClickHouse<br>
License Information: https://github.com/ClickHouse/ClickHouse/blob/master/LICENSE<br>
Originally licensed under the Apache 2.0 license.

2. URL: https://github.com/google/farmhash.git<br>
License Information: https://github.com/google/farmhash/blob/master/README<br>
Originally licensed under the MIT license.

3. URL: https://github.com/google/protobuf<br>
License Information: https://github.com/protocolbuffers/protobuf/blob/master/LICENSE<br>
Copyright 2008 Google Inc.

4. URL: https://github.com/google/flatbuffers<br>
License Information: https://github.com/google/flatbuffers/blob/master/LICENSE.txt<br>
Originally licensed under the Apache 2.0 license.

5. URL: https://github.com/edenhill/librdkafka<br>
License Information: https://github.com/edenhill/librdkafka/blob/master/LICENSE<br>
Copyright 2012-2020, Magnus Edenhill.

6. URL: https://github.com/google/glog/<br>
License Information: https://github.com/google/glog/blob/master/COPYING<br>
Copyright 2008, Google Inc.

7. URL: https://github.com/jupp0r/prometheus-cpp<br>
License Information: https://github.com/jupp0r/prometheus-cpp/blob/master/LICENSE<br>
Originally licensed under the MIT license.

8. URL: https://github.com/urcu/userspace-rcu<br>
License Information:  https://github.com/urcu/userspace-rcu/blob/master/LICENSE<br>
Originally licensed under the LGPLv2.1 license

9. URL: https://github.com/nlohmann/json<br>
License Information:  https://github.com/nlohmann/json/blob/develop/LICENSE.MIT<br>
Originally licensed under the MIT License

10. URL: https://github.com/arun11299/cpp-jwt<br>
License Information:  https://github.com/arun11299/cpp-jwt/blob/master/LICENSE<br>
Originally licensed under the MIT License

11. URL: https://chromium.googlesource.com/breakpad/breakpad.git<br>
License Information:  https://chromium.googlesource.com/breakpad/breakpad.git/+/refs/heads/main/LICENSE<br>
Copyright 2006 Google Inc.

12. URL: https://downloads.sourceforge.net/project/tclap<br>
License Information: https://sourceforge.net/p/tclap/code/ci/1.4/tree/COPYING<br>
Copyright 2003-2012 Michael E. Smoot, 2004-2016 Daniel Aarno, 2017-2021 Google LLC

13. URL: https://github.com/emcrisostomo/fswatch<br>
License Information: https://github.com/emcrisostomo/fswatch/blob/master/LICENSE-2.0.txt<br>
Originally licensed under the Apache 2.0 License.

14. URL: https://github.com/jemalloc/jemalloc<br>
License Information: https://github.com/jemalloc/jemalloc/blob/dev/COPYING<br>
Copyright 2002-present Jason Evans, 2007-2012 Mozilla Foundation, 2009-present Facebook Inc.

15. URL: https://github.com/google/googletest<br>
License Information: https://github.com/google/googletest/blob/main/LICENSE<br>
Originally licensed under the BSD 3-Clause "New" or "Revised" License