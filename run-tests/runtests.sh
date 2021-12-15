#!/bin/bash
set -e

## to prepare the necessary server process that the unit tests interact with
## for example, start kafka, start clickhouse server, prepare the table schema

export test_top_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export current_user="$(whoami)"

echo -e "===========to launch kafka server (and also zookeeper server)=============="
scripts/run_kafka.sh 

# run aggregator simple tests.
#echo "==========run simple tests under scripts directory==========="
scripts/run_simple_tests.sh


# run kafka connector tests
echo "==========run kafka connector tests under scripts directory========="
scripts/run_kafkaconnector_tests.sh

sleep 10

# afterwards, shutdown zookeeper and then kafka server.
echo -e "===========to shutdown kafka server (and also zookeeper server)=============="
scripts/stop_kafka.sh 
