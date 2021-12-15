#!/bin/bash
set -e

#GLOG_V related logging, as for testing, we may not need to log specifically to a defined log module (like in production)
export GLOG_logtostderr=1
#export GLOG_log_dir=/tmp/nuclm
export GLOG_v=4

#ASAN (memory sanitizer used in debug mode)
export LSAN_OPTIONS=verbosity=1:log_threads=1
export ASAN_OPTIONS=detect_container_overflow=0,alloc_dealloc_mismatch=0

echo "LSAN_OPTIONS: $LSAN_OPTIONS"
echo "ASAN_OPTIONS: $ASAN_OPTIONS"

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
project_root="$( cd "${DIR}/.." && pwd )"
echo "project root is: $project_root"
export TEST_CONFIG_FILE_PATH=${project_root}/run-tests/conf
echo "TEST_CONFIG_FILE_PATH is set at: ${TEST_CONFIG_FILE_PATH}"

export COMMAND_FLAGS_DIR=${project_root}/run-tests/nucolumnaraggrflags
echo "COMMAND_FLAGS_DIR is set at: ${COMMAND_FLAGS_DIR}"

#for kafka-connector related testing
export KAFKAHOME=/opt/kafka_2.11-2.1.1



## to prepare the necessary server process that the unit tests interact with
## for example, start kafka, start clickhouse server, prepare the table schema

export test_top_dir=${project_root}/run-tests
export current_user="$(whoami)"

# run aggregator simple tests.
#echo "==========run simple tests under scripts directory==========="
cd $test_top_dir;scripts/run_simple_coverage_tests.sh


