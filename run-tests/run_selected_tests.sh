#!/bin/bash
set -e

SIMPLE_TESTS="false"
KAFKA_CONNECTOR_TESTS="false"
MIB_INTEGRATION_TESTS="false"

function usage()
{
  echo "run_selected_tests.sh input options"
  echo "./run_selected_tests.sh"
  echo " --help to print out the options"
  echo " --simple_tests=<true|false>"
  echo " --kafka_connector_tests=<true|false>"
  echo " --mib_integration_tests=<true|false>"
}

while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --simple_tests)
            SIMPLE_TESTS=$VALUE
            ;;
        --kafka_connector_tests)
            KAFKA_CONNECTOR_TESTS=$VALUE
            ;;
        --mib_integration_tests)
            MIB_INTEGRATION_TESTS=$VALUE
            ;;
        *)
            echo "ERROR: unknown parameter \"$PARAM\""
            usage
            exit 1
            ;;
    esac
    shift
done

## to prepare the necessary server process that the unit tests interact with
## for example, start kafka, start clickhouse server, prepare the table schema

export test_top_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export current_user="$(whoami)"

if [ "$SIMPLE_TESTS" = "true" ]; then
    # run aggregator simple tests.
    echo "==========run simple tests under scripts directory========="
    scripts/run_simple_tests.sh
fi

if [ "$KAFKA_CONNECTOR_TESTS" = "true" ]; then
   echo -e "===========to launch kafka server=============="
   scripts/run_kafka.sh 

   # run kafka connector tests
   echo "==========run kafka connector tests under scripts directory========="
   scripts/run_kafkaconnector_tests.sh
fi
