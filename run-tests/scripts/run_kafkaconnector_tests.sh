#!/bin/bash 
set -e

echo "in scripts directory, to run kafka connector tests, test_top_dir is:  ${test_top_dir}"
## the test invokes "./producer", which is in the run-tests/deployed directory.
#export PATH=$PATH:${test_top_dir}/deployed

echo "in scripts directory, to run kafka connector tests, PATH env specified is: ${PATH}"

echo -e "\n===========run metadata_tests ==========\n"
cd ${test_top_dir}/deployed; ./metadata_tests

echo -e "\n===========run buffer_tests ==========\n"
cd ${test_top_dir}/deployed; ./buffer_tests

echo -e "\n===========run flushtask_tests ==========\n"
cd ${test_top_dir}/deployed; ./flushtask_tests

echo -e "\n===========run kafka_connector_tests ==========\n"
cd ${test_top_dir}/deployed; ./kafka_connector_tests
