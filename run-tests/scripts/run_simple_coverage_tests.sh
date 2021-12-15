#!/bin/bash
set -e

# parameters of setting library path
#export LD_LIBRARY_PATH=/usr/local/lib

echo "current user: ${current_user}"
echo "in scripts directory, test_top_dir is:  ${test_top_dir}"

ls -al ${test_top_dir}
ls -al ${test_top_dir}/deployed

echo -e "\n===========Serializable: run test_serializer_all=====================\n"
cd ${test_top_dir}/deployed; ./test_serializer_all


echo -e "\n===========Aggregator: run test_aggregator_all=====================\n"
cd ${test_top_dir}/deployed; ./test_aggregator_all

echo -e "\n===========Aggregator: run test_main_launcher=====================\n"
cd ${test_top_dir}/deployed; ./test_main_launcher




