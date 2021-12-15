#!/bin/bash

# This script file is to serve as an example on how to build a docker image.
# The script should be modified for your own image build environment.

# The script should have the following inputs:
  * Input Parameter 1: the docker file specified in build_aggregator_image.sh
  *Input Parameter 2: the script directory specified in build_aggregator_image.sh
  * Input Parameter 3: the docker image repository specified in build_aggregator_image.sh
  *Input Parameter 4: the image tag chosen in build_aggregator_image.sh

# The script output is the docker image geting pushed into the image repo destination

name=$(date +%s%N)
if [ -z "$5" ]
then
  cmd="executor --log-path=/data/dockerfiles/rst-request/${name}.log --dockerfile=$1 --context=$2 --destination=$3:$4 --destination=$3:latest"
else
  cmd="executor --log-path=/data/dockerfiles/rst-request/${name}.log --dockerfile=$1 --context=$2 --destination=$3:$4 --destination=$3:latest --build-arg $5"
fi
echo "Executing cmd: $cmd"
echo "$cmd" > /data/dockerfiles/request/$name
echo "Waiting cmd result /data/dockerfiles/rst-request/$name"

while true
do
  if [ -f /data/dockerfiles/rst-request/$name ]
  then
    cat /data/dockerfiles/rst-request/${name}.log
    result=$(cat /data/dockerfiles/rst-request/$name)
    echo "Cmd exit code: $result"
    exit $result
  else
    sleep 5
  fi
done
