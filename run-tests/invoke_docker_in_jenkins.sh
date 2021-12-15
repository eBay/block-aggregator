#!/bin/bash
set -ex

root_directory=$(pwd)
#clone the code 

#NOTE: the following is to install one-time dependencies, need only once for each Jenkins machine
#bash external-deps-ubuntu.sh

#create dependencies 
#sudo apt install -y  libsasl2-dev
bash deps.sh

#build the code 
./bootstrap.sh -DUNITTEST=1
cmake --build ./cmake-build-release -- -j16 VERBOSE=1
#install the test code
cmake --build ./cmake-build-release -- install

echo "finish the full build process" 

cd $root_directory

#run the docker 
docker run \
        -v $(pwd)/run-tests:/opt/run-tests  \
        hub.tess.io/nudata/nucolumnaraggr:unittest-v0.1.0-release \
        bash -c "cd /opt/run-tests; . ./set_env.sh; ./runtests.sh"
