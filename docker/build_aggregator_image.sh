#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "build-image script dir: $SCRIPT_DIR"

MODE=$1
IMAGE_REPO_FOR_AGGREGATOR=$2

BIN_DIR=cmake-build-${MODE:-debug}

# copy the  binaries needed for docker build into corresponding staging dirs (nucolumnarbins/, /bins, sharedlibs/)
mkdir -p ./sharedlibs
mkdir -p ./nucolumnarbins
mkdir -p ./bins

# copy required shared libs from local build environment into BIN_DIR/sharedlibs/
cp /usr/lib/x86_64-linux-gnu/libasan.so.5 ./sharedlibs/
cp /usr/lib/x86_64-linux-gnu/libjemalloc.so.1 ./sharedlibs/
cp /usr/lib/x86_64-linux-gnu/libubsan.so.1 ./sharedlibs/
cp /usr/lib/x86_64-linux-gnu/libstdc++.so.6 ./sharedlibs/
#addr2line
#cp /usr/bin/addr2line ./bins/addr2line
cp /usr/lib/x86_64-linux-gnu/libbfd-2.30-system.so ./sharedlibs/

chmod a+x ./sharedlibs/*

cp -f ../deps_prefix/bin/minidump_stackwalk ./nucolumnarbins/
cp -f ../$BIN_DIR/NuColumnarAggr ./nucolumnarbins/
cp -f ../$BIN_DIR/NuColumnarAggr.debug ./nucolumnarbins/
cp -rf ../$BIN_DIR/symbols ./nucolumnarbins/

# add git version number to version.
tag=$(git rev-parse --short HEAD)
echo "version tag is: $MODE-$tag"
echo "$MODE-$tag" > ./nucolumnarbins/version

chmod a+x ./nucolumnarbins/NuColumnarAggr

# now to build the docker image for aggregator
IMAGE_TAG=$tag-${MODE:-debug}
./build-docker.sh  ${SCRIPT_DIR}/Dockerfile_nucolumnaraggr ${SCRIPT_DIR} ${IMAGE_REPO_FOR_AGGREGATOR} ${IMAGE_TAG}

#cleanup
rm -fr ./nucolumnarbins/version
rm -fr ./nucolumnarbins/minidump_stackwalk
rm -fr ./nucolumnarbins/NuColumnarAggr
rm -fr ./nucolumnarbins/NuColumnarAggr.debug
rm -fr ./nucolumnarbins/symbols
rm -fr ./sharedlibs/*
