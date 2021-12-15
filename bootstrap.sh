#!/bin/bash

set -e

uname -a | grep Darwin >> /dev/null && is_mac=1 || true
uname -a | grep Ubuntu >> /dev/null && is_ubuntu=1 || true

if [ -n "$is_mac" ]; then
    sroot=${OSX_SYSROOT-$(xcrun --sdk macosx --show-sdk-path)}
    export flags=-isysroot\ $sroot
fi

deps_dir=`pwd`
if [[ -d $1 ]]; then
    deps_dir=$1
    shift
fi

./deps.sh -p $deps_dir
echo ---------- CMAKE_CXX_FLAGS=$flags --------
cmake -H. -Bcmake-build-debug \
    -DCMAKE_CXX_FLAGS="$flags" \
    -G "CodeBlocks - Unix Makefiles" \
    -DDEPS_DIR=$deps_dir \
    -DWITH_ASAN=ON \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCOVERAGE=OFF \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    -DWITH_PCH=OFF "$@"

cmake -H. -Bcmake-build-release \
     -DCMAKE_CXX_FLAGS="$flags" \
     -G "CodeBlocks - Unix Makefiles" \
     -DDEPS_DIR=$deps_dir \
     -DCMAKE_BUILD_TYPE=Release \
     -DWITH_PCH=ON "$@"


cmake -H. -Bcmake-build-debug-noasan \
     -DCMAKE_CXX_FLAGS="$flags" \
     -G "CodeBlocks - Unix Makefiles" \
     -DDEPS_DIR=$deps_dir \
     -DWITH_ASAN=OFF \
     -DCMAKE_BUILD_TYPE=Debug \
     -DCOVERAGE=OFF \
     -DWITH_PCH=OFF "$@"


# cmake -H. -Bcmake-build-gcov \
#     -DCMAKE_CXX_FLAGS="$flags" \
#     -G "CodeBlocks - Unix Makefiles" \
#     -DDEPS_DIR=$deps_dir \
#     -DCMAKE_BUILD_TYPE=Debug \
#     -DCOVERAGE=ON \
#     -DWITH_PCH=ON "$@"

# cmake -H. -Bcmake-build-prerelease \
#     -DCMAKE_CXX_FLAGS="$flags" \
#     -G "CodeBlocks - Unix Makefiles" \
#     -DDEPS_DIR=$deps_dir \
#     -DCMAKE_BUILD_TYPE=PreRelease \
#     -DWITH_PCH=ON "$@"

#after this build any of these two configurations with
#cmake --build ./cmake-build-release -- -j8
#or
#cmake --build ./cmake-build-debug -- -j8


