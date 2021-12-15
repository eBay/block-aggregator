#!/bin/bash

start_dir=`pwd`
trap "cd $start_dir" EXIT

# Must execute from the directory containing this script
cd "$(dirname "$0")"
project_dir=`pwd`

source deps-versions.sh
source deps-base.sh
setup $@

echo "deps_prefix=$deps_prefix"
#-------------------------------------------------

gflags() {
    do-cmake
}
library gflags ${LIBGFLAGS_VERSION} https://github.com/gflags/gflags/archive/v${LIBGFLAGS_VERSION}.tar.gz

protobuf() {
    cd cmake
    do-cmake -DCMAKE_CXX_STANDARD=17 -DWITH_PROTOC=ON -Dprotobuf_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -Dprotobuf_MODULE_COMPATIBLE=ON
    #./autogen.sh
    ##CXXFLAGS=-std=c++11 DIST_LANG=cpp
    #do-configure
    #do-make
}
library protobuf ${PROTOBUF_RELEASE} https://github.com/google/protobuf/releases/download/v${PROTOBUF_RELEASE}/protobuf-cpp-${PROTOBUF_RELEASE}.tar.gz

farmhash () {
    aclocal
    automake --add-missing
    do-configure CXXFLAGS="$CMAKE_CXX_FLAGS -g -mavx -maes -O3"
    do-make
}
library farmhash $FARMHASH_COMMIT https://github.com/google/farmhash.git

export PATH=$deps_prefix/bin:$PATH
export LD_LIBRARY_PATH=${deps_prefix}/lib:$LD_LIBRARY_PATH

flatbuffers() {
    echo "Applying change patches"
    # This is an important patch that restires embedded_t type compatibility which became impossible in v 1.12 whre they are calling _o.release() in the return of the generated Unpack  method.
    # type embedded_t is chosen by use to replace sthe default pointer type td::unique_ptr in order to make config factor object flat and improve performance significantly.
    git apply -p1 $project_dir/patches/flatbuffers/idl_gen_cpp.patch || true

    patch -p1 -f < $project_dir/patches/flatbuffers/idl.patch || true
    patch -p0 -f < $project_dir/patches/flatbuffers/CMakeList.patch || true

    if [ "$is_mac" == "1" ]; then
        do-cmake -DCMAKE_CXX_FLAGS="-std=c++17 -Wno-c++17-extensions" -DGRPC_VERSION=$GRPC_RELEASE -DCMAKE_BUILD_TYPE=Debug
    else
        do-cmake -DCMAKE_CXX_FLAGS="-std=c++17 -Wno-class-memaccess -Wno-stringop-overflow" -DGRPC_VERSION=$GRPC_RELEASE
    fi
}
library flatbuffers v$FLATBUFFERS_RELEASE https://github.com/google/flatbuffers.git v$FLATBUFFERS_RELEASE
# library flatbuffers $FLATBUFFERS_RELEASE https://github.com/google/flatbuffers/archive/v$FLATBUFFERS_RELEASE.tar.gz

librdkafka () {
### disable-sasl: SASL_CYRUS
    flags=-v do-configure --disable-sasl --disable-zstd
    do-make
}
#library librdkafka $LIBRDKAFKA_RELEASE https://github.com/edenhill/librdkafka/archive/v$LIBRDKAFKA_RELEASE.tar.gz
library librdkafka ${LIBRDKAFKA_TAG} git@github.corp.ebay.com:NuData/librdkafka-1.5.2.git nucolumnar-patch

glog () {
    echo "Applying change patches"
    patch -p1 -f < $project_dir/patches/glog/custom_log.patch
    do-cmake
}
library glog ${GLOG_RELEASE} https://github.com/google/glog/archive/v${GLOG_RELEASE}.tar.gz

googletest-release() {
  do-cmake
}
library googletest-release ${GTEST_RELEASE} https://github.com/google/googletest/archive/release-${GTEST_RELEASE}.tar.gz

prometheus-cpp () {
    git submodule update --init
    git apply --whitespace=warn $project_dir/patches/prometheus-cpp/prometheus-cpp.patch
    do-cmake \
            -DProtobuf_LIBRARY=$deps_prefix/lib/libprotobuf.a \
            -DProtobuf_PROTOC_EXECUTABLE=$deps_prefix/bin/protoc
}
library prometheus-cpp ${PROMETHEUS_CPP_CLIENT_TAG} https://github.com/jupp0r/prometheus-cpp.git ${PROMETHEUS_CPP_CLIENT_TAG}

userspace-rcu () {
    ./bootstrap
    CFLAGS="-m64 -g -O2" do-configure
    #CFLAGS="-m64 -g -O2" do-configure --enable-rcu-debug
    do-make
}
library userspace-rcu ${URCU_RELEASE_TAG} https://github.com/urcu/userspace-rcu/archive/v${URCU_RELEASE_TAG}.tar.gz

nlohmann_json () {
  mkdir -p $deps_prefix/include/nlohmann
  cp -a json.hpp $deps_prefix/include/nlohmann
}
library nlohmann_json ${NLOHMANN_JSON_VERSION} https://github.com/nlohmann/json/releases/download/v${NLOHMANN_JSON_VERSION}/json.hpp

cpp-jwt () {
    patch -p0 -f < $project_dir/patches/cpp-jwt/CMakeList.txt.patch
    patch -p1 -f < $project_dir/patches/cpp-jwt/jwt.hpp.patch
    patch -p1 -f < $project_dir/patches/cpp-jwt/jwt.ipp.patch
    rm include/jwt/json/json.hpp

    pushd ${deps_build}/protobuf/third_party/googletest
    if [[ ! -f ${deps_build}/protobuf/third_party/googletest/googletest/lib/.libs/libgtest.a ]]; then
        if [ "$is_mac" == "1" ]; then
            CXXFLAGS=-std=c++11 ./configure
        else
            ./configure
        fi
        make
    fi
    popd
    CPLUS_INCLUDE_PATH="${deps_prefix}/include:${deps_build}/protobuf/third_party/googletest/googletest/include" \
        do-cmake -DOPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include -DOPENSSL_LIBRARIES=/usr/local/opt/openssl/lib -DCMAKE_EXE_LINKER_FLAGS=-Wl,-v,-L${deps_build}/protobuf/third_party/googletest/googletest/lib/.libs,-rpath,${deps_build}/protobuf/third_party/googletest/googletest/lib/.libs,-L/usr/local/opt/openssl/lib -DCMAKE_CXX_FLAGS=-pthread
    # cp -a include/jwt ${deps_prefix}/include
}
library cpp-jwt ${CPPJWT_RELEASE} https://github.com/arun11299/cpp-jwt/archive/v${CPPJWT_RELEASE}.tar.gz


breakpad() {
    if [ -n "$is_linux" ]; then
        git clone https://chromium.googlesource.com/linux-syscall-support src/third_party/lss
        LIBS=-l:libstdc++.a LDFLAGS=-static-libgcc do-configure
    else
        do-configure --disable-tools
    fi

    git apply -p1 $project_dir/patches/breakpad/memory_allocator.h.patch || true
    patch -p1 -f < $project_dir/patches/breakpad/my_strchr.patch

    do-make
    if [ -n "$is_mac" ]; then
        rm $deps_prefix/include/breakpad_src || true
        pushd src
        ln -s `pwd` $deps_prefix/include/breakpad_src

        cd client
        client_dir=`pwd`
        cd mac
        cp $project_dir/patches/breakpad/Breakpad.xib sender
        xcodebuild -toolchain XcodeDefault -project Breakpad.xcodeproj -target Breakpad -configuration Release
        mkdir -p $deps_dir/Frameworks
        rm $deps_dir/Frameworks/Breakpad.framework || true
        ln -s $client_dir/mac/build/Release/Breakpad.framework $deps_dir/Frameworks
        install_name_tool -id $deps_dir/Frameworks/Breakpad.framework/Versions/A/Breakpad $deps_dir/Frameworks/Breakpad.framework/Versions/A/Breakpad
        install_name_tool -change @executable_path/../Frameworks/Breakpad.framework/Resources/breakpadUtilities.dylib  $deps_dir/Frameworks/Breakpad.framework/Resources/breakpadUtilities.dylib $deps_dir/Frameworks/Breakpad.framework/Versions/A/Breakpad
        popd

        pushd src/tools/mac/dump_syms
        xcodebuild -toolchain XcodeDefault -project dump_syms.xcodeproj -target dump_syms -configuration Release
        cp -a build/Release/dump_syms $deps_prefix/bin
        popd
    fi
}
library breakpad ${BREAKPAD_COMMIT} https://chromium.googlesource.com/breakpad/breakpad.git

tclap () {
    do-configure
    do-make
}
library tclap $TCLAP_VERSION https://downloads.sourceforge.net/project/tclap/tclap-${TCLAP_VERSION}.tar.gz

jemalloc() {
    ./autogen.sh --prefix=$deps_prefix --with-version=${JEMALLOC_RELEASE}-0-g0000000000000000000000000000000000000000
    make install_bin install_include install_lib
}
library jemalloc ${JEMALLOC_RELEASE} https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_RELEASE}/jemalloc-${JEMALLOC_RELEASE}.tar.bz2

ClickHouse_ () {
    build_type=$1
    patching=$2
    git submodule update --init --recursive
    cmake_params="-DCMAKE_BUILD_TYPE=$build_type $(xargs <<EOF
        -DWERROR=FALSE
	    -DCMAKE_VERBOSE_MAKEFILE=OFF
        -DENABLE_TESTS=OFF
        -DENABLE_GRPC=OFF
        -DENABLE_MYSQL=ON
        -DUSE_MYSQL=ON
        -DOPENSSL_USE_STATIC_LIBS=TRUE
        -DUSE_INTERNAL_MYSQL_LIBRARY=OFF
        -DUSE_INTERNAL_BOOST_LIBRARY=OFF
        -DBoost_USE_STATIC_LIBS=ON
        -DUSE_INTERNAL_SSL_LIBRARY=OFF
        -DUSE_INTERNAL_PROTOBUF_LIBRARY=OFF
        -DENABLE_BROTLI=OFF
        -DENABLE_CAPNP=OFF
        -DENABLE_HDFS=OFF
        -DENABLE_EMBEDDED_COMPILER=OFF
        -DENABLE_ODBC=OFF
        -DENABLE_CLICKHOUSE_ODBC_BRIDGE=OFF
        -DENABLE_PARQUET=ON
        -DENABLE_AVRO=ON
        -DENABLE_POCO_MONGODB=OFF
        -DENABLE_POCO_REDIS=OFF
        -DENABLE_POCO_ODBC=OFF
        -DENABLE_POCO_SQL=OFF
        -DENABLE_DATA_SQLITE=OFF
        -DENABLE_DATA_MYSQL=ON
        -DENABLE_DATA_POSTGRESQL=OFF
        -DENABLE_MONGODB=OFF
        -DENABLE_CASSANDRA=ON
        -DUSE_AMQPCPP=OFF
        -DENABLE_ICU=OFF
        -DENABLE_PROTOBUF=ON
        -DENABLE_RDKAFKA=OFF
        -DENABLE_S3=OFF
        -DENABLE_JEMALLOC=OFF
        -DENABLE_RAPIDJSON=OFF
	    -DCMAKE_EXE_LINKER_FLAGS=-v
        -Dprotobuf_MODULE_COMPATIBLE=ON
        -DCMAKE_IGNORE_PATH=$deps_prefix/lib/cmake/flatbuffer
EOF
)"

    if [ -n "$patching" ]; then
      git apply -p1 $project_dir/patches/clickhouse/avro.cmake.patch || [[ $? == 1 ]]
      git apply -p1 $project_dir/patches/clickhouse/avro.zigzag.patch || true
      git apply -p1 $project_dir/patches/clickhouse/amqp.cmake.patch || true
      git apply -p1 $project_dir/patches/clickhouse/nosig.patch || true
      git apply -p1 $project_dir/patches/clickhouse/pool.patch || true
      git apply -p1 $project_dir/patches/clickhouse/zookeeper.patch || true
    fi
    if [ -n "$is_linux" ]; then
        git apply -p1 $project_dir/patches/clickhouse/poco.timer.h.patch || true
        git apply -p1 $project_dir/patches/clickhouse/poco.TCPServerDispatcher.h.patch || true

        cmake_params="$cmake_params -DUSE_LIBCXX=OFF -DENABLE_FASTOPS=ON -DCMAKE_EXE_LINKER_FLAGS=-Wl,-v,-L/usr/local/lib"
    else
        extra_params=$(xargs <<EOF
           -DCMAKE_CXX_FLAGS=-I/usr/local/opt/openssl/include
           -DENABLE_FASTOPS=OFF
EOF
)
        #  -DSDK_PATH=$HOME/work/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
        export VERBOSE=y

        TOOLCHAIN=${TOOLCHAIN-XcodeDefault}
        if [[ $(sw_vers -productVersion) == 10.15.* ]]; then
            echo "Applying clickhouse cmake patches"
            git apply -p1 $project_dir/patches/clickhouse/cxx.cmake.patch || true
            git apply -p1 $project_dir/patches/clickhouse/mysqlclient.cmake.patch || true

            TOOLCHAINS_DIR=${TOOLCHAINS_DIR-/Applications/Xcode.app/Contents/Developer/Toolchains}
            cmake_params="$cmake_params -DUSE_LIBCXX=ON -DUSE_INTERNAL_LIBCXX_LIBRARY=ON -DCMAKE_CXX_FLAGS=-DSTD_EXCEPTION_HAS_STACK_TRACE -DLIBCXX_ENABLE_FILESYSTEM=OFF -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl -DCMAKE_EXE_LINKER_FLAGS=-Wl,-v,-U,_inside_main,-L/usr/local/lib"
        else
            cmake_params="$cmake_params -DUSE_LIBCXX=ON -DUSE_INTERNAL_LIBCXX_LIBRARY=ON"
            TOOLCHAINS_DIR=${TOOLCHAINS_DIR-$HOME/work/Xcode.app/Contents/Developer/Toolchains}
        fi

        tc=$TOOLCHAINS_DIR/${TOOLCHAIN}.xctoolchain
        test -d $tc && echo "using toolchain $tc to build ClickHouse" || (echo "unable to locate xcode toolchain ${tc} to build ClickHouse" && false)

        cmake_params="$cmake_params $extra_params -DCMAKE_CXX_COMPILER=${tc}/usr/bin/clang++ -DCMAKE_C_COMPILER=${tc}/usr/bin/clang -DLINKER_NAME=${tc}/usr/bin/ld"

    fi
    build_dir=$( [[ $build_type == 'Debug' ]] && echo -n cmake-build-debug || echo -n cmake-build) Protobuf_DIR=$deps_prefix do-cmake-no-prefix $cmake_params
}

#If we need to compile only the release mode for image generation and for unit testing, we will need to have
#    ClickHouse_ 'Release' true 
#in order to apply the patches needed
ClickHouse () {
    ClickHouse_ 'Debug' true
    ClickHouse_ 'Release'
}

library ClickHouse ${CLICKHOUSE_COMMIT} https://github.com/ClickHouse/ClickHouse.git
fswatch () {
    if [ -n "$is_linux" ]; then
        make_target=libfswatch
        ./autogen.sh
        do-configure --with-pic
        do-make
    else
        #  consider using -DSDK_PATH=/Applications/Xcode.app/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk
        TOOLCHAIN=${TOOLCHAIN-XcodeDefault}
        TOOLCHAINS_DIR=${TOOLCHAINS_DIR-/Applications/Xcode.app/Contents/Developer/Toolchains}
        tc=$TOOLCHAINS_DIR/${TOOLCHAIN}.xctoolchain
        test -d $tc && echo "using toolchain $tc to build fswatch" || (echo "unable to locate xcode toolchain ${tc} to build fswatch" && false)
        ./autogen.sh
        CC=${tc}/usr/bin/clang CXX=${tc}/usr/bin/clang++ do-configure
        # make_target=libfswatch
        do-make
    fi
}
library fswatch ${FSWATCH_RELEASE} https://github.com/emcrisostomo/fswatch.git

cd $deps_prefix/lib

rm -rf libgcc_s.so.1 || "Linking libgcc_s.so.1"

if [ -n "$is_linux" -a ! -e "libgcc_s.so.1" ]; then
    ln -s `gcc --print-file-name libgcc_s.so.1`
fi

