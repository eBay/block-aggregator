#!/bin/bash

sudo apt-get update
sudo apt-get install -yq software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test

sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.6 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.6.list

sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update

GCC_MAJOR_VER=10
# Some extra dependencies for Ubuntu 14.04 and 16.04
sudo apt-get install -yq \
  g++-${GCC_MAJOR_VER} \
  pkg-config \
  libssl-dev \
  libjemalloc-dev \
  libtool \
  autoconf \
  wget \
  autopoint \
  gettext \
  libpcre3-dev \
  golang \
  mongodb-org \
  automake \
  gcovr \
  lcov  \
  libdouble-conversion-dev \
  libiberty-dev \
  liblz4-dev \
  liblzma-dev \
  zlib1g-dev \
  libmysqlclient-dev \
  openjdk-8-jdk \
  ninja-build \
  toxiproxy \
  toxiproxy-cli


sudo apt-get remove -yq \
  libgflags2v5 \
  libboost-all-dev \
  libc-ares-dev \
  cmake

pushd /tmp
curl -sL https://deb.nodesource.com/setup_12.x | sudo -E bash -
# curl -sL https://deb.nodesource.com/setup_8.x -o nodesource_setup.sh
# sudo bash nodesource_setup.sh
popd

sudo apt-get install nodejs

GCC_MAJOR_VER=10

sudo update-alternatives --install /usr/bin/cpp cpp /usr/bin/cpp-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/gcc-ar gcc-ar /usr/bin/gcc-ar-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/gcc-nm gcc-nm /usr/bin/gcc-nm-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/gcc-ranlib gcc-ranlib /usr/bin/gcc-ranlib-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0 && \
sudo update-alternatives --install /usr/bin/gcov gcov /usr/bin/gcov-${GCC_MAJOR_VER} ${GCC_MAJOR_VER}0

set -e

# install boost
BOOST_MAJOR=1
BOOST_MINOR=75
BOOST_PATCH=0
BOOST_RELEASE_TAG=${BOOST_MAJOR}_${BOOST_MINOR}_${BOOST_PATCH}
BOOST_VERSION_NUMBER=$(printf "%d%03d%02d" $BOOST_MAJOR $BOOST_MINOR $BOOST_PATCH)
if [[ -f /usr/local/include/boost/version.hpp && $(cat /usr/local/include/boost/version.hpp | grep "#define BOOST_VERSION " | cut -d' ' -f3) == $BOOST_VERSION_NUMBER ]]
then
    echo found boost version $BOOST_RELEASE_TAG
else
    echo installing boost version $BOOST_RELEASE_TAG ...
    sudo rm -rf /usr/local/include/boost
    sudo rm -f /usr/local/lib/libboost_*
    sudo rm -rf /usr/local/lib/cmake/Boost*
    sudo rm -rf /usr/local/lib/cmake/boost*

    pushd /tmp
    wget -O /tmp/boost_${BOOST_RELEASE_TAG}.tar.gz https://boostorg.jfrog.io/artifactory/main/release/${BOOST_MAJOR}.${BOOST_MINOR}.${BOOST_PATCH}/source/boost_${BOOST_RELEASE_TAG}.tar.gz
    tar -xzf boost_${BOOST_RELEASE_TAG}.tar.gz
    cd boost_${BOOST_RELEASE_TAG}
    ./bootstrap.sh --with-libraries=system,filesystem,iostreams,thread,stacktrace,date_time,regex,serialization,chrono,program_options,context,coroutine
    sudo ./b2 cxxflags="-std=c++17" visibility=global install
    popd
    sudo rm -rf /tmp/boost_${BOOST_RELEASE_TAG}*
fi

# install cmake
cmake_version=3.16.2
if [[ -x /usr/local/bin/cmake && $(/usr/local/bin/cmake --version | grep "cmake version " | cut -d' ' -f3) == $cmake_version ]]
then
    echo cmake $cmake_version is found
else
    echo installing cmake $cmake_version ...
    pushd /tmp
    wget -O CMake-$cmake_version.tar.gz https://github.com/Kitware/CMake/archive/v${cmake_version}.tar.gz
    tar -xzvf CMake-$cmake_version.tar.gz
    cd CMake-$cmake_version/
    ./bootstrap
    make -j4
    sudo make install

    popd
    rm -rf /tmp/CMake*
fi
