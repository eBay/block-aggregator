#!/bin/bash

error() {
   local sourcefile=$1
   local lineno=$2
   echo abnormal exit at $sourcefile, line $lineno
}
uname -a | grep Darwin >> /dev/null && is_mac=1 || true

function setup () {
    deps_dir=`pwd`

    # Parse args
    JOBS=8

    #for debian
    if [ -f /etc/lsb-release ];then
        dist_name=`lsb_release -i -s`
    #for readhat
    elif [ -f /etc/redhat-release ]; then
        dist_name=`cat /etc/redhat-release|awk '{print $1}'`
    fi

    if [ x"${dist_name}" != "x" ];then
        echo "Linux distribution : ${dist_name}"
        is_linux=1
    fi


    # Parse args
    read -r -d '' USAGE << EOM
deps.sh [-j num_jobs] [-F]
    -j number of paralle jobs to use during the build (default: 8)
    -F destroy caches and rebuild all dependencies
EOM

    while getopts "xFj:p:" opt; do
        case $opt in
            p)
                deps_dir=$OPTARG;;
            F)
                FORCE=1;;
            x)
                set -x;;
            j)
                JOBS=$OPTARG;;
            *)
                echo "$USAGE"
                exit 1;;
            \?)
                echo "Invalid option: -$OPTARG" >&2
                exit 1;;
            :)
            echo "Option $OPTARG requires an argument." >&2
            exit 1
        esac
    done

    set -eE #enable bash debug mode

    trap 'error "${BASH_SOURCE}" "${LINENO}"' ERR

    deps_prefix=$deps_dir/deps_prefix
    deps_build=$deps_dir/deps_build

    if [ -n "$FORCE" ]; then
        rm -rf $deps_prefix
        rm -rf $deps_build
    fi


    mkdir -p $deps_prefix
    mkdir -p $deps_build
    cd $deps_build

} # end of setup

function library() {
    local name=$1 # - library name; becomes directory prefix
    local version=$2 # - library version or commit or tag; becomes directory suffix
    local url=$3 # - URL; must have the name and version already expanded
    local branch=${4:-master} #- parameter for git download method

    if [[ $url =~ ".git"$ ]] ; then
        if [ $version == "latest" ] ; then
            version=`git ls-remote $url | grep HEAD | awk '{print $1}'`
		fi
    fi

    dirname=$name-$version
    if [ ! -e $dirname -o ! -e $dirname/build_success ]; then
        [[ $url != *.git ]] && rm -rf $dirname
        echo "Fetching $dirname"

        case $url in
            *ebaycentral.qa.ebay.com*)
                mkdir -p $dirname
                wget --max-redirect=5 --no-check-certificate -O $dirname.tar.gz $url
                cd $dirname
                tar zxf ../$dirname.tar.gz;;
            *.tar.gz)
                if [ ! -f $dirname.tar.gz ]; then
                    wget --max-redirect=5 --no-check-certificate -O $dirname.tar.gz $url
                fi
                tar zxf $dirname.tar.gz
                test -d $dirname
                cd $dirname;;
             *.tar.bz2)
                if [ ! -f $dirname.tar.bz2 ]; then
                    wget --max-redirect=5 --no-check-certificate -O $dirname.tar.bz2 $url
                fi
                tar jxf $dirname.tar.bz2
                test -d $dirname
                cd $dirname;;
            *.h|*.hpp )
                mkdir $dirname
                wget --max-redirect=5  --no-check-certificate --directory-prefix=$dirname $url
                cd $dirname;;
            *.git)
                git clone -b $branch $url $dirname > /dev/null 2>&1 || true
                cd $dirname
                git cat-file -e $version^{commit} && git checkout $version || true;;
            *)
              echo Unable to derive download method from url $url
              exit 1;;
        esac

        $name # invoke the build function

        cd $deps_build
        touch $dirname/build_success
    fi

    #check that symlink exists and is pointing to current version
    if [ "$(readlink ./$name)" != "$dirname" ]; then
        rm ./$name > /dev/null 2>&1 || true
        ln -s $dirname $name
    fi
}


do-cmake() {
    echo "----------------- deps_build=--${deps_build}--   build_dir=--${build_dir} -- ---------"
    build_dir=${build_dir-'cmake-build'}
    cmake -H. -B${build_dir} -DCMAKE_INSTALL_PREFIX:PATH=$deps_prefix -DCMAKE_PREFIX_PATH=$deps_prefix \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release} \
        -DCMAKE_CXX_FLAGS="$CMAKE_CXX_FLAGS" \
        "$@"
    cmake --build ${build_dir} -- -j$JOBS
    make_target=${make_target-install}
    cmake --build ${build_dir} -- $make_target

}

#if we want to turn on full compilation logging for cmake command called from the following method.
#add option "-v" to the end of the cmake command.
#cmake --build ${build_dir} -- -j$JOBS -v 
do-cmake-no-prefix() {
    echo "----------------- deps_build=--${deps_build}--   build_dir=--${build_dir} -- ---------"
    build_dir=${build_dir-'cmake-build'}
    cmake -H. -B${build_dir} -DCMAKE_INSTALL_PREFIX:PATH=$deps_prefix \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-Release} \
        -DCMAKE_CXX_FLAGS="$CMAKE_CXX_FLAGS" \
        "$@"
    cmake --build ${build_dir} -- -j$JOBS
    make_target=${make_target-install}
    cmake --build ${build_dir} -- $make_target

}


do-configure () {
    if [[ "$is_mac" != "" ]]; then 
        sroot=${OSX_SYSROOT-$(xcrun --sdk macosx --show-sdk-path)}
        flags=-isysroot\ $sroot
    fi
    CFLAGS="$flags" CXXFLAGS="$flags" ./configure --prefix=$deps_prefix "$@"
}
do-make() {
    make -j$JOBS "$@"
    make install "$@"
}

