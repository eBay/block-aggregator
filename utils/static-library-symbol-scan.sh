#!/bin/bash

# Examples of static library symbol: "compress" or "uncompress" that gets provided from the compression
# related library, and we need to know in which library:
#   (1)The symbol is marked as "U", that is, the symbol needs to be reolved by linking with
#    another dependent library. Or, 
#   (2)The symbol is marked as "T", that is, the symbol is exported from the specified library,
#    and this library is able to provide the symbol for linking.
#
# We can use this simple tool to understand which library in all of the ClickHouse "contrib" libraries
# need to be added for linking in order to resolve the missing symbols.

# Command example:
# ./library-system-scan.sh $NUCOLUMNAR_PATH/NuColumnarAggr/deps_build/ClickHouse-v21.8.3.44-lts/cmake-build-debug "uncompress"

# Example results found:
# for contrib/zlib-ng/libzd.a, we have symbol export:
#    find chosen symbol: 0000000000000260 T uncompress
# 
if [ "$#" -ne 2 ]; then
  echo "Usage: <top_directory_hosting_libraries> <symbol_to_be_scanned>"
  exit -1
fi

top_directory=$1
library_symbol_target=$2

echo "chosen top directory: $top_directory to scan library symbol: $library_symbol_target"

#Get all of he library files into an array

function examine_library_symbol() {
   library=$1
   symbol=$2 
   symbol_found=$(nm -C $library| grep $2)
   echo "*find chosen symbol: $symbol_found"
}

IFS=$'\n'
library_files=($(find $top_directory -name "*.a"))
unset IFS

#printf "%s\n" "${library_files[@]}"

for library_file in ${library_files[@]}
do
   echo "obtained library file: $library_file, to exam symbol: $library_symbol_target"
   examine_library_symbol $library_file $library_symbol_target
done    
