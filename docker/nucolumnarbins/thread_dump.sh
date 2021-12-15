#!/bin/sh

set -e

CRASH_DUMP_DIR=$1
THREAD_DUMP_DIR=$2
EXE="$CRASH_DUMP_DIR/minidump_stackwalk"
SYMBOLS="$CRASH_DUMP_DIR/symbols"

for file in $(ls -rt $THREAD_DUMP_DIR/*.dmp || true)
do
  nfile=$file".processing"
  mv $file $nfile

  cat "/nucolumnar/bin/version"
  # execute minidump_stackwalk
  $EXE -m $nfile $SYMBOLS 2>/dev/null
  # delete the crashdump file
  rm -f $nfile
  # Process one file only
  break
done