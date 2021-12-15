#!/bin/sh

set -e

CRASH_DUMP_DIR=$1
LAST_LOG_PATH=$2
CWORKDIR=$3
EXE="$CRASH_DUMP_DIR/minidump_stackwalk"
SYMBOLS="$CRASH_DUMP_DIR/symbols"

## init symbols and version
if [ ! -f "$CRASH_DUMP_DIR/version" ]; then
  cp -r /nucolumnar/bin/symbols $CRASH_DUMP_DIR
  cp /nucolumnar/bin/minidump_stackwalk $CRASH_DUMP_DIR/minidump_stackwalk
  cp /nucolumnar/bin/version $CRASH_DUMP_DIR/version
fi

for file in $(ls -rt $CRASH_DUMP_DIR/*.dmp || true)
do
  datetime=$(stat -c %y $file | awk -F'[ \.]' '{ print $1"T"$2}')
  # crash logfile for writing human-readable crash dump
  logfile="$CRASH_DUMP_DIR/dump_$datetime.log"

  # create duplicate txtfile for crash alerts
  txtfile="$CRASH_DUMP_DIR/dump_$datetime.txt"

  # write version into logfile
  version=`cat "$CRASH_DUMP_DIR/version"`
  echo "NuColumnarAggr $version" > $logfile

  # execute minidump_stackwalk
  $EXE -m $file $SYMBOLS 2>/dev/null >> $logfile

  # attach the last exit logs to crash file
  if [ -f "$LAST_LOG_PATH" ]; then
    echo "" >> $logfile
    echo "################################ExitLogs################################" >> $logfile
    tail -n 200 $LAST_LOG_PATH >> $logfile
  fi

  # creating a copy of this file for alerts
  # agent sees this file, reads the contents and deletes the file
  cp $logfile $txtfile
  
  # delete the crashdump file
  rm -f $file
done

for file in $(ls -rt $CWORKDIR/core.* || true)
do
  mv $file $CWORKDIR/last.core
done

## remove old symbols and copy new symbols to crash_dumps
rm -rf $CRASH_DUMP_DIR/symbols
cp -r /nucolumnar/bin/symbols $CRASH_DUMP_DIR
cp -f /nucolumnar/bin/minidump_stackwalk $CRASH_DUMP_DIR/minidump_stackwalk
# replace the version file with the current version
cp -f /nucolumnar/bin/version $CRASH_DUMP_DIR/version
