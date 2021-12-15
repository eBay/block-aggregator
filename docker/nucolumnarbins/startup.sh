#!/bin/sh

echo "[$(date)] Starting NuColumnarAggr"
echo "[$(date)] Logging to /log/"

if [ ${BACKUP_ENABLED:-false} = "true" ]; then
    echo "Starting with backup enabled"
    echo "ls /dev/:"
    ls /dev
    echo "====="
    echo "ls /dev/zvol:"
    ls /dev/zvol
    echo "====="
    echo "ls /dev/zvol/${BACKUP_POOL_NAME}:"
    ls /dev/zvol/${BACKUP_POOL_NAME}
    echo "====="
    mkdir -p /nucolumnar/data
    echo "Execute command: mount -o logbsize=256k -o largeio -o logbufs=8 -o discard -o nouuid -o noatime /dev/zvol/${BACKUP_POOL_NAME}/data /nucolumnar/data/"
    mount -o logbsize=256k -o largeio -o logbufs=8 -o discard -o nouuid -o noatime /dev/zvol/${BACKUP_POOL_NAME}/data /nucolumnar/data/
    [ $? -eq 0 ] || exit -1
    df -h
fi

USER="$(whoami)"
echo "Running as user ${USER}"
if [ ${USER} != "root" ]; then
  echo "Locking down..."
  sudo /nucolumnar/bin/lockdown.sh --user ${USER}
  echo "Lock down finished"
fi

_term() {
	echo "Caught SIGTERM signal!"
	kill -TERM "$child" 2>/dev/null
}

trap _term SIGTERM SIGINT

# Set default log dir
if [ -z "$GLOG_log_dir" ]; then
  GLOG_log_dir=/log
fi
export GLOG_log_dir

# Backup the last exit log
if [ -f "/log/NuColumnarAggr.INFO" ]; then
  echo "[$(date)] Backup /log/NuColumnarAggr.INFO.lastexit"
  cp -f $GLOG_log_dir/NuColumnarAggr.INFO  $GLOG_log_dir/NuColumnarAggr.INFO.lastexit
fi

if [ -f "/log/NuColumnarAggr.ERROR" ]; then
  echo "[$(date)] Backup /log/NuColumnarAggr.ERROR.lastexit"
  cp -f $GLOG_log_dir/NuColumnarAggr.ERROR $GLOG_log_dir/NuColumnarAggr.ERROR.lastexit
fi

#To check and create flags for NuColumnarAggr that can persist over NuColumnar Aggr instances
if [ -z "$COMMAND_FLAGS_DIR" ]; then
  COMMAND_FLAGS_DIR=/nucolumnar/data/nucolumnaraggrflags
fi
export COMMAND_FLAGS_DIR
mkdir -p $COMMAND_FLAGS_DIR || echo 'Failed to create $COMMAND_FLAGS_DIR for command flags'


#May have large core dump, so change workdir to data disk
CWORKDIR=/nucolumnar/data/nucolumnaraggrworkdir
mkdir -p $CWORKDIR || echo 'Failed to create $CWORKDIR'
cd $CWORKDIR

if [ -z "$CRASH_DUMPS_DIR" ]; then
  CRASH_DUMPS_DIR=$GLOG_log_dir/crash-dumps
fi
export CRASH_DUMPS_DIR
mkdir -p $CRASH_DUMPS_DIR

# Launch the crash dump processing script in the background
/nucolumnar/bin/crash_dump.sh $CRASH_DUMPS_DIR $GLOG_log_dir/NuColumnarAggr.INFO.lastexit $CWORKDIR &

echo "[$(date)] Started" >> ${GLOG_log_dir}/nucolumnaraggr.console
/nucolumnar/bin/NuColumnarAggr "$@" >> ${GLOG_log_dir}/nucolumnaraggr.console 2>&1 &

child=$!
echo "[$(date)] Started as pid $child at workdir $CWORKDIR"
wait "$child"
ecode=$?
echo "[$(date)] Exited as code ${ecode}" >> ${GLOG_log_dir}/nucolumnaraggr.console
exit $ecode