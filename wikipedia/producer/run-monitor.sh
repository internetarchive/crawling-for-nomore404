#!/usr/bin/env bash
BIN=$(cd $(dirname $0); pwd)
WORKDIR=$BIN/..
export PATH=$BIN:$PATH
LOGDIR=$WORKDIR/logs
cd $WORKDIR
/home/ia/bin/logger -f $LOGDIR/retries.log -fe $LOGDIR/retries.err \
  bash -c "shim $BIN/monitor.js | producer.py"

