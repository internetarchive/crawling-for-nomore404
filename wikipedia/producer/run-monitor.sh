#!/usr/bin/env bash
BIN=$(cd $(dirname $0); pwd)
WORKDIR=$BIN/..
export PATH=$BIN:$PATH
LOGDIR=$WORKDIR/logs
cd $WORKDIR
shim $BIN/monitor.js | producer.py --log $LOGDIR/retries.log \
  --failures $LOGDIR/retries.err
