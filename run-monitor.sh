#!/usr/bin/env sh
WORKDIR=$1
cd $WORKDIR

node monitor.js | python producer.py
