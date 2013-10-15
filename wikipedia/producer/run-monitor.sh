#!/usr/bin/env bash
WORKDIR=$1
cd $WORKDIR
node monitor.js | python producer.py
