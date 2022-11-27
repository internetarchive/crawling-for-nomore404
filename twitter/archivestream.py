#!/usr/bin/env python
# 
#
"""
dependencies:
- TweetStream (https://github.com/joshmarshall/TweetStream)
- Tornado
"""
from __future__ import print_function, unicode_literals

import gevent.monkey
gevent.monkey.patch_all()

import sys
import os
import re

from configobj import ConfigObj
import logging
import argparse
import time
import json
from tweetarchiver.twitterstream import Stream as TwitterStream

from tweetarchiver.tweetstream import TweetStream
from tweetarchiver.archiver import Archiver

parser = argparse.ArgumentParser()
parser.add_argument(
    '-c', '--config', default='./twitter.conf',
    help='configuration file in INI format (default %(default)s)')

args = parser.parse_args()
if not os.path.isfile(args.config):
    print("configuration file %s does not exist" % (args.config,),
          file=sys.stderr)
    exit(1)

conf = ConfigObj(args.config)
as_config = conf.get('archivestream')
if as_config:
    destdir = as_config.get('destdir', '.')
    logfile = as_config.get('logfile', 'archivestream.log')
if not os.path.isdir(destdir):
    print('destdir %s does not exist' % destdir, file=sys.stderr)
    exit(1)


logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    filename=logfile, level=logging.INFO)

archiver = Archiver(destdir=destdir)
logging.info('starting up')

try:    
    tw_stream = TwitterStream()
    for tw in tw_stream.connect():
        archiver.archive_message(tw)
except KeyboardInterrupt as ex:
    pass
finally:
    archiver = None
    logging.info('terminating')
