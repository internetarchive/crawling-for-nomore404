#!/usr/bin/python
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
from gzip import GzipFile
from datetime import datetime
import logging
import argparse
try:
    import ujson as json
except ImportError:
    import json
import time

from tweetstream import TweetStream

class ArchiveFile(object):
    def __init__(self, fn, complevel=2):
        self.fn = fn
        self.complevel = complevel
        self.f = open(self.fn + '.open', 'wb')
    def __del__(self):
        self.close()
    def close(self):
        if self.f:
            self.f.close()
            self.f = None
            try:
                os.rename(self.fn + '.open', self.fn)
            except Exception, ex:
                logging.warn('failed to rename %s.open to %s (%s)',
                             self.fn, self.fn, ex)
    def write_record(self, message):
        """message must be one whole streaming message."""
        if self.f is None:
            raise IOError, "attempted to write into closed file %s" % self.fn
        z = GzipFile(fileobj=self.f, mode='wb', compresslevel=self.complevel)
        z.write(message)
        z.write('\r\n')
        z.close()
        self.f.flush()

    def size(self):
        if self.f:
            return self.f.tell()
        else:
            return os.path.getsize(self.fn)

class Archiver(object):
    def __init__(self, rollsize=int(1e9), prefix='twitter-stream-sample',
                 tsformat='%Y%m%d%H%M%S', destdir='.'):
        self.rollsize = rollsize
        self.prefix = prefix
        self.tsformat = tsformat
        self.destdir = destdir

        self.arc = None

    def __del__(self):
        self.close()

    def close(self):
        if self.arc: self.arc.close()
        self.arc = None

    def _makefn(self):
        t = datetime.now()
        fn = "%s-%s.gz" % (self.prefix, t.strftime(self.tsformat))
        return os.path.join(self.destdir, fn)

    def archive_message(self, message):
        if isinstance(message, dict):
            message = json.dumps(message)
        if self.arc is None:
            self.arc = ArchiveFile(self._makefn())

        self.arc.write_record(message)

        if self.arc.size() > self.rollsize:
            self.arc = None

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

ts_config = conf.get('twitter')
if ts_config is None:
    print("configuration file must have [twitter] section", file=sys.stderr)
    exit(1)

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    filename=logfile, level=logging.INFO)

archiver = Archiver(destdir=destdir)
logging.info('starting up')
try:    
    stream = TweetStream("/1.1/statuses/sample.json", ts_config)
    for tw in stream:
        archiver.archive_message(tw)
except KeyboardInterrupt as ex:
    pass
finally:
    archiver = None
    logging.info('terminating')
