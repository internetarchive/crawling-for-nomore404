#!/usr/bin/python
# 
#
"""
dependencies:
- TweetStream (https://github.com/joshmarshall/TweetStream)
- Tornado
"""

import sys
import os
import re

from configobj import ConfigObj
from gzip import GzipFile
from datetime import datetime
import logging
from optparse import OptionParser
import json
import time

import tweetstream

from tornado.ioloop import IOLoop

BACKOFF_INCREMENT = 0.250
BACKOFF_MAX = 16

BACKOFF_RATELIMIT_INITIAL = 60
BACKOFF_RATELIMIT_FACTOR = 2

ratelimit_backoff = BACKOFF_RATELIMIT_INITIAL

conf = ConfigObj()
for confdir in ('.', os.path.dirname(__file__)):
    conffile = os.path.join(confdir, 'twitter.conf')
    if os.path.isfile(conffile):
        c = ConfigObj(conffile)
        conf.merge(c)
        break

authkeys = dict([(k, conf.get(k)) for k in ('consumer_key', 'consumer_secret',
                                            'access_token_key',
                                            'access_token_secret')])

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

    def on_error(self, ex):
        """error callback"""
        logging.error(ex.message)

class RawTweetStream(tweetstream.TweetStream):
    def __init__(self, *args, **kwargs):
        super(RawTweetStream, self).__init__(*args, **kwargs)
        self._connection_tries = 0
    def parse_json(self, response):
        if not response.strip():
            # Empty line, happens sometimes for keep alive
            return self.wait_for_message()
        if self._callback:
            self._callback(response)
        self.wait_for_message()
    def open_twitter_stream(self):
        super(RawTweetStream, self).open_twitter_stream()
        self._twitter_stream.set_close_callback(self.on_close)
    def on_connect(self):
        self._connection_tries = 0
        super(RawTweetStream, self).on_connect()
    def on_close(self):
        print >>sys.stderr, "socket closed"
        # ref. https://dev.twitter.com/docs/streaming-apis/connecting
        delay = min(self._connection_tries * 0.250, 16.0)
        time.sleep(delay)
        self._connection_tries += 1
        self.open_twitter_stream()

opt = OptionParser()
opt.add_option('-d', dest='destdir', default='.',
               help='directory to write archive file into (default %default)')
opt.add_option('-L', dest='logfile', default='archivestream.log',
               help='logging output (default %default)')

options, args = opt.parse_args()
if not os.path.isdir(options.destdir):
    opt.error('destdir %s does not exist' % options.destdir)
logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s',
                    filename=options.logfile, level=logging.INFO)

archiver = Archiver(destdir=options.destdir)
try:    
    ts_config = dict(
        twitter_consumer_secret=authkeys['consumer_secret'],
        twitter_consumer_key=authkeys['consumer_key'],
        twitter_access_token_secret=authkeys['access_token_secret'],
        twitter_access_token=authkeys['access_token_key']
        )

    stream = RawTweetStream(ts_config)
    stream.set_error_callback(archiver.on_error)
    stream.fetch("/1.1/statuses/sample.json",
                 callback=archiver.archive_message)

    IOLoop.instance().start()
finally:
    archiver = None
