#!/usr/bin/env python
#
"""
read WordPress post firehose, archive it, and schedule URLs for crawling.
"""
import gevent.monkey
gevent.monkey.patch_all()

import os
import sys
from optparse import OptionParser

import gevent
from gevent import socket
from gevent.event import Event
from gevent.queue import Queue, Empty

from six.moves.http_client import BadStatusLine
import six.moves.urllib.request as urllib2
from six.moves.urllib.error import URLError
import json
import logging
import time
from datetime import datetime
from gzip import GzipFile
from six.moves.configparser import ConfigParser

from wpfeed.firehose import FirehoseDownloader

"""
end point for WP-hosted blogs: 'http://xmpp.wordpress.com:8008/posts.json'
end point for self-hosted blogs: 'http://xmpp.wordpress.com:8008/posts.org.json'
"""

# TODO: these two classes were copied from Twitter archiver.
# move to reusable library (widecrawl?)
class ArchiveFile(object):
    def __init__(self, fn, complevel=2):
        self.fn = fn
        self.complevel = complevel
        self.f = open(self.fn + '.open', 'wb')
        self.ctime = datetime.now()
    def __del__(self):
        self.close()
    def close(self):
        if self.f:
            self.f.close()
            self.f = None
            try:
                os.rename(self.fn + '.open', self.fn)
            except Exception as ex:
                logging.warning('failed to rename %s.open to %s (%s)',
                                self.fn, self.fn, ex)
    def write_record(self, message):
        """message must be one whole streaming message."""
        if self.f is None:
            raise IOError("attempted to write into closed file %s" % self.fn)
        z = GzipFile(fileobj=self.f, mode='wb', compresslevel=self.complevel)
        z.write(message)
        z.write(b'\r\n')
        z.close()
        self.f.flush()

    def size(self):
        if self.f:
            return self.f.tell()
        else:
            return os.path.getsize(self.fn)

class Archiver(object):
    def __init__(self, destdir='.', prefix='wordpress-post-firehose',
                 rollsize=int(1e9), rolldate='%Y%m%d',
                 tsformat='%Y%m%d%H%M%S'):
        self.rollsize = rollsize
        self.rolldate = rolldate
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
        """
        :type message: bytes
        """
        if isinstance(message, dict):
            message = json.dumps(message).encode('utf-8')

        if self.arc and self.rolldate is not None:
            cdate = self.arc.ctime.strftime(self.rolldate)
            nowdate = datetime.now().strftime(self.rolldate)
            if cdate != nowdate:
                self.arc = None

        if self.arc is None:
            self.arc = ArchiveFile(self._makefn())

        self.arc.write_record(message)

        if self.rollsize is not None:
            if self.arc.size() > self.rollsize:
                self.arc = None

class HeadquarterSubmitter(object):
    def __init__(self, epbase, job):
        self.epbase = epbase
        self.job = job

    def put(self, curls):
        ep = '{}/{}/mdiscovered'.format(self.epbase, self.job)
        headers = { 'Content-Type': 'text/json' }
        data = json.dumps(curls).encode('utf-8')
        req = urllib2.Request(ep, data, headers)
        f = urllib2.urlopen(req, timeout=10)
        if f.code != 200:
            raise IOError('{}: {}'.format(ep, f.code))
        res = f.read()
        f.close()
        logging.info('submitted %d curls, response=%s',
                     len(curls), res.rstrip())

    def makedata(self, j):
        verb = j.get('verb')
        if verb != 'post':
            # we know firehose also includes events with verb="delete",
            # presumably for article deletion.
            return None
        post = j.get('object')
        if post is None:
            return None
        posturl = post.get('permalinkUrl')
        if posturl is None:
            return None
        return dict(u=posturl)

    def __repr__(self):
        return 'HeadquarterSubmitter({!r},{!r})'.format(self.epbase, self.job)

class Pipeline(object):
    """Persistent, bounded-memory-usage pipeline:
    - 'spills' items to disk if internal memory queue is filled-up.
    - restores queue contents from a journal file upon restart after crash.
    (planned, persistency is not implemented yet.)
    """
    def __init__(self, receiver, logdir, size):
        """logdir: must exist
        """
        self.receiver = receiver
        self.makedata = getattr(self.receiver, 'makedata', lambda o: o)
            
        self.logdir = logdir
        self.size = size
        self.queue = Queue(None)

        self.items = None
        self.seq = 0
    def put(self, item):
        # TODO: write to journal
        data = self.makedata(item)
        if data is not None:
            self.queue.put((self.seq, data))
    def _get_batch(self):
        # wait until at least one is available
        items = [self.queue.get()]
        # read more items if available
        try:
            while True:
                item = self.queue.get_nowait()
                items.append(item)
        except Empty:
            pass
        return items
            
    def run(self):
        try:
            while True:
                if self.items is None:
                    self.items = self._get_batch()
                if self.items:
                    try:
                        data = [o[1] for o in self.items]
                        self.receiver.put(data)
                        # TODO: write to journal
                        self.items = None
                    except Exception as ex:
                        logging.warning('%s.put() failed (%s), retrying...',
                                        self.receiver, ex, exc_info=1)
                        time.sleep(5*60)
        except Exception as ex:
            logging.error('Pipeline.run exiting by error', exc_info=1)

class StatSubmitter(object):
    def __init__(self, carbon, basename, stats):
        sp = carbon.split(':', 1)
        if len(sp) == 1: sp.append(2003)
        self.server = (sp[0], int(sp[1]))
        self.basename = basename
        self.stats = stats

    def run(self):
        while True:
            time.sleep(5*60.0)
            msg = []
            now = time.time()
            for stat, value in self.stats.items():
                msg.append('{}.{} {:.3f} {:.0f}\n'.format(
                    self.basename, stat, value, now))

            sock = socket.socket()
            try:
                sock.connect(self.server)
            except:
                logging.warning("couldn't connect to carbon server %s:%d",
                                *self.server)
                continue
            try:
                sock.sendall(''.join(msg).encode('ascii'))
                sock.close()
            except Exception as ex:
                logging.warning("error writing to carbon server (%s)", ex)

           
opt = OptionParser('%prog URL')
opt.add_option('--endpoint', dest='endpoint', default=None)
opt.add_option('-A', dest='auth', default=None)
opt.add_option('-d', dest='arcdir', default='arcs')
opt.add_option('--carbon', dest='carbon', default=None)
opt.add_option('-L', dest='logfile', default=None)
opt.add_option('--statbase', dest='statbase',
               default='crawl.wordpress.firehose')
opt.add_option('-p', '--preset', dest='preset', default=None)
opt.add_option('--prefix', dest='prefix', default='wordpress-post-firehose')
opt.add_option('-H', dest='hq',
               default='http://crawl-dev.us.archive.org/hq/jobs')
opt.add_option('-J', dest='hqjob', default='wordpress')
               
if os.path.isfile('config.ini'):
    config = ConfigParser()
    config.read('config.ini')
else:
    config = None
# load default values from config.ini, if exists
if config:
    opt.set_defaults(**dict(config.items('firehose')))

options, args = opt.parse_args()

# load preset option values from config
if options.preset:
    if config:
        if config.has_section(options.preset):
            options._update_careful(dict(config.items(options.preset)))
        else:
            opt.error("no preset {!r} is defined".format(options.preset))
    else:
        opt.error("--preset is specified, but there's no ./config.ini")

logopts = dict(level=logging.INFO,
               datefmt='%FT%T',
               format='%(asctime)s %(levelname)s %(message)s')
if options.logfile:
    logopts['filename'] = options.logfile
logging.basicConfig(**logopts)

if len(args) < 1:
    endpoint = options.endpoint
    if endpoint is None:
        opt.error('URL argument is required')
else:
    endpoint = args[0]

arcdir = options.arcdir
if not os.path.isdir(arcdir):
    opt.error('{}: no such directory'.format(arcdir))

archiver = Archiver(destdir=arcdir, prefix=options.prefix,
                    rollsize=int(1e9), rolldate='%Y%m%d')

pipelines = []
if options.hq and options.hqjob:
    pipelines.append(
        Pipeline(HeadquarterSubmitter(options.hq, options.hqjob), 'hq', 1000)
        )
for pl in pipelines:
    gevent.spawn(pl.run)

downloader = FirehoseDownloader(endpoint, archiver, pipelines, auth=options.auth)

g = gevent.spawn(downloader.run)
if options.carbon:
    statsubmitter = StatSubmitter(options.carbon, options.statbase,
                                  downloader.stats)
    gevent.spawn(statsubmitter.run)

try:
    g.join()
finally:
    archiver.close()

