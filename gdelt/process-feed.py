#!/usr/bin/env python
#
from __future__ import print_function
import sys
import os
import re
import urllib2
import socket

import itertools
import logging
import time
from datetime import datetime
from calendar import timegm
import yaml

from argparse import ArgumentParser
from gdelt.feed import FeedReader, Deduper
from crawllib.headquarter import HeadquarterSubmitter

CONFIG_FILE = 'config.yaml'

def crawluri(urls):
    for url in urls:
        yield dict(u=url)

def batchup(iter, n):
    b = []
    for e in iter:
        b.append(e)
        if len(b) >= n:
            yield b
            b = []
    if b:
        yield b

def httpdate(dt):
    """format time tuple `dt` in HTTP Date format."""
    return time.strftime('%a, %d %b %Y %H:%M:%S %Z', dt)

class FeedScheduler(object):
    def __init__(self, feed_url, hqbase, hqjob,
                 datadir='data', timeout=20,
                 check_interval=-1):
        self.log = logging.getLogger(
            'gdelt.{0.__name__}'.format(FeedScheduler))
        self.feed_url = feed_url
        self.hqbase = hqbase
        self.hqjob = hqjob
        self.datadir = datadir
        self.timeout = int(timeout)
        self.check_interval = int(check_interval)

        assert os.path.isdir(self.datadir)

        self.deduper = Deduper(self.datadir)
        self.hqclient = HeadquarterSubmitter(self.hqbase, self.hqjob)

        rfiles = [fn for fn in os.listdir(self.datadir)
                  if re.match(r'feed-\d{14}$', fn)]
        if rfiles:
            self.log.debug('last=%s', max(rfiles))
            # time.strptime() returns time tuple without timezone. make it
            # UTC with timegm() and gmtime()
            self.last_time = time.gmtime(timegm(
                    time.strptime(max(rfiles)[-14:], '%Y%m%d%H%M%S')))
        else:
            self.last_time = None

    def process(self):
        while True:
            t = time.time()
            try:
                self.process1()
            except KeyboardInterrupt as ex:
                raise
            except Exception as ex:
                self.log.error('process1 failed', exc_info=1)
            if self.check_interval < 0:
                self.log.debug('exiting because check_interval < 0')
                break
            if test_mode:
                self.log.debug('exiting because test_mode=True')
                break
            dt = t + self.check_interval - time.time()
            if dt >= 1.0:
                self.log.debug('sleeping %ds until next cycle', int(dt))
                time.sleep(dt)

    def process1(self):
        # file name is in UTC. 
        rid = time.strftime('%Y%m%d%H%M%S', time.gmtime())
        rfile = os.path.join(self.datadir, 'feed-{}'.format(rid))
        try:
            req = urllib2.Request(self.feed_url)
            if self.last_time:
                self.log.debug('last_time=%s', httpdate(self.last_time))
                req.add_header('If-Modified-Since', httpdate(self.last_time))
            f = urllib2.urlopen(req, timeout=self.timeout)
            try:
                with open(rfile, 'wb') as w:
                    while True:
                        d = f.read(16*1024)
                        if not d: break
                        w.write(d)
                    self.log.info('downloaded %d bytes in %s', w.tell(),
                                  rfile)
            except KeyboardInterrupt as ex:
                if os.path.exists(rfile):
                    os.remove(rfile)
                raise
        except urllib2.HTTPError as ex:
            if ex.code == 304:
                # Not Modified
                self.log.debug('feed %s not modified since %s', self.feed_url,
                              httpdate(self.last_time))
                return
            self.log.warn('%s %s %s', self.feed_url, ex.code, ex.reason)
            return
        except (urllib2.URLError, socket.error) as ex:
            self.log.warn('%s %s', self.feed_url, ex)
            return

        self.last_time = time.gmtime()
        
        urlcount = 0
        slfile = os.path.join(self.datadir, 'sche-{}'.format(rid))
        with open(slfile, 'wb') as sl:
            with open(rfile, 'rb') as f:
                reader = FeedReader(f)
                for urls in batchup(crawluri(self.deduper.dedup(reader)), 500):
                    self.log.debug('submitting %s URLs...', len(urls))
                    if not test_mode:
                        self.hqclient.put(urls)
                    for curl in urls:
                        sl.write(curl['u'])
                        sl.write('\n')
                    urlcount += len(urls)
        self.log.info('submitted total %s URLs (see %s)',
                      urlcount, os.path.basename(slfile))

        self.deduper.step()

parser = ArgumentParser()
parser.add_argument('--test', action='store_true', default=False,
                    help='disables submission to HQ')
parser.add_argument('--config', default=CONFIG_FILE,
                    help='configuration file filename (default %(default)s)')

args = parser.parse_args()

test_mode = args.test
config_file = args.config

if os.path.isfile(config_file):
    config = yaml.load(open(config_file))
else:
    print("config file {} is not found".format(config_file), file=sys.stderr)
    exit(1)

logconf = config.get('logging')
if logconf:
    import logging.config
    logging.config.dictConfig(logconf)
else:
    logging.basicConfig(
        level=logging.INFO, filename='process.log',
        format='%(asctime)s %(levelname)s %(name)s %(message)s')

if 'gdelt' not in config:    
    print("configuration must have 'gdelt' section", file=sys.stderr)
    exit(1)

sch = FeedScheduler(**config['gdelt'])
try:
    sch.process()
except KeyboardInterrupt:
    pass

