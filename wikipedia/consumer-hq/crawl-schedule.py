#!/usr/bin/python
#
import sys
import os
import re
from urllib import quote
import itertools
import logging

# kafka.client calls logging.getLogger() at module level!!
logging.basicConfig(level=logging.DEBUG, filename='crawl-schedule.log',
                    format='%(asctime)s %(levelname)s %(message)s)',
                    datefmt='%FT%T')

import ujson

import threading
#import gevent
#from gevent import socket
#import gevent.monkey

#gevent.monkey.patch_socket()

import time
import urllib2
from kafka.client import KafkaClient
from kafka.zookeeper import ZSimpleConsumer
from kafka.consumer import SimpleConsumer
from kafka.common import KAFKA_THREAD_DRIVER
from crawllib.headquarter import HeadquarterSubmitter
from crawllib.graphite import StatSubmitter

# TODO put these consts in a config file
KAFKA_SERVER = ('crawl-db02.us.archive.org', 9092)
ZKHOSTS = ','.join('crawl-zk{:d}.us.archive.org'.format(n)
                   for n in range(1, 4))
HQ_BASE_URL = 'http://crawl-hq05.us.archive.org/hq/jobs'
HQ_JOB = 'wikipedia'

CARBON_SERVER = 'crawl-monitor.us.archive.org:2003'
STAT_BASE = 'crawl.wikipedia.links'

def useful_link(url):
    if not re.match(r'https?://', url):
        return False
    if re.match(r'https?://([^:./]\.)*archive.org/', url):
        return False
    return True

class CrawlScheduler(object):
    def __init__(self):
        if False:
            self.kafka = KafkaClient(*KAFKA_SERVER)
            self.consumer = SimpleConsumer(self.kafka, "crawl", "wiki-links",
                                           driver_type=KAFKA_THREAD_DRIVER,
                                           auto_commit=False)
        else:
            self.kafka = None
            self.consumer = ZSimpleConsumer(ZKHOSTS, "crawl", "wiki-links",
                                            driver_type=KAFKA_THREAD_DRIVER,
                                            manage_offsets=True,
                                            auto_commit=False)

        self.submitter = HeadquarterSubmitter(HQ_BASE_URL, HQ_JOB)

        self.stats = dict(fetched=0, scheduled=0, discarded=0)
    def shutdown(self):
        if self.kafka:
            self.kafka.close()

    def submit(self, curls):
        logging.info('submitting %d curls to HQ', len(curls))
        for n in itertools.count():
            try:
                self.submitter.put(curls)
                if n > 0:
                    logging.info('submission retry succeeded')
                break
            except Exception, ex:
                logging.warn('submission failed (%s), retrying after 30s',
                             ex)
                time.sleep(30.0)
        self.consumer.commit()
        self.stats['scheduled'] += len(curls)

    def pull_and_submit(self):
        empty_count = 0
        while 1:
            # iteration stops once queue becomes empty
            curls = []
            for mao in self.consumer:
                message = mao.message
                #print '{} {}'.format(mao.offset, message)
                o = ujson.loads(message.value.rstrip())
                links = o.get('links')
                language = o.get('language').encode('utf-8')
                article = o.get('article').encode('utf-8')

                if language and article:
                    via = 'http://{}.wikipedia.org/wiki/{}'.format(
                        language, quote(article, safe=''))
                    for link in links:
                        if useful_link(link):
                            curl = dict(u=link)
                            logging.debug('curl %s', curl)
                            curls.append(curl)
                            self.stats['fetched'] += 1
                            if len(curls) >= 100:
                                self.submit(curls)
                                curls = []
                        else:
                            self.stats['discarded'] += 1
            if len(curls) > 0:
                self.submit(curls)
                curls = []
                empty_count = 0
            empty_count += 1
            # report queue exhaustion only once.
            if empty_count == 1:
                logging.info('queue exhausted')
            time.sleep(1)

scheduler = CrawlScheduler()
statsubmitter = threading.Thread(
    target=StatSubmitter(CARBON_SERVER, STAT_BASE, scheduler.stats).run
    )
statsubmitter.setDaemon(True)
statsubmitter.start()

#g = gevent.spawn(pull_and_submit)
try:
    #g.join()
    scheduler.pull_and_submit()
finally:
    scheduler.shutdown()

