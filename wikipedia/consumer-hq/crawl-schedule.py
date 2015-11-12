#!/usr/bin/env python
#
import sys
import os
import re
from urllib import quote
import itertools
import logging
import socket

# kafka.client calls logging.getLogger() at module level!!
logging.basicConfig(level=logging.DEBUG, filename='crawl-schedule.log',
                    format='%(asctime)s %(levelname)s %(message)s)',
                    datefmt='%FT%T')

import ujson

import threading

import time
import urllib2
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from crawllib.headquarter import HeadquarterSubmitter
from crawllib.graphite import StatSubmitter

# TODO put these consts in a config file
KAFKA_SERVER = ','.join(['crawl-db02.us.archive.org:9092'])
ZKHOSTS = ','.join('crawl-zk{:d}.us.archive.org'.format(n)
                   for n in range(1, 4))
HQ_BASE_URL = 'http://crawl-hq06.us.archive.org/hq/jobs'
HQ_JOB = 'wikipedia'

CARBON_SERVER = 'crawl-monitor.us.archive.org:2003'
STAT_BASE = 'crawl.wikipedia.links'

KAFKA_CONSUMER_GROUP = "crawl"
KAFKA_TOPIC = "wiki-links"

def useful_link(url):
    if not re.match(r'https?://', url):
        return False
    if re.match(r'https?://([^:./]\.)*archive.org/', url):
        return False
    return True

class CrawlScheduler(object):
    def __init__(self):
        self.kafka = KafkaClient(hosts=KAFKA_SERVER)
        self.consumer = SimpleConsumer(
            self.kafka, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC,
            auto_commit=True)

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
            try:
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
            except socket.error as ex:
                # typically timeout
                logging.warn('error reading from Kafka (%s)', ex)
            time.sleep(1)

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config')
config_actions = [
    parser.add_argument('--kafka-server'),
    parser.add_argument('--zkhosts'),
    parser.add_argument('--hq-base-url'),
    parser.add_argument('--hq-job'),
    parser.add_argument('--carbon-server'),
    parser.add_argument('--stat-base'),
    parser.add_argument('--kafka-consumer-group'),
    parser.add_argument('--kafka-topic')
    ]

args = parser.parse_args()

if args.config:
    scope = dict()
    execfile(args.config, scope)
    for k, v in scope.items():
        if k and 'A' <= k[0] <= 'Z':
            globals()[k] = v
for a in config_actions:
    value = getattr(args, a.dest)
    # if option is not specified in command line, use default values
    # provided either by config file, or code above.
    if value is not None:
        globals()[a.dest.upper()] = value

scheduler = CrawlScheduler()
statsubmitter = threading.Thread(
    target=StatSubmitter(CARBON_SERVER, STAT_BASE, scheduler.stats).run
    )
statsubmitter.setDaemon(True)
statsubmitter.start()

try:
    scheduler.pull_and_submit()
except Exception as ex:
    logging.warn('scheduler exitting on error', exc_info=1)
    print >>sys.stderr, "threads:"
    for th in threading.enumerate():
        print "  {}".format(th)
finally:
    scheduler.shutdown()
