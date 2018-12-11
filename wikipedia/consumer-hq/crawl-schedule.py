#!/usr/bin/env python
#
from __future__ import print_function, unicode_literals
import sys
import os
import re
from urllib import quote
import itertools
import logging
import socket

import ujson

import threading

import time
import urllib2

from servicelink.router import EventRouter

import yaml

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from crawllib.headquarter import HeadquarterSubmitter
from crawllib.graphite import StatSubmitter

def useful_link(url):
    if not re.match(r'https?://', url):
        return False
    if re.match(r'https?://([^:./]\.)*archive.org/', url):
        return False
    return True

class CrawlScheduler(object):
    def __init__(self, config):
        self._log = logging.getLogger('{0.__name__}'.format(CrawlScheduler))

        self.submitter = HeadquarterSubmitter(
            config['output']['hq']['base_url'],
            config['output']['hq']['job']
        )
        self.eventrouter = EventRouter(config['input'], self)

        self.stats = dict(fetched=0, scheduled=0, discarded=0)

        self.curls = []

    def shutdown(self):
        pass

    def submit(self, curls):
        self._log.info('submitting %d curls to HQ', len(curls))
        for n in itertools.count():
            try:
                self.submitter.put(curls)
                if n > 0:
                    self._log.info('submission retry succeeded')
                break
            except Exception, ex:
                self._log.warn('submission failed (%s), retrying after 30s',
                             ex)
                time.sleep(30.0)
        #self.consumer.commit()
        self.stats['scheduled'] += len(curls)

    def handle_message(self, msg):
        try:
            o = msg.get_json()
        except ValueError as ex:
            self._log.error('failed to parse message as JSON: %s', msg)
            return
        links = o.get('links')
        try:
            language = o['language'].encode('utf-8')
            article = o['article'].encode('utf-8')
            via = 'http://{}.wikipedia.org/wiki/{}'.format(
                language, quote(article, safe=b''))
        except (KeyError, AttributeError):
            self._log.warning('cannot build via - language and/or article'
                              ' information is missing: %r', o)
            via = None

        for link in links:
            if useful_link(link):
                curl = dict(u=link)
                if via:
                    curl['v'] = via
                self._log.debug('curl %s', curl)

                self.curls.append(curl)

                self.stats['fetched'] += 1

                if len(self.curls) >= 100:
                    self.submit(self.curls)
                    self.curls = []
            else:
                self.stats['discarded'] += 1

    def pull_and_submit(self):
        self.eventrouter.run()

config = {
    'input': {
        'kafka': {
            'topic': 'wiki-links',
            'params': {
                'bootstrap_servers:': 'crawl-db07.us.archive.org:9092',
                'client_id': 'wikipedia-hq',
                'group_id': 'crawl'
            }
        }
    },
    'output': {
        'hq': {
            'base_url': 'http://crawl-hq06.us.archive.org/hq/jobs',
            'job': 'wikipedia'
        }
    },
    'stat': {
        'carbon_server': 'crawl-monitor.us.archive.org:2003',
        'base': 'crawl.wikipedia.links'
    },
}

import argparse

def string_list(v):
    return v.split(',')

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config')
parser.add_argument('--kafka-server', type=string_list)
parser.add_argument('--hq-base-url')
parser.add_argument('--hq-job')
parser.add_argument('--carbon-server')
parser.add_argument('--stat-base')
parser.add_argument('--kafka-consumer-group')
parser.add_argument('--kafka-topic')

# temporary migration measure
parser.add_argument('--kafka-offset', type=int)

option_config_map = {
    'kafka_server': 'input.kafka.params.bootstrap_servers',
    'kafka_consumer_group': 'input.kafka.params.group_id',
    'kafka_topic': 'input.kafka.topic',
    'hq_base_url': 'output.hq.base_url',
    'hq_job': 'otput.hq.job',
    'carbon_server': 'stat.carbon_server',
    'stat_base': 'stat.base'
}

args = parser.parse_args()

# kafka.client calls logging.getLogger() at module level!!
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s)',
                    datefmt='%FT%T')

if args.config:
    logging.info('loading config %s', args.config)
    config.update(yaml.load(open(args.config, "r")))
    assert isinstance(config, dict)

def set_config(d, name, value):
    citems = name.split('.')
    for item in citems[:-1]:
        nd = d.get(item)
        if not isinstance(nd, dict):
            nd = d[item] = dict()
        d = nd
    d[citems[-1]]= value

# command line options overrides specific values in config
for o, c in option_config_map.items():
    value = getattr(args, o)
    if value is not None:
        set_config(config, c, value)

if args.kafka_offset is not None:
    topic = config['input']['kafka']['topic']
    set_config(config, 'input.kafka.consumer_offset.{}:0'.format(topic),
               args.kafka_offset)

scheduler = CrawlScheduler(config)

stat_config = config['stat']
StatSubmitter(
    stat_config['carbon_server'],
    stat_config['base'],
    scheduler.stats
).start()

logging.info('starting')
try:
    scheduler.pull_and_submit()
except KeyboardInterrupt:
    logging.info('scheduler exiting INT signal')
except Exception as ex:
    logging.warn('scheduler exitting on error', exc_info=1)
    print("threads:", file=sys.stderr)
    for th in threading.enumerate():
        print("  {}".format(th), file=sys.stderr)
finally:
    scheduler.shutdown()
