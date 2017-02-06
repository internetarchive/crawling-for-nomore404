#!/usr/bin/env python
#
"""
Sends tweets to kafka topic.
"""
from __future__ import print_function, unicode_literals

import os
from configobj import ConfigObj
import logging
import argparse
import io
import time
from email.utils import formatdate

from tweetarchiver.tweetstream import TweetStream
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument(
    '-c', '--config', default='./twitter.conf',
    help='configuration file in INI format (default %(default)s)s')


class ConfigError(Exception):
    pass

def httpdate(ts):
    return formatdate(timeval=ts, localtime=False, usegmt=True)

try:
    args = parser.parse_args()
    if not os.path.isfile(args.config):
        raise ConfigError(
            "configuration file %s does not exist" % (args.config,),
            file=sys.stderr)

    conf = ConfigObj(args.config)

    ks_config = conf.get('kafkastream')
    if not ks_config:
        raise ConfigError(
            'configuration file must have non-empty kafkastream section')
    server = ks_config.get('server')
    if not server:
        raise ConfigError('kafkastream.server cofnig is required')
    topic = ks_config.get('topic')
    if not topic:
        raise ConfigError('kafkastream.topic config is required')
    logfile = ks_config.get('logfile', 'kafkastream.log')

    logging.basicConfig(
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        filename=logfile, level=logging.INFO)

    tw_config = conf.get('twitter')
    if not tw_config:
        raise ConfigError('configuration file must have [twitter] section')
except ConfigError as ex:
    print(ex.message, file=sys.stderr)
    exit(1)

common_headers = {
    'source': '/1.1/statuses/sample.json',
    }
common_header_bytes = b''.join(
    '{}: {}\r\n'.format(n, v).encode('utf-8')
    for n, v in common_headers.items()
    )

try:
    producer = KafkaProducer(bootstrap_servers=server)

    stream = TweetStream("/1.1/statuses/sample.json", tw_config)
    for tweet in stream:
        buf = io.BytesIO(common_header_bytes)
        buf.write('Date: {}\r\n'.format(httpdate(time.time())).encode('ascii'))
        buf.write(b'\r\n')
        buf.write(tweet)
        buf.write(b'\r\n')

        t0 = time.time()
        
        payload = buf.getvalue()
        producer.send(topic, payload)
        t = time.time() - t0
        logging.info('message %d bytes %.0fmus', len(payload), t * 1000000)

except KeyboardInterrupt as ex:
    pass
finally:
    logging.info('terminating')
