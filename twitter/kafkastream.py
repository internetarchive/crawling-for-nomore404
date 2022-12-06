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
import json
# from tweetarchiver.twitterstream import Stream as TwitterStream

from tweetarchiver.tweetstream import connect_to_endpoint

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
    logfile = ks_config.get('logfile')

    logargs = dict(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
        )
    if logfile:
        logargs.update(filename=logfile)
    logging.basicConfig(**logargs)

    tw_config = conf.get('twitter')
    if not tw_config:
        raise ConfigError('configuration file must have [twitter] section')
except ConfigError as ex:
    print(ex.message, file=sys.stderr)
    exit(1)

common_headers = {
    'Source': 'https://api.twitter.com/2/tweets/sample/stream',
    }
common_header_bytes = b''.join(
    '{}: {}\r\n'.format(n, v).encode('utf-8')
    for n, v in common_headers.items()
    )

try:
    producer = KafkaProducer(bootstrap_servers=server)
    stream = connect_to_endpoint()
    while True:
        tweet = next(stream)
        if(tweet):
            buf = io.BytesIO()
            buf.write(common_header_bytes)
            buf.write('Date: {}\r\n'.format(httpdate(time.time())).encode('ascii'))
            buf.write(b'\r\n')
            buf.write(json.dumps(tweet).encode('utf-8'))
            buf.write(b'\r\n')

            payload = buf.getvalue()
            t0 = time.time()
            producer.send(topic, payload)
            t = time.time() - t0
            logging.debug('message %d bytes %.0fmus', len(payload), t * 1000000)
            
except KeyboardInterrupt as keyboardex:
    pass
except Exception as ex:
    logging.info(ex)
finally:
    logging.info('terminating')
