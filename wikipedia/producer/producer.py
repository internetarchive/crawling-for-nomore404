#!/usr/bin/env python
from __future__ import print_function
import sys
import re
from kafka.client import KafkaClient
from kafka.errors import KafkaError
from kafka.producer import SimpleProducer
import argparse
import logging
import logging.config

parser = argparse.ArgumentParser()
parser.add_argument('--kafka', help='kafka server host:port',
                    default='crawl-db02.us.archive.org:9092')
parser.add_argument('--log', help='file to send logging output to')
parser.add_argument('--failures', help='file to write lines that could not '
                    'send to Kafka')

args = parser.parse_args()

if args.log:
    loghandler = {
        'class': 'logging.handlers.RotatingFileHandler',
        'formatter': 'default',
        'filename': args.log,
        'maxBytes': 50*1024*1024,
        'backupCount': 3
        }
else:
    loghandler = {
        'class': 'logging.StreamHandler',
        'formatter': 'default',
        'stream': 'ext://sys.stderr'
        }
logconfig = dict(
    version=1,
    formatters={'default': {
            'format': '%(asctime)s %(levelname)s %(message)s'
            }},
    handlers={'default': loghandler},
    root={
        'level': 'INFO',
        'handlers': ['default']
        }
    )

logging.config.dictConfig(logconfig)

failure_log = open(args.failures, "w") if args.failures else sys.stdout

TOPIC_IRC = "wiki-irc"
TOPIC_LINKS = "wiki-links"

kafka = KafkaClient(args.kafka)

# async -> async_send: https://github.com/dpkp/kafka-python/pull/1454
wikiIRCProducer = SimpleProducer(
    kafka, async_send=False,
    req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE)

wikiLinksProducer = wikiIRCProducer

#read in lines from wikipedia live monitor server
for line in sys.stdin:
    try:
        line.decode('utf-8')
    except UnicodeDecodeError:
        continue
    line = line.strip()
    if line:
        kafkaSuccess = False
        if line.startswith('Wiki-IRC-Message:'):
            message = line.replace("Wiki-IRC-Message:","",1);
            try:
                response = wikiIRCProducer.send_messages(TOPIC_IRC, message);
                if response and response[0].error == 0:
                    kafkaSuccess = True
            except KafkaError as ex:
                logging.warn("failed to send to %s (%r)",
                             TOPIC_IRC, ex)
        elif line.startswith('Wiki-Links-Results:'):
            message = line.replace("Wiki-Links-Results:","",1);
            try:
                response = wikiLinksProducer.send_messages(TOPIC_LINKS, message);
                if response and response[0].error == 0:
                    kafkaSuccess = True
            except KafkaError as ex:
                logging.warn("failed to send to %s (%r)",
                             TOPIC_LINKS, ex)
        else:
            logging.warn("ignoring line: %r", line)
            continue

        if not kafkaSuccess:
            # FLUSH to disk
            # log failed lines so we can reprocess such lines by feeding
            # this output to this script at a later time.
            print(line, file=failure_log)

kafka.close()
