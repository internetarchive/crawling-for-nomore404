#!/usr/bin/env python
"""
Create WARC files from tweets stored in Apache Kafka in user defined dir.
Read configuration from YAML file. Run like this:
    tweetwarc.py -c tweetwarc.yaml -d /dir-to-save-warc
"""
from __future__ import unicode_literals
import argparse
import yaml
import hashlib
import uuid
import json
import logging
import os
import socket
import signal
from datetime import datetime
import time
from hanzo.warctools import WarcRecord
from hanzo.warctools.warc import warc_datetime_str
from kafka import KafkaConsumer, TopicPartition

def warc_uuid(text):
    """Utility method for WARC header field urn:uuid"""
    return ("<urn:uuid:%s>" %
            uuid.UUID(hashlib.sha1(text).hexdigest()[0:32])).encode('ascii')


def warcinfo_record(warc_filename):
    """Return warcinfo WarcRecord.
    Required to write in the beginning of a WARC file.
    """
    warc_date = warc_datetime_str(datetime.utcnow())
    metadata = "\r\n".join((
        "format: WARC File Format 1.0",
        "conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf"
    ))
    return WarcRecord(
        headers=[
            (WarcRecord.TYPE, WarcRecord.WARCINFO),
            (WarcRecord.CONTENT_TYPE, b'application/warc-fields'),
            (WarcRecord.ID, warc_uuid(metadata+warc_date)),
            (WarcRecord.DATE, warc_date),
            (WarcRecord.FILENAME, warc_filename)
        ],
        content=(b'application/warc-fields', metadata + "\r\n"),
        version=b"WARC/1.0"
    )


def tweet_warc_record(tweet_json):
    """Parse Tweet JSON and return WarcRecord.
    """
    try:
        tweet = json.loads(tweet_json)
        # skip deleted tweet
        if 'user' not in tweet:
            return
        url = "https://twitter.com/%s/status/%s" % (
            tweet['user']['screen_name'],
            tweet['id']
        )
    except Exception as ex:
        logging.error('error in tweet_warc_record', exc_info=1)
        return None

    warc_date = warc_datetime_str(datetime.utcfromtimestamp(
        float(tweet['timestamp_ms'])/1000.0))
    return WarcRecord(
        headers=[
            (WarcRecord.TYPE, WarcRecord.RESOURCE),
            (WarcRecord.CONTENT_TYPE, b'application/json'),
            (WarcRecord.ID, warc_uuid(url+warc_date)),
            (WarcRecord.URL, url),
            (WarcRecord.DATE, warc_date)
        ],
        content=(b'application/json', tweet_json + "\r\n"),
        version=b"WARC/1.0"
    )

class WarcFile(object):
    def __init__(self, directory):
        self.target_filename = self.warc_filename(directory)
        logging.info("Archiving to file %s", self.target_filename)
        # filename recorded in warcinfo record - without .open, no path.
        self.base_filename = os.path.basename(self.target_filename)[:-5]

        logging.debug('opening %s', self.target_filename)
        self.f = open(self.target_filename, "ab")
        record = warcinfo_record(self.base_filename)
        try:
            self.write_record(record)
        except IOError:
            os.remove(self.target_filename)
            raise
        except KeyboardInterrupt:
            os.remove(self.target_filename)
            raise

        self.start_time = time.time()

    def warc_filename(self, directory):
        """WARC filename example: /tmp/tweets-20170307100027-0001-fqdn.warc.gz.open
        After the file is closed, remove the .open suffix
        The filename format is compatible with draintasker
        WARC_naming:   1 # {TLA}-{timestamp}-{serial}-{fqdn}.warc.gz
        """
        return "%s/tweets-%s-0001-%s.warc.gz.open" % (
            directory, datetime.utcnow().strftime('%Y%m%d%H%M%S'),
            socket.getfqdn())

    def write_record(self, record):
        record.write_to(self.f, gzip=True)
        self.last_offset = self.f.tell()

    def rollback(self):
        """truncate file to last_offset."""
        os.ftruncate(self.last_offset)

    def should_rollover(self, size_limit, time_limit):
        size = os.fstat(self.f.fileno()).st_size
        return size > size_limit or (time.time() - self.start_time) > time_limit

    def close(self):
        self.f.close()
        logging.info('renaming %s to %s', self.target_filename,
                     self.target_filename[:-5])
        os.rename(self.target_filename, self.target_filename[:-5])


parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', default='./tweetwarc.yaml',
                    help='YAML configuration file (default %(default)s)s')
parser.add_argument('-d', '--directory', default='.',
                    help='Directory to store tweets WARC.')
parser.add_argument('--group', default=None,
                    help='override consumer group ID for testing')
parser.add_argument('-v', action='count', dest='loglevel', default=0,
                    help='generate DEBUG level logs')
parser.add_argument('--seek', default=None,
                    help='update consumer offset and exit (for failure recovery)')
args = parser.parse_args()

with open(args.config) as f:
    config = yaml.load(f)

# logging level control: -v will log DEBUG for all but kafka.*
# -vv will log DEBUG including kafka.*
logging.basicConfig(
    level=logging.DEBUG if args.loglevel > 0 else logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s %(message)s',
    datefmt='%F %T'
)
if args.loglevel < 2:
    logging.getLogger('kafka').setLevel(logging.INFO)

consumer = KafkaConsumer(
    bootstrap_servers=config.get('kafka_bootstrap_servers'),
    client_id=config.get('kafka_client_id'),
    group_id=args.group or config.get('kafka_group_id'),
    # use small number not to exceed session_timeout.
    max_poll_records=5,
    session_timeout_ms=120*1000,
    auto_offset_reset='earliest',
    enable_auto_commit=False if args.seek else True
)
kafka_topic = config.get('kafka_topic')
consumer.subscribe([kafka_topic])

if args.seek:
    # this is a rudimentary recovery aid for advancing consumer offset.
    # currently this app experiences unexpected revokation of assigned
    # partition and subsequent failure to commit consumer offsets. When
    # this happens, stop the app and advance offset with this.
    target_partition, _, target_offset = args.seek.partition(':')
    if not target_offset:
        parser.error('--seek expects PARTITION:OFFSET')
    target_partition = TopicPartition(kafka_topic, int(target_partition))
    target_offset = int(target_offset)

    msgs = consumer.poll(max_records=1)
    logging.info('msgs=%s', msgs)
    while True:
        a = consumer.assignment()
        if a:
            assert target_partition in a
            break
        logging.info('assignment=%s', a)
        time.sleep(1.0)
    offset = consumer.committed(target_partition)
    # this must be identical to the committed offset.
    position = consumer.position(target_partition)

    logging.info('current committed offset=%s position=%s', offset, position)
    if target_offset == position:
        logging.info('no need to change position')
        exit(0)

    logging.info('seeking to %s', target_offset)
    consumer.seek(target_partition, target_offset)
    logging.info('new position=%s', consumer.position(target_partition))
    consumer.commit()
    exit(0)

time_limit = config.get('warc_time_limit')
size_limit = config.get('warc_size_limit')

interrupted = False
def interrupt(sig, stack):
    global interrupted
    interrupted = True

signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, interrupt)

warc = None
try:
    while not interrupted:
        partitions = consumer.partitions_for_topic(kafka_topic)
        logging.debug('partitions=%s', partitions)
        for msg in consumer:
            logging.debug('msg: partition=%s offset=%s', msg.partition,
                          msg.offset)
            tweet = msg.value.decode('utf-8').split('\n')[-2]
            record = tweet_warc_record(tweet)
            if record:
                # used for tracking
                record.headers.append((
                    'Archive-Source',
                    '{0.topic}/{0.partition}/{0.offset}'.format(msg)
                ))
                if warc is None:
                    try:
                        warc = WarcFile(args.directory)
                    except IOError as ex:
                        logging.error('failed to create an WARC flle (%s)', ex)
                        break

                try:
                    warc.write_record(record)
                except IOError as ex:
                    logging.error('failed to archive tweet (%s), rolling back',
                                  ex)
                    warc.rollback()
                    # re-read the same msg again
                    consumer.seek(TopicPartition(msg.topic, msg.partition),
                                  msg.offset)
                    consumer.commit()

                    logging.info('pausing 5 seconds before continueing')
                    time.sleep(5.0)
                    break

            # app termination is checked only after each tweet is archived
            # to ensure all tweets are archived.
            if interrupted:
                break

            if warc and warc.should_rollover(size_limit, time_limit):
                warc.close()
                warc = None
                consumer.commit()
    logging.info('exiting by interrupt')
    exitcode = 0
except Excpetion as ex:
    logging.error('exiting by error', exc_info=1)
    # 2 indicates serious error needing operator attention.
    # by default supervisor won't restart if program exist with code 2.
    exitcode = 2
finally:
    if warc is not None:
        warc.close()
        warc = None

    consumer.close()

exit(exitcode)
