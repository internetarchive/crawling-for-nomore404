#!/usr/bin/env python
"""
Create WARC files from tweets stored in Apache Kafka in user defined dir.
Read configuration from YAML file. Run like this:
    tweetwarc.py -c tweetwarc.yaml -d /dir-to-save-warc
"""
from __future__ import unicode_literals, print_function
import sys
import argparse
import yaml
import hashlib
import uuid
import json
import logging
import logging.config
import os
from os import scandir, walk
import re
import traceback
import socket
import signal
import gzip
from datetime import datetime
import time
from hanzo.warctools import WarcRecord
from hanzo.warctools.warc import warc_datetime_str
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import CommitFailedError

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
        tweet = json.loads(tweet_json.replace("\'", "\""))
        url = "https://twitter.com/%s/status/%s" % (
            tweet['includes']['users'][0]['username'],
            tweet['data']['id']
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


def start_consumer():
    # access global vars: config, args
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

    partitions = consumer.partitions_for_topic(kafka_topic)
    logging.debug('partitions=%s', partitions)

    return consumer

def recover_offsets(args):
    warcdir = args.directory
    partition_offset = {}
    last_warc = ''
    for ent in scandir(warcdir):
        if not ent.is_file(): continue
        if ent.name.endswith('.warc.gz.open'):
            print('repair this file first: %s' % (ent.name,), file=sys.stderr)
            return 1
        if ent.name.endswith('.warc.gz'):
            if ent.name > last_warc:
                last_warc = ent.name
    if last_warc is '':
        print('no warc.gz files found in %s' % (warcdir,), file=sys.stderr)
        return 1
    logging.info('scanning %s for partition offsets', last_warc)
    with gzip.open(os.path.join(warcdir, last_warc), 'rb') as f:
        clen = None
        for l in f:
            if l == b'\r\n':
                if clen is not None:
                    f.read(clen)
                    clen = None
                continue
            l = l.rstrip()
            m = re.match(br'(?i)content-length:\s*(\d+)', l)
            if m:
                clen = int(m.group(1))
                continue
            m = re.match(br'(?i)archive-source:\s*'
                         br'(?P<topic>\w+)/(?P<partition>\d+)/(?P<offset>\d+)',
                         l)
            if m:
                o = int(m.group('offset'))
                p = m.group('partition')
                partition_offset[p] = max(o, partition_offset.get(p, 0))
                continue

    print('--seek', ','.join("%s:%d" % (p, partition_offset[p] + 1)
                             for p in sorted(partition_offset.keys())))
    return 0

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', default='./tweetwarc.yaml',
                    help='YAML configuration file (default %(default)s)')
parser.add_argument('-d', '--directory', default='.',
                    help='Directory to store tweets WARC.')
parser.add_argument('--group', default=None,
                    help='override consumer group ID for testing')
parser.add_argument('-v', action='count', dest='loglevel', default=0,
                    help='generate DEBUG level logs')
parser.add_argument('--recover-offsets', action='store_true', default=False)
parser.add_argument('--seek', default=None,
                    help='update consumer offset and exit (for failure recovery)')
parser.add_argument('--lock', default=None,
                    help='path of lock file that is created at startup and '
                    'removed at clean shutdown. If this file exists, tweetwarc '
                    'refuses to start')

args = parser.parse_args()

with open(args.config) as f:
    config = yaml.safe_load(f)

# logging level control: -v will log DEBUG for all but kafka.*
# -vv will log DEBUG including kafka.coordinator.*
# -vvv will log DEBUG including kafka.*
logging.basicConfig(
    level=logging.DEBUG if args.loglevel > 0 else logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s %(message)s',
    datefmt='%F %T'
)

if 'logging' in config:
    logging.config.dictConfig(config['logging'])

if args.loglevel < 3:
    logging.getLogger('kafka').setLevel(logging.INFO)
if args.loglevel == 2:
    logging.getLogger('kafka.coordinator').setLevel(logging.INFO)

if args.recover_offsets:
    exit(recover_offsets(args))

consumer = start_consumer()
if args.seek:
    # this is a rudimentary recovery aid for advancing consumer offset.
    # currently this app experiences unexpected revokation of assigned
    # partition and subsequent failure to commit consumer offsets. When
    # this happens, stop the app and advance offset with this.
    seeks = []
    parts = args.seek.split(',')
    for part in parts:
        target_partition, _, target_offset = part.strip().partition(':')
        if not target_offset:
            parser.error('--seek expects PARTITION:OFFSET[,PARTITION:OFFSET...]')
        partition = TopicPartition(config.get('kafka_topic'),
                                          int(target_partition))
        offset = int(target_offset)
        seeks.append((partition, offset))

    msgs = consumer.poll(max_records=1)
    logging.info('msgs=%s', msgs)
    while True:
        a = consumer.assignment()
        if a:
            missing_partitions = [tp for tp, o in seeks if tp not in a]
            if missing_partitions:
                logging.error('partitions %s are missing in the assignment. '
                              'it may succeed if you try again,',
                              missing_partitions)
                consumer.close()
                exit(1)
            if not all(tp in a for tp, o in seeks):
                parser.error('assignment {} is insufficient'.format(a))
            break
        logging.info('assignment=%s', a)
        time.sleep(1.0)

    for target_partition, target_offset in seeks:
        offset = consumer.committed(target_partition)
        # this must be identical to the committed offset.
        position = consumer.position(target_partition)

        logging.info('partition %s: current committed offset=%s position=%s',
                     target_partition.partition, offset, position)
        if target_offset == position:
            logging.info('no need to change position')
            continue

        logging.info('seeking to %s', target_offset)
        consumer.seek(target_partition, target_offset)
        logging.info('new position=%s', consumer.position(target_partition))

    consumer.commit()
    consumer.close()
    exit(0)

time_limit = config.get('warc_time_limit')
size_limit = config.get('warc_size_limit')

interrupted = False
def interrupt(sig, stack):
    logging.info('received signal %s', sig)
    global interrupted
    interrupted = True

signal.signal(signal.SIGINT, interrupt)
signal.signal(signal.SIGTERM, interrupt)

if args.lock:
    if os.path.isfile(args.lock):
        logging.error(
            "lock file %s exists. it means previous run was not shut "
            "down cleanly. you may need to repair warc files and adjust "
            "consumer offset before restarting. remove this file when ready",
            args.lock)
        # exit with code=2 so that supervisor won't retry launching.
        exit(2)
    with open(args.lock, "w") as f:
        f.write("{}\n".format(os.getpid()))

# coordinator may die because of communication breakdown. it can result in
# duplicate archive if offset has not been committed. we record offsets that
# has been successfully processed
# each partition to avoid
# avoid
archived_offsets = {}
warc = None
try:
    while not interrupted:
        if consumer is None:
            consumer = start_consumer()
        try:
            for msg in consumer:
                # KafkaConsumer may have re-joined consumer group because of
                # communication issue. Such event is invisible to client code
                # and very likely results in duplicate archive. So we compare
                # offset in the message against the last archived, and skip
                # duplicates. It's kinda sad client has to do this for itself.
                if msg.partition in archived_offsets:
                    last_offset = archived_offsets[msg.partition]
                    if last_offset >= msg.offset:
                        logging.info('detected duplicate msg %s:%s, seeking to %s',
                                     msg.partition, msg.offset, last_offset + 1)
                        consumer.seek(TopicPartition(msg.topic, msg.partition),
                                      last_offset + 1)
                        continue

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
                        archived_offsets[msg.partition] = msg.offset
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
                else:
                    logging.info('discarded %s/%s', msg.partition, msg.offset)

                # app termination is checked only after each tweet is archived
                # to ensure all tweets are archived.
                if interrupted:
                    break

                if warc and warc.should_rollover(size_limit, time_limit):
                    warc.close()
                    warc = None
                    consumer.commit()

        except CommitFailedError as ex:
            logging.error('explicit commit failed. restarting consumer')
            consumer = None
            continue

        except AssertionError as ex:
            # kafka-python uses "assert" for checking assumptions that
            # fail sometimes
            if re.match(r'Broker id \S+ not in current metadata', ex.message):
                traceback.print_exc()
                logging.error('restarting consumer')
                consumer = None
                continue

            raise

    logging.info('exiting by interrupt')

    exitcode = 0
except Exception as ex:
    logging.error('exiting by error', exc_info=1)
    # 2 indicates serious error needing operator attention.
    # by default supervisor won't restart if program exist with code 2.
    exitcode = 2
finally:
    if warc is not None:
        warc.close()
        warc = None

    logging.info('archived_offsets is %s', archived_offsets)

    if consumer is not None:
        consumer.close()

    if args.lock:
        try:
            os.unlink(args.lock)
        except OSError as ex:
            logging.warning("failed to delete %s: %s", args.lock, ex)

exit(exitcode)
