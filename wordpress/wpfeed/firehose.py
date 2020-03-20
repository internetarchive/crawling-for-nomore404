from __future__ import print_function

from six.moves.http_client import BadStatusLine
import six.moves.urllib.request as urllib2
from six.moves.urllib.error import URLError

import time
import socket
import base64
import json

import logging

class NullArchiver(object):
    """Do nothing archiver for testing.
    """
    def archive_message(self, line):
        pass

class PrintPipeline(object):
    """Pipline for testing.
    """
    def put(self, item):
        print(item)

def printsafe(s):
    if s is None: return None
    if not isinstance(s, bytes):
        s = s.encode('utf-8')
    return repr(s)[1:-1]

class FirehoseDownloader(object):
    RETRY_BACKOFF_FACTOR = 1.5
    INITIAL_RETRY_INTERVAL = 10
    MAX_RETRY_INTERVAL = 120 * 60

    def __init__(self, endpoint, archiver, pipelines, auth=None):
        self.endpoint = endpoint
        self.archiver = archiver or NullArchiver()
        self.pipelines = pipelines
        self.auth = auth

        self.retry_interval = self.INITIAL_RETRY_INTERVAL

        # stats
        self.stats = {
            'connection.success': 0,
            'connection.failure': 0,
            'downloaded': 0
            }

    def run(self):
        while True:
            # we cannot use HTTPBasicAuthHandler because server does not
            # request authentication.
            req = urllib2.Request(self.endpoint)
            if self.auth:
                auth = b'Basic ' + base64.b64encode(self.auth.encode('ascii'))
                req.add_header('Authorization', auth)
            opener = urllib2.build_opener(
                )
            try:
                f = opener.open(req)
                logging.info('firehose stream %s opened', self.endpoint)
                self.stats['connection.success'] += 1
                self.retry_interval = self.INITIAL_RETRY_INTERVAL
            except (URLError, BadStatusLine, socket.error) as ex:
                self.stats['connection.failure'] += 1
                logging.warning('failed to open firehose stream (%s), '
                                'holding off %d seconds',
                                ex, self.retry_interval)
                time.sleep(self.retry_interval)
                self.retry_interval = min(
                    self.retry_interval * self.RETRY_BACKOFF_FACTOR,
                    self.MAX_RETRY_INTERVAL
                    )
                logging.info('retrying connection')
                continue

            for line in f:
                if line == b'\n': continue
                self.stats['downloaded'] += 1
                self.archiver.archive_message(line)

                try:
                    j = json.loads(line.rstrip().decode('utf-8'))
                except ValueError as ex:
                    logging.warning('JSON decode failed: %r', line)
                    continue

                # TODO: make this one of pipelines?
                verb = j.get('verb')
                published = j.get('published')
                blog = j.get('target')
                blogurl = blog and blog.get('url')
                post = j.get('object')
                posturl = post and post.get('permalinkUrl')

                print("{} {} {} {}".format(
                    published, verb,
                    printsafe(blogurl), printsafe(posturl)
                ))

                for pl in self.pipelines:
                    pl.put(j)

            logging.warning('firehose stream closed')
            self.archiver.close()
