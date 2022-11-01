#!/usr/bin/python
#
"""
Twitter Stream API client inspired by 
- https://github.com/joshmarshall/TweetStream
- https://github.com/ultrabug/geventweet

Features:
- work with gevent
- supports OAuth
- retries upon connection failure transparently.
"""
import sys
import time
import httplib
import oauth2
import urlparse
from cStringIO import StringIO

import logging

class MissingConfiguration(Exception):
    """Raised when a configuration value is not found."""
    pass

class TweetStream(object):
    """ Twitter stream connection """

    HEADERS = {
        'User-Agent': 'TweetStream',
        'Accept': '*/*'
        }

    # ref https://dev.twitter.com/docs/streaming-apis/connecting
    
    # when connection fails, increase backoff interval by BACKOFF_INCREMENT
    # starting with 0, up to MAXIMUM BACKOFF_MAX seconds.
    BACKOFF_INCREMENT = 0.250
    BACKOFF_MAX = 16

    BACKOFF_RATELIMIT_INITIAL = 60
    BACKOFF_RATELIMIT_FACTOR = 2

    def __init__(self, path, configuration):
        """ Just set up the cache list and get first set """
        self._log = logging.getLogger(__name__)

        # prepopulating cache
        self._callback = None
        self._error_callback = None
        self._configuration = configuration
        consumer_key = self._get_configuration_key("consumer_key")
        consumer_secret = self._get_configuration_key(
            "consumer_secret")
        self._consumer = oauth2.Consumer(
            key=consumer_key, secret=consumer_secret)
        access_token = self._get_configuration_key("access_token_key")
        access_secret = self._get_configuration_key(
            "access_token_secret")
        self._token = oauth2.Token(key=access_token, secret=access_secret)
        self._twitter_stream_host = self._get_configuration_key(
            "twitter_stream_host", "stream.twitter.com")
        self._twitter_stream_scheme = self._get_configuration_key(
            "twitter_stream_scheme", "https")
        self._twitter_stream_port = self._get_configuration_key(
            "twitter_stream_port", 443)
        self._timeout = self._get_configuration_key(
            "twitter_stream_timeout", 1800)

        parts = urlparse.urlparse(path)
        self._path = parts.path
        self._full_path = self._path
        self._parameters = {}
        if parts.query:
            self._full_path = "%s?%s".format(self._full_path, parts.query)

        # throwing away empty or extra query arguments
        self._parameters = dict([
            (key, value[0]) for key, value in
            urlparse.parse_qs(parts.query).iteritems()
            if value
        ])

        self._response = None
        self._connection_tries = 0

    def _get_configuration_key(self, key, default=None):
        """
        Retrieve a configuration option, raising an exception if no
        default is provided.

        """
        configured_value = self._configuration.get(key, default)
        if configured_value is None:
            raise MissingConfiguration("Missing configuration item: %s" % key)
        return configured_value

    def set_error_callback(self, error_callback):
        """Pretty self explanatory."""
        self._error_callback = error_callback

    def __iter__(self):
        return self

    def _readline(self):
        buf = self._rbuf
        buf.seek(0, 2) # seek to the end
        buffers = []
        if buf.tell() > 0:
            buf.seek(0)
            bline = buf.readline()
            if bline.endswith('\n'):
                self._rbuf = StringIO()
                self._rbuf.write(buf.read())
                return bline
            buffers.append(bline)
        self._rbuf = StringIO()
        while True:
            try:
                data = self._response.read(1024)
            except Exception as ex:
                raise
            if not data:
                break
            nl = data.find('\n')
            if nl >= 0:
                nl += 1
                buffers.append(data[:nl])
                self._rbuf.write(data[nl:])
                del data
                break
            buffers.append(data)
        return ''.join(buffers)
        
    def next(self):
        """Return next tweet coming in. Blocks until tweet arrives.
        Tweet is returned as raw bytes (not including EOL separating
        tweets - not parsed JSON.
        """
        while 1:
            if self._response is None:
                if self._connection_tries > 0:
                    backoff = min(self._connection_tries * self.BACKOFF_INCREMENT,
                                  self.BACKOFF_MAX)
                    time.sleep(backoff)
                self._connection_tries += 1
                self._open_twitter_stream()
                continue
            self._connection_tries = 0

            try:
                tweet = self._readline()
                if tweet == '':
                    self._log.info('stream closed by the server')
                    self._response = None
                    continue
                
            except IOError as ex:
                self._log.info('failed to read line: %s', ex)
                self._response = None
                continue
            except httplib.IncompleteRead as ex:
                self._log.info('failed to read line: %s', ex)
                self._response = None
                continue
            return tweet.rstrip()
            
    def _open_twitter_stream(self):
        """ Creates the client and watches stream """
        if self._twitter_stream_scheme == 'https':
            http = httplib.HTTPSConnection(self._twitter_stream_host,
                                           self._twitter_stream_port,
                                           timeout=self._timeout)
        else:
            http = httplib.HTTPConnection(self._twitter_stream_host,
                                          self._twitter_stream_port,
                                          timeout=self._timeout)
        parameters = dict(self._parameters,
                          oauth_token=self._token.key,
                          oauth_consumer_key=self._consumer.key,
                          oauth_version='1.0',
                          oauth_nonce=oauth2.generate_nonce(),
                          oauth_timestamp=int(time.time())
                          )
        request = oauth2.Request(
            method="GET",
            url="{}://{}{}".format(self._twitter_stream_scheme,
                                   self._twitter_stream_host,
                                   self._path),
            parameters=parameters)
        signature_method = oauth2.SignatureMethod_HMAC_SHA1()

        request.sign_request(signature_method, self._consumer, self._token)
        headers = request.to_header()
        headers.update(self.HEADERS)

        self._log.info('requesting GET %s', self._path)
        http.request('GET', self._path, headers=headers)

        self._response = http.getresponse()
        self._log.info('got response %s', self._response.status)
        if self._response.status == 401:
            self._response.close()
            raise Exception('{} {}'.format(self._response.status,
                                           self._response.reason))
        if self._response.status != 200:
            self._response.close()
            self._response = None

        self._rbuf = StringIO()

if __name__ == '__main__':
    import argparse
    import configobj
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='twitter.conf')
    args = parser.parse_args()
    config = configobj.ConfigObj(args.config)
    if 'twitter' not in config:
        raise Exception('twitter section not found in twitter.conf')
    stream = TweetStream("/2/tweets/sample/stream", config.get('twitter'))

    for tw in stream:
        print >>sys.stderr, tw
