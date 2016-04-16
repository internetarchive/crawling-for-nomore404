#!/usr/bin/python
#
"""
Twitter Stream API client inspired by 
- https://github.com/joshmarshall/TweetStream
- https://github.com/ultrabug/geventweet

Features:
- uses gevent
- supports OAuth
- retries upon connection failure transparently.
"""
import gevent.monkey
gevent.monkey.patch_all()

import sys
import gevent
#import gevent.ssl
try:
    import ujson as json
except ImportError:
    import json
#import socket
#import gevent.socket as socket
import time
import httplib
import oauth2
import urlparse
from cStringIO import StringIO

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
        # prepopulating cache
        #self._ioloop = ioloop or IOLoop.instance()
        self._callback = None
        self._error_callback = None
        #self._clean_message = clean
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

        parts = urlparse.urlparse(path)
        #self._method = method
        #self._callback = callback
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
        while 1:
            if self._response is None:
                if self._connection_tries > 0:
                    backoff = min(self._connection_tries * BACKOFF_INCREMENT,
                                  BACKOFF_MAX)
                    time.sleep(backoff)
                self._connection_tries += 1
                self._open_twitter_stream()
                continue
            self._connection_tries = 0

            try:
                tweet = self._readline()
                if tweet == '':
                    self._response = None
                    continue
                
            except IOError as ex:
                self._response = None
                continue
            #return json.loads(tweet.rstrip())
            return tweet.rstrip()
            
    # def fetch(self, path, method="GET", callback=None):
    #     """ Opens the request """
    #     parts = urlparse.urlparse(path)
    #     self._method = method
    #     self._callback = callback
    #     self._path = parts.path
    #     self._full_path = self._path
    #     self._parameters = {}
    #     if parts.query:
    #         self._full_path = "{}?{}".format(self._full_path, parts.query)

    #     # throwing away empty or extra query arguments
    #     self._parameters = dict([
    #         (key, value[0]) for key, value in
    #         urlparse.parse_qs(parts.query).iteritems()
    #         if value
    #     ])
    #     self.open_twitter_stream()

    # def on_error(self, error):
    #     """ Just a wrapper for the error callback """
    #     if self._error_callback:
    #         return self._error_callback(error)
    #     else:
    #         raise error

    def _open_twitter_stream(self):
        """ Creates the client and watches stream """
        if self._twitter_stream_scheme == 'https':
            http = httplib.HTTPSConnection(self._twitter_stream_host,
                                           self._twitter_stream_port)
        else:
            http = httplib.HTTPConnection(self._twitter_stream_host,
                                          self._twitter_stream_port)
        # address_info = socket.getaddrinfo(self._twitter_stream_host,
        #     self._twitter_stream_port, socket.AF_INET, socket.SOCK_STREAM,
        #     0, 0)
        # af, socktype, proto = address_info[0][:3]
        # socket_address = address_info[0][-1]
        # sock = socket.socket(af, socktype, proto)
        # if self._twitter_stream_scheme == 'https':
        #     sock = gevent.ssl.SSLScoket(sock)
        # stream_class = IOStream
        # if self._twitter_stream_scheme == "https":
        #     stream_class = SSLIOStream
        # self._twitter_stream = stream_class(sock, io_loop=self._ioloop)
        # self._twitter_stream.connect(socket_address, self.on_connect)
        # sock.connect(socket_address)
        # self._twitter_stream_reader = sock.makefile()
        # self._twitter_stream_writer = sock.makefile('w')

        parameters = dict(self._parameters,
                          oauth_token=self._token.key,
                          oauth_consumer_key=self._consumer.key,
                          oauth_version='1.0',
                          oauth_nonce=oauth2.generate_nonce(),
                          oauth_timestamp=int(time.time())
                          )
        # parameters = {
        #     "oauth_token": self._token.key,
        #     "oauth_consumer_key": self._consumer.key,
        #     "oauth_version": "1.0",
        #     "oauth_nonce": oauth2.generate_nonce(),
        #     "oauth_timestamp": int(time.time())
        # }
        # parameters.update(self._parameters)
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

        http.request('GET', self._path, headers=headers)

        # headers["Host"] = self._twitter_stream_host
        # headers["User-Agent"] = "TweetStream"
        # headers["Accept"] = "*/*"
        # request = ["GET {} HTTP/1.1".format(self._full_path)]
        # request.extend("{}: {}".format(key, value)
        #                for key, value in headers.iteritems())
        # request = "\r\n".join(request) + "\r\n\r\n"
        # self._twitter_stream.write(str(request))
        # self._twitter_stream.read_until("\r\n\r\n", self.on_headers)
        #self._twitter_stream_writer.write(str(request))

        # status_line = self._twitter_stream_reader.readline()
        # response_code = status_line.replace("HTTP/1.1", "")
        # response_code = int(response_code.split()[0].strip())
        # if response_code != 200:

        self._response = http.getresponse()
        if self._response.status == 401:
            self._response.close()
            raise Exception('{} {}'.format(self._response.status,
                                           self._response.reason))
        if self._response.status != 200:
            self._response.close()
            self._response = None

        self._rbuf = StringIO()

    # def on_headers(self, response):
    #     """ Starts monitoring for results. """
    #     status_line = response.splitlines()[0]
    #     response_code = status_line.replace("HTTP/1.1", "")
    #     response_code = int(response_code.split()[0].strip())
    #     if response_code != 200:
    #         exception_string = "Could not connect: %s\n%s" % (
    #             status_line, response)
    #         headers = dict([
    #             (l.split(":")[0].lower(), ":".join(l.split(":")[1:]))
    #             for l in response.splitlines()[1:]
    #         ])
    #         content_length = int(headers.get("content-length") or 0)
    #         if not content_length:
    #             return self.on_error(Exception(exception_string))
    #         def get_error_body(content):
    #             full_string = "%s\n%s" % (exception_string, content)
    #             self.on_error(Exception(full_string))
    #         return self._twitter_stream.read_bytes(
    #             content_length, get_error_body)
            
    #     self.wait_for_message()

    # def wait_for_message(self):
    #     """ Throw a read event on the stack. """
    #     self._twitter_stream.read_until("\r\n", self.on_result)

    # def on_result(self, response):
    #     """ Gets length of next message and reads it """
    #     if (response.strip() == ""):
    #         return self.wait_for_message()
    #     length = int(response.strip(), 16)
    #     self._twitter_stream.read_bytes(length, self.parse_json)

    # def parse_json(self, response):
    #     """ Checks JSON message """
    #     if not response.strip():
    #         # Empty line, happens sometimes for keep alive
    #         return self.wait_for_message()
    #     try:
    #         response = json.loads(response)
    #     except ValueError:
    #         print "Invalid response:"
    #         print response
    #         return self.wait_for_message()

    #     self.parse_response(response)

    # def parse_response(self, response):
    #     """ Parse the twitter message """
    #     if self._clean_message:
    #         try:
    #             text = response["text"]
    #             name = response["user"]["name"]
    #             username = response["user"]["screen_name"]
    #             avatar = response["user"]["profile_image_url_https"]
    #         except KeyError, exc:
    #             print "Invalid tweet structure, missing %s" % exc
    #             return self.wait_for_message()

    #         response = {
    #             "type": "tweet",
    #             "text": text,
    #             "avatar": avatar,
    #             "name": name,
    #             "username": username,
    #             "time": int(time.time())
    #         }
    #     if self._callback:
    #         self._callback(response)
    #     self.wait_for_message()

if __name__ == '__main__':
    import argparse
    import configobj
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='twitter.conf')
    args = parser.parse_args()
    config = configobj.ConfigObj(args.config)
    if 'twitter' not in config:
        raise Exception('twitter section not found in twitter.conf')
    stream = TweetStream("/1.1/statuses/sample.json", config.get('twitter'))

    for tw in stream:
        print >>sys.stderr, tw
