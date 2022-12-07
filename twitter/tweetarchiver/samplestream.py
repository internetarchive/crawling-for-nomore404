"""
A simple client for Twitter Volume streams API v2 (aka "1% sample stream")

This is a simple, bare-bone, Volume streams specific client
implemented on top of urllib3. We don't need fancy Twitter client
library for just relaying JSON tweets from the API to Kafka. This
should be very stable as it is not affefted by dependency changes etc.

The client implements exponential back-off on repeated failures.
"""
import sys
import time
from urllib.parse import urlencode
from io import BufferedReader

from urllib3 import PoolManager
from urllib3.exceptions import HTTPError

import logging

def csl(*names):
    """builds comma-separated list str from a list of str"""
    return ','.join(names)

class TweetStream(object):
    """ Twitter stream connection """

    HEADERS = {
        'User-Agent': 'TweetStream',
        'Accept': '*/*'
        }

    # v2 API reference: https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/api-reference/get-tweets-sample-stream
    SAMPLE_API_URL = 'https://api.twitter.com/2/tweets/sample/stream'
    SAMPLE_API_PARAMS = {
        # -----------------------------------------------------------------
        # expansions/*.fields options below include everything described in
        # the API doc. If you're disabling any of them, comment them out
        # instead of removing.
        # -----------------------------------------------------------------
        # expansions ask for additional data for IDs referenced in a tweet.
        # they are returned in "includes" property.
        'expansions': csl(
            'attachments.poll_ids',
            'attachments.media_keys',
            'author_id',
            'edit_history_tweet_ids',
            'entities.mentions.username',
            'geo.place_id',
            'in_reply_to_user_id',
            'referenced_tweets.id',
            'referenced_tweets.id.author_id'
        ),
        # specifies fields to be included in the main tweet object.
        # this also applies to referenced tweets (returned in "includes" when
        # "referenced_tweets.id" is in expansion.
        # default for the main tweet object is: "id,text,edit_history_tweet_ids"
        'tweet.fields': csl(
            'attachments',
            'author_id',
            'context_annotations',
            'conversation_id',
            'created_at',
            'edit_controls',
            'edit_history_tweet_ids',
            'entities',
            'geo',
            'id',
            'in_reply_to_user_id',
            'lang',
            'non_public_metrics',
            'organic_metrics',
            'possibly_sensitive',
            'promoted_metrics',
            'public_metrics',
            'referenced_tweets',
            'reply_settings',
            'source',
            #'text',
            'withheld',
        ),
        # effective only when "attachments.media_keys" is in expansions.
        'media.fields': csl(
            'alt_text',
            'duration_ms',
            'height',
            'media_key',
            'non_public_metrics',
            'organic_metrics',
            'preview_image_url',
            'promoted_metrics',
            'public_metrics',
            'type',
            'url',
            'variants',
            'width'
        ),
        # effective only when "attachments.poll_ids" is in expansions
        'poll.fields': csl(
            'duration_minutes',
            'end_datetime',
            'id',
            'options',
            'voting_status'
        ),
        # effective only when expansions has any of "author_id",
        # "entities.mentions.username", "in_reply_to_user_id",
        # "referenced_tweets.id.author_id"
        'user.fields': csl(
            'created_at',
            'description',
            'entities',
            'id',
            'location',
            'name',
            'pinned_tweet_id',
            'profile_image_url',
            'protected',
            'public_metrics',
            'url',
            'username',
            'verified',
            'withheld'
        ),
        # effective only when "geo.place_id" is in expansions
        'place.fields': csl(
            'contained_within',
            'country',
            'country_code',
            'full_name',
            'geo',
            'id',
            'name',
            'place_type'
        )
    }

    # ref https://dev.twitter.com/docs/streaming-apis/connecting

    # when connection fails, increase backoff interval by BACKOFF_INCREMENT
    # starting with 0, up to MAXIMUM BACKOFF_MAX seconds.
    BACKOFF_INCREMENT = 0.250
    BACKOFF_MAX = 16

    BACKOFF_RATELIMIT_INITIAL = 60
    BACKOFF_RATELIMIT_FACTOR = 2

    def __init__(self, config):
        self._log = logging.getLogger(__name__)

        # self._timeout = self._get_configuration_key(
        #     "twitter_stream_timeout", 1800)

        self._response = None
        self._connection_tries = 0

        self._bearer_token = config['bearer_token']
        self._timeout = config.get('twitter_stream_timeout', 1800)

        # we don't need connection pool management, but this is easier than
        # dealing with low-level urllib3 API.
        self.pool_manager = PoolManager()

    def __iter__(self):
        return self

    def __next__(self):
        """Return next tweet coming in. Blocks until tweet arrives.
        Tweet is returned as raw bytes (not including EOL separating
        tweets - not parsed JSON.
        """
        while True:
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
                tweet = self._response.readline()
                if not tweet:
                    self._log.info('stream closed by the server')
                    self._response = None
                    continue
                return tweet.rstrip()
            except HTTPError as ex:
                # IncompleteRead is a sub-class of this exception.
                self._log.warning('failed to read line: %s', ex)
                self._response = None
                continue
            except OSError as ex:
                self._log.warning('failed to read line: %s', ex)
                self._response = None
                continue

    def _open_twitter_stream(self):
        """ Creates the client and watches stream """
        url = self.SAMPLE_API_URL
        params = self.SAMPLE_API_PARAMS
        headers = dict(
            self.HEADERS,
            Authorization=f'Bearer {self._bearer_token}',
        )
        r = self.pool_manager.request(
            'GET', url, fields=params, headers=headers,
            retries=0, timeout=float(self._timeout),
            preload_content=False
        )
        if r.status != 200:
            self._log.warning('%s: failed with status %s', url, r.status)
            self._response = None
            return

        self._response = BufferedReader(r)

def quicktest():
    import argparse
    import configobj

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='twitter.conf')
    args = parser.parse_args()

    config = configobj.ConfigObj(args.config)
    if 'twitter' not in config:
        raise Exception('twitter section not found in twitter.conf')

    logging.basicConfig(level=logging.INFO)

    stream = TweetStream(config.get('twitter'))
    for tw in stream:
        print(tw.decode('utf-8'))

if __name__ == '__main__':
    try:
        quicktest()
    except KeyboardInterrupt:
        pass
