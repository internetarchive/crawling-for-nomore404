"""
dependencies:
- twitter-stream (https://github.com/twitivity/twitter-stream.py)
"""

import json
from twitter_stream import SampledStream

class Stream(SampledStream):
    user_fields = ['username','entities']
    expansions = ['author_id']
    tweet_fields = ['created_at']
