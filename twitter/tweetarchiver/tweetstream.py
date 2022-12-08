import requests
import sys
import os
import json

def csl(*names):
    """builds comma-separated list str from a list of str"""
    return ','.join(names)

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
        'text',
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

def connect_to_endpoint(bearer_token):
    url = SAMPLE_API_URL
    params = SAMPLE_API_PARAMS

    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "User-Agent": "v2SampledStreamPython"
    }

    response = requests.request(
        "GET", url, params=params, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    yield from response.iter_lines()


def quicktest():
    import argparse
    import configobj
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default='twitter.conf')
    args = parser.parse_args()

    config = configobj.ConfigObj(args.config)
    if 'twitter' not in config:
        raise Exception('twitter section not found in twitter.conf')

    logging.basicConfig(level=logging.INFO)

    stream = connect_to_endpoint(config['twitter']['authorization'])
    for tw in stream:
        print(tw.decode('utf-8'))

if __name__ == '__main__':
    try:
        quicktest()
    except KeyboardInterrupt:
        pass
