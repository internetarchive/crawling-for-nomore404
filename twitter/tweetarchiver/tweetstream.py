import requests
import sys
import os
import json
import argparse
from configobj import ConfigObj


def connect_to_endpoint(bearer_token):
    expansions_fields = "author_id,referenced_tweets.id,attachments.media_keys,attachments.poll_ids,geo.place_id"
    user_fields = "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld"
    tweet_fields = "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld"
    media_fields = "duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width,alt_text"
    poll_fields = "duration_minutes,end_datetime,id,options,voting_status"
    place_fields = "contained_within,country,country_code,full_name,geo,id,name,place_type"

    url = f"https://api.twitter.com/2/tweets/sample/stream?expansions={expansions_fields}&user.fields={user_fields}&tweet.fields={tweet_fields}&media.fields={media_fields}&poll.fields={poll_fields}&place.fields={place_fields}"

    headers = {"Authorization":f"Bearer {bearer_token}","User-Agent":"v2SampledStreamPython"}

    response = requests.request("GET", url, stream=True, headers=headers)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            yield json_response
    