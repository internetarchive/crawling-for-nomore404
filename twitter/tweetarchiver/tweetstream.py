import requests
import sys
import os
import json
import argparse
from configobj import ConfigObj

parser = argparse.ArgumentParser()
parser.add_argument(
    '-c', '--config', default='./twitter.conf',
    help='configuration file in INI format (default %(default)s)s')

args = parser.parse_args()
if not os.path.isfile(args.config):
    print("configuration file %s does not exist" % (args.config,),
          file=sys.stderr)
    exit(1)

conf = ConfigObj(args.config)
tw_config = conf.get('twitter')

bearer_token = tw_config.get('authoization')

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r


def connect_to_endpoint():
    url = "https://api.twitter.com/2/tweets/sample/stream?expansions=author_id&user.fields=entities"
    response = requests.request("GET", url, auth=bearer_oauth, stream=True)
    print(response.status_code)
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            yield json_response
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )