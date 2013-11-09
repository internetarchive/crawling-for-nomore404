# consumer-hq

This application consumes wiki-links queue, and submits citation URLs to
Headquarter server for crawling.

## How to run

1. create a virtualenv

> mkdir /1/crawling/wikipedia-hq
> cd /1/crawling/wikipedia-hq
> virtualenv venv
> . venv/bin/activate

install libevent-dev needed to build gevent

> sudo apt-get install libevent-dev

install consumer-hq. this will pull all dependencies including private
libraries crawllib and patched python-kafka.

> pip install -e <repository>/wikipedia/consumer-hq

2. configure

(currently there's no configuratin needed - everything is hard-coded!)

3. run

> venv/bin/crawl-schedule.py

## TODO

everything is hard-coded in this version. queue name, HQ server name,
HQ job name, etc. need to be made configurable.
