#!/bin/bash
#
HERE=$(cd $(dirname $0); pwd)

: ${KAFKA_SERVERS:=crawl-db07:9092,crawl-db02:9092}
: ${KAFKA_BIN:=$HERE/kafka/bin}
: ${DATADIR:=$HERE/data}
: ${CONSUMER_GROUP:=wikipedia-logger}

topic=$1
case "$topic" in
  wiki-irc|wiki-links) ;;
  '') echo Usage: $0 '{wiki-irc|wiki-links}' >&2; exit;;
  *) echo invalid topic: $topic >&2; exit;;
esac

CONSUMER_OPTS=(
  --bootstrap-server $KAFKA_SERVERS
  --topic $topic
  --consumer-property group.id=$CONSUMER_GROUP
)
$KAFKA_BIN/kafka-console-consumer.sh "${CONSUMER_OPTS[@]}" | \
    cronolog $DATADIR/$topic-%Y-%m-%d.log --symlink $DATADIR/$topic.log
