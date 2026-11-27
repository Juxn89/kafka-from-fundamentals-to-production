#!/usr/bin/env bash
# Waits until the orders topic holds at least $2 events on broker container $1.
export MSYS_NO_PATHCONV=1
for i in $(seq 1 120); do
  n=$(docker exec "$1" /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic orders --time -1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
  [ "$n" -ge "$2" ] && exit 0
  sleep 0.5
done
exit 1
