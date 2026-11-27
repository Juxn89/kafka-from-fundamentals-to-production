#!/usr/bin/env bash
# Checkout produces 3,000 orders at 200/s with a 2 s request timeout; 5 s in, the broker is frozen with
# docker pause for PAUSE_S seconds. Then the topic is read back and its events counted.
# Usage: run-producer.sh <true|false> <label>   (true/false = IDEMPOTENT)
set -uo pipefail
export MSYS_NO_PATHCONV=1
idem=$1; label=$2
cd "$(dirname "$0")/.."
out=captures/$label; mkdir -p "$out"
dc() { docker compose "$@"; }
echo "idempotent=$idem pause=${PAUSE_S:-8}s socket_timeout=2000ms" > "$out/setup.txt"
dc build -q >/dev/null 2>&1
dc down -v --remove-orphans >/dev/null 2>&1
dc up -d kafka >/dev/null 2>&1
dc run --rm tools node src/create-topic.ts orders 3 2>/dev/null
dc run --rm -e ORDERS=3000 -e RATE=200 -e IDEMPOTENT=$idem -e SOCKET_TIMEOUT_MS=2000 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1 &
producer=$!
: > "$out/event.txt"
scripts/wait-flowing.sh aurora-kafka-kafka-1 600 || echo "events never started flowing" >> "$out/event.txt"
docker pause aurora-kafka-kafka-1 >/dev/null; echo "paused, with at least 600 events in the topic, at $(date -u +%H:%M:%S.%3N) (host clock)" >> "$out/event.txt"
sleep "${PAUSE_S:-8}"
docker unpause aurora-kafka-kafka-1 >/dev/null; echo "unpaused at $(date -u +%H:%M:%S.%3N) (host clock)" >> "$out/event.txt"
wait $producer
docker exec aurora-kafka-kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic orders --time -1 > "$out/offsets.txt" 2>/dev/null
docker exec aurora-kafka-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders \
  --from-beginning --timeout-ms 8000 > "$out/events.jsonl" 2>/dev/null
total=$(wc -l < "$out/events.jsonl"); unique=$(sort -u "$out/events.jsonl" | wc -l)
echo "events in topic: $total  distinct: $unique  duplicates: $(( total - unique ))" > "$out/audit.txt"
cat "$out/setup.txt" "$out/checkout.txt" "$out/event.txt" "$out/offsets.txt" "$out/audit.txt"
