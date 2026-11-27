#!/usr/bin/env bash
# Three invoicers, checkout producing 6,000 orders at 200/s; 10 s in, one free order (total 0) is produced
# when POISON=true. Lag is sampled every 5 s; afterwards invoices and the dead-letter topic are counted.
# Usage: run-poison.sh <label>   env: VARIANT, POISON, OUTAGE_AT_S, OUTAGE_FOR_S, WATCH_S
set -uo pipefail
export MSYS_NO_PATHCONV=1
label=$1
cd "$(dirname "$0")/.."
out=captures/$label; mkdir -p "$out"
files=(-f compose.yaml); [ -n "${VARIANT:-}" ] && files+=(-f compose.variant.yaml)
dc() { docker compose "${files[@]}" "$@"; }
echo "variant=${VARIANT:-invoicer.ts} poison=${POISON:-false} garbage=${GARBAGE:-false} outage_at=${OUTAGE_AT_S:-0} outage_for=${OUTAGE_FOR_S:-0}" > "$out/setup.txt"
dc build -q >/dev/null 2>&1
dc down -v --remove-orphans >/dev/null 2>&1
dc up -d kafka >/dev/null 2>&1
dc run --rm tools node src/create-topic.ts orders 3 2>/dev/null
dc run --rm tools node src/create-topic.ts invoices 3 2>/dev/null
dc run --rm tools node src/create-topic.ts orders.dlt 1 2>/dev/null
dc up -d --scale invoicer=3 invoicer >/dev/null 2>&1
sleep 10
dc run --rm -e ORDERS=6000 -e RATE=200 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1 &
producer=$!
sleep 10
if [ "${POISON:-false}" = true ]; then
  now=$(date +%s%3N)
  printf '%s\n' \
    "ord-free-coffee|{\"orderId\":\"ord-free-coffee\",\"type\":\"placed\",\"seq\":1,\"totalCents\":0,\"producedAt\":$now}" \
    "ord-free-coffee|{\"orderId\":\"ord-free-coffee\",\"type\":\"paid\",\"seq\":2,\"totalCents\":0,\"producedAt\":$now}" \
    "ord-free-coffee|{\"orderId\":\"ord-free-coffee\",\"type\":\"shipped\",\"seq\":3,\"totalCents\":0,\"producedAt\":$now}"     $( [ "${GARBAGE:-false}" = true ] && echo "ord-garbled|{\"orderId\":\"ord-garbled\",\"type\":\"paid\",\"seq\":2,\"totalCents\":\"1250\"" ) |
    docker exec -i aurora-kafka-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 \
      --topic orders --reader-property parse.key=true --reader-property key.separator='|' 2>&1 | grep -v '^>' ;
  echo "free order produced at $(docker exec aurora-kafka-kafka-1 date -u +%H:%M:%S.%3N)" > "$out/event.txt"
fi
: > "$out/lag.txt"
for i in $(seq 1 $(( (${WATCH_S:-60}) / 5 ))); do
  { echo "--- $(docker exec aurora-kafka-kafka-1 date -u +%H:%M:%S)"; dc run --rm tools node src/describe-group.ts invoicer 2>/dev/null | grep partition; } >> "$out/lag.txt"
  sleep 3
done
wait $producer
dc stop invoicer >/dev/null 2>&1
dc logs --no-color invoicer > "$out/invoicers.txt" 2>&1
dc run --rm tools node src/count-invoices.ts > "$out/count.txt" 2>/dev/null
docker exec aurora-kafka-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders.dlt \
  --from-beginning --timeout-ms 5000 --formatter-property print.key=true --formatter-property print.headers=true > "$out/dlt.txt" 2>/dev/null
cat "$out/setup.txt" "$out/checkout.txt" "${out}/event.txt" "$out/count.txt" "$out/dlt.txt" 2>/dev/null
echo "client error lines: $(grep -c 'Consumer encountered error while processing message' "$out/invoicers.txt")"
