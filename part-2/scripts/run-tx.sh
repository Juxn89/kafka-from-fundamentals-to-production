#!/usr/bin/env bash
# A read_committed invoice reader, three transactional invoicers, checkout producing 16,000 orders at 200/s.
# LEAVE_AFTER s in, invoicer-tx-3 is killed (kill), or killed and started again RESTART_AFTER s later (kill-restart).
# Usage: run-tx.sh <kill|kill-restart|none> <label>
set -uo pipefail
export MSYS_NO_PATHCONV=1
how=$1; label=$2
cd "$(dirname "$0")/.."
out=captures/$label; mkdir -p "$out"
files=(-f compose.yaml); [ -n "${EXTRA_COMPOSE:-}" ] && files+=(-f "$EXTRA_COMPOSE")
dc() { docker compose --profile tx "${files[@]}" "$@"; }
echo "how=$how restart_after=${RESTART_AFTER:-}" > "$out/setup.txt"
dc build -q >/dev/null 2>&1
dc down -v --remove-orphans >/dev/null 2>&1
dc up -d kafka >/dev/null 2>&1
dc run --rm tools node src/create-topic.ts orders 3 2>/dev/null
dc run --rm tools node src/create-topic.ts invoices 3 2>/dev/null
dc up -d invoice-reader >/dev/null 2>&1
dc up -d --scale invoicer-tx=3 invoicer-tx >/dev/null 2>&1
sleep 10
dc run --rm -e ORDERS=16000 -e RATE=200 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1 &
producer=$!
sleep "${LEAVE_AFTER:-15}"
if [ "$how" != none ]; then
  docker kill aurora-kafka-invoicer-tx-3 >/dev/null
  echo "invoicer-tx-3 killed at $(docker exec aurora-kafka-kafka-1 date -u +%H:%M:%S.%3N) (container clock, read after the command returned)" > "$out/event.txt"
  if [ "$how" = kill-restart ]; then
    sleep "${RESTART_AFTER:-5}"
    docker start aurora-kafka-invoicer-tx-3 >/dev/null
    echo "invoicer-tx-3 started again at $(docker exec aurora-kafka-kafka-1 date -u +%H:%M:%S.%3N)" >> "$out/event.txt"
  fi
fi
wait $producer
for i in $(seq 1 60); do
  dc run --rm tools node src/describe-group.ts invoicer > "$out/group.txt" 2>/dev/null
  if ! grep -q -E 'lag [1-9]' "$out/group.txt"; then break; fi
  sleep 2
done
sleep 5
dc stop invoicer-tx invoice-reader >/dev/null 2>&1
dc logs --no-color invoicer-tx > "$out/invoicers.txt" 2>&1
dc logs --no-color invoice-reader > "$out/reader.txt" 2>&1
dc run --rm tools node src/count-invoices.ts > "$out/count.txt" 2>/dev/null
dc run --rm -e READ_UNCOMMITTED=true tools node src/count-invoices.ts >> "$out/count.txt" 2>/dev/null
cat "$out/setup.txt" "$out/checkout.txt" "$out/event.txt" "$out/group.txt" "$out/count.txt"
