#!/usr/bin/env bash
# Invoicers + checkout producing 16,000 orders at 200/s. LEAVE_AFTER s in: invoicer-3 is stopped (stop),
# killed (kill), or started as a new member (join; the run starts with two). Then the group drains,
# every invoicer is stopped with SIGTERM, and the invoices are counted.
# Usage: run-leave.sh <stop|kill|join> <label>   env: COMMIT_EVERY, VARIANT (src file name), LEAVE_AFTER
set -uo pipefail
export MSYS_NO_PATHCONV=1
how=$1; label=$2
cd "$(dirname "$0")/.."
out=captures/$label; mkdir -p "$out"
files=(-f compose.yaml); [ -n "${VARIANT:-}" ] && files+=(-f compose.variant.yaml)
dc() { docker compose "${files[@]}" "$@"; }
start=3; [ "$how" = join ] && start=2
echo "how=$how commit_every=${COMMIT_EVERY:-auto} variant=${VARIANT:-invoicer.ts} start=$start" > "$out/setup.txt"
dc build -q >/dev/null 2>&1
dc down -v --remove-orphans >/dev/null 2>&1
dc up -d kafka >/dev/null 2>&1
dc run --rm tools node src/create-topic.ts orders 3 2>/dev/null
dc run --rm tools node src/create-topic.ts invoices 3 2>/dev/null
dc up -d --scale invoicer=$start invoicer >/dev/null 2>&1
sleep 10
dc run --rm -e ORDERS=16000 -e RATE=200 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1 &
producer=$!
sleep "${LEAVE_AFTER:-15}"
case $how in
  stop) docker stop aurora-kafka-invoicer-3 >/dev/null ;;
  kill) docker kill aurora-kafka-invoicer-3 >/dev/null ;;
  join) dc up -d --no-recreate --scale invoicer=3 invoicer >/dev/null 2>&1 ;;
esac
echo "invoicer-3: $how at $(docker exec aurora-kafka-kafka-1 date -u +%H:%M:%S.%3N) (container clock, read after the command returned)" > "$out/event.txt"
dc run --rm tools node src/describe-group.ts invoicer > "$out/group-after-$how.txt" 2>/dev/null
wait $producer
for i in $(seq 1 60); do
  dc run --rm tools node src/describe-group.ts invoicer > "$out/group.txt" 2>/dev/null
  if ! grep -q -E 'lag [1-9]' "$out/group.txt"; then break; fi
  sleep 2
done
dc stop invoicer >/dev/null 2>&1
dc logs --no-color invoicer > "$out/invoicers.txt" 2>&1
dc run --rm tools node src/count-invoices.ts > "$out/count.txt" 2>/dev/null
cat "$out/setup.txt" "$out/checkout.txt" "$out/event.txt" "$out/count.txt"
