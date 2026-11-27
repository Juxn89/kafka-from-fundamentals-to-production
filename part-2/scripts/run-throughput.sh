#!/usr/bin/env bash
# Checkout produces a backlog of 30,000 orders (90,000 events) first; then ONE invoicer catches up alone.
# Usage: run-throughput.sh <service> <label>   (service: invoicer | invoicer-tx; COMMIT_EVERY passes through)
set -uo pipefail
export MSYS_NO_PATHCONV=1
service=$1; label=$2
cd "$(dirname "$0")/.."
out=captures/$label; mkdir -p "$out"
files=(-f compose.yaml); [ -n "${EXTRA_COMPOSE:-}" ] && files+=(-f "$EXTRA_COMPOSE")
dc() { docker compose --profile tx "${files[@]}" "$@"; }
echo "service=$service commit_every=${COMMIT_EVERY:-auto}" > "$out/setup.txt"
dc build -q >/dev/null 2>&1
dc down -v --remove-orphans >/dev/null 2>&1
dc up -d kafka >/dev/null 2>&1
dc run --rm tools node src/create-topic.ts orders 3 2>/dev/null
dc run --rm tools node src/create-topic.ts invoices 3 2>/dev/null
dc run --rm -e ORDERS=30000 -e RATE=3000 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1
dc up -d --scale "$service"=1 "$service" >/dev/null 2>&1
for i in $(seq 1 150); do
  sleep 2
  dc run --rm tools node src/describe-group.ts invoicer > "$out/group.txt" 2>/dev/null
  if grep -q 'committed' "$out/group.txt" && ! grep -q -E 'lag [1-9]' "$out/group.txt"; then break; fi
done
dc stop "$service" >/dev/null 2>&1
dc logs --no-color "$service" > "$out/invoicer.txt" 2>&1
dc run --rm tools node src/count-invoices.ts > "$out/count.txt" 2>/dev/null
# full seconds of catching up: drop the first and the last line with work in them
grep -oE 'processed [0-9]+' "$out/invoicer.txt" | awk '{print $2}' | awk '$1>0' | sed '1d;$d' | sort -n > "$out/per-second.txt"
n=$(wc -l < "$out/per-second.txt")
echo "seconds at full speed: $n  median events/s: $(sed -n "$(( (n+1)/2 ))p" "$out/per-second.txt")  min: $(head -1 "$out/per-second.txt")  max: $(tail -1 "$out/per-second.txt")" > "$out/summary.txt"
cat "$out/setup.txt" "$out/checkout.txt" "$out/count.txt" "$out/summary.txt"
