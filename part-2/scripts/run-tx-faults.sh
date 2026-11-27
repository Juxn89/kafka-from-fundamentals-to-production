#!/usr/bin/env bash
# Failure paths of invoicer-tx.ts.
#   pause: a backlog of 9,000 orders plus a free coffee and an unreadable event; one invoicer-tx catches up and,
#          once invoices are flowing, the broker is frozen for 8 s. If the invoicer exited, it is started again.
#   fence: one invoicer-tx running with TRANSACTIONAL_ID=invoicer-shared while checkout produces; 10 s in, a
#          second instance starts with the same id.
# Usage: run-tx-faults.sh <pause|fence> <label>
set -uo pipefail
export MSYS_NO_PATHCONV=1
how=$1; label=$2
cd "$(dirname "$0")/.."
out=captures/$label; mkdir -p "$out"
dc() { docker compose --profile tx "$@"; }
broker=aurora-kafka-kafka-1
echo "how=$how" > "$out/setup.txt"
dc build -q >/dev/null 2>&1
dc down -v --remove-orphans >/dev/null 2>&1
docker rm -f tx-a tx-b >/dev/null 2>&1
dc up -d kafka >/dev/null 2>&1
topics=("orders 3" "invoices 3" "orders.dlt 1"); [ "$how" = nodlt ] && topics=("orders 3" "invoices 3")
for t in "${topics[@]}"; do dc run --rm tools node src/create-topic.ts $t 2>/dev/null; done
: > "$out/event.txt"
inject() {
  now=$(date +%s%3N)
  printf '%s\n' \
    "ord-free-coffee|{\"orderId\":\"ord-free-coffee\",\"type\":\"paid\",\"seq\":2,\"totalCents\":0,\"producedAt\":$now}" \
    "ord-garbled|{\"orderId\":\"ord-garbled\",\"type\":\"paid\",\"seq\":2,\"totalCents\":\"1250\"" |
    docker exec -i $broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 \
      --topic orders --reader-property parse.key=true --reader-property key.separator='|' >/dev/null 2>&1
  echo "free coffee and unreadable event produced at $(docker exec $broker date -u +%H:%M:%S.%3N)" >> "$out/event.txt"
}
if [ "$how" = pause ]; then
  dc run --rm -e ORDERS=15000 -e RATE=3000 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1
  inject
  dc run --rm -e ORDERS=15000 -e RATE=3000 tools node src/produce-orders.ts >> "$out/checkout.txt" 2>&1
  dc up -d --scale invoicer-tx=1 invoicer-tx >/dev/null 2>&1
  for i in $(seq 1 60); do
    n=$(docker exec $broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic invoices --time -1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
    [ "$n" -ge 2000 ] && break; sleep 0.2
  done
  docker pause $broker >/dev/null; echo "broker paused at $(date -u +%H:%M:%S.%3N) (host clock)" >> "$out/event.txt"
  sleep 8
  docker unpause $broker >/dev/null; echo "broker unpaused at $(date -u +%H:%M:%S.%3N) (host clock)" >> "$out/event.txt"
  sleep 20
  state=$(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}}' aurora-kafka-invoicer-tx-1)
  echo "invoicer-tx-1 20 s after unpause: $state" >> "$out/event.txt"
  if [ "${state%% *}" != running ]; then
    docker start aurora-kafka-invoicer-tx-1 >/dev/null; echo "invoicer-tx-1 started again at $(date -u +%H:%M:%S.%3N) (host clock)" >> "$out/event.txt"
  fi
  logs() { dc logs --no-color invoicer-tx > "$out/invoicers.txt" 2>&1; }
elif [ "$how" = nodlt ]; then
  dc up -d --scale invoicer-tx=1 invoicer-tx >/dev/null 2>&1
  sleep 10
  dc run --rm -e ORDERS=3000 -e RATE=200 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1 &
  producer=$!
  sleep 5
  inject
  sleep ${DLT_AFTER:-30}
  dc run --rm tools node src/create-topic.ts orders.dlt 1 2>/dev/null
  echo "orders.dlt created at $(docker exec $broker date -u +%H:%M:%S.%3N)" >> "$out/event.txt"
  wait $producer
  sleep 10
  logs() { dc logs --no-color invoicer-tx > "$out/invoicers.txt" 2>&1; }
else
  dc run -d --name tx-a -e TRANSACTIONAL_ID=invoicer-shared -e CLIENT_ID=tx-a invoicer-tx >/dev/null 2>&1
  sleep 10
  dc run --rm -e ORDERS=4000 -e RATE=200 tools node src/produce-orders.ts > "$out/checkout.txt" 2>&1 &
  producer=$!
  sleep 10
  dc run -d --name tx-b -e TRANSACTIONAL_ID=invoicer-shared -e CLIENT_ID=tx-b invoicer-tx >/dev/null 2>&1
  echo "tx-b started with the same id at $(docker exec $broker date -u +%H:%M:%S.%3N)" >> "$out/event.txt"
  wait $producer
  sleep 15
  echo "tx-a: $(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}}' tx-a)" >> "$out/event.txt"
  logs() { { docker logs tx-a 2>&1 | sed 's/^/tx-a  | /'; docker logs tx-b 2>&1 | sed 's/^/tx-b  | /'; } > "$out/invoicers.txt"; }
fi
for i in $(seq 1 60); do
  dc run --rm tools node src/describe-group.ts invoicer > "$out/group.txt" 2>/dev/null
  grep -q 'committed' "$out/group.txt" && ! grep -q -E 'lag [1-9]' "$out/group.txt" && break
  sleep 2
done
dc stop invoicer-tx >/dev/null 2>&1; docker stop tx-a tx-b >/dev/null 2>&1
logs
dc run --rm tools node src/count-invoices.ts > "$out/count.txt" 2>/dev/null
dc run --rm -e READ_UNCOMMITTED=true tools node src/count-invoices.ts >> "$out/count.txt" 2>/dev/null
docker exec $broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic orders.dlt \
  --from-beginning --timeout-ms 5000 --formatter-property print.key=true --formatter-property print.headers=true > "$out/dlt.txt" 2>/dev/null
docker rm -f tx-a tx-b >/dev/null 2>&1
cat "$out/setup.txt" "$out/checkout.txt" "$out/event.txt" "$out/group.txt" "$out/count.txt" "$out/dlt.txt"
