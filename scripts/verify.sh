#!/usr/bin/env bash
# Runs each part's lab on a fresh broker and checks the behaviour its article relies on: which events are
# processed, how many invoices each order gets, what lands in the dead-letter topic. It never checks timings,
# which move between machines and between runs. CI runs it on every push and pull request.
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash on Windows would otherwise rewrite the /opt/kafka paths below

root=$(cd "$(dirname "$0")/.." && pwd)
broker=aurora-kafka-kafka-1

fail() { echo "FAIL: $*" >&2; exit 1; }
dc() { docker compose --profile tx --profile tools "$@"; }

fresh() { # a new broker with no data, and the image rebuilt from this part's files
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose build -q
  docker compose up -d --wait kafka >/dev/null
}

wait_lag_zero() { # group: until every partition's committed offset reaches its end
  local out=""
  for _ in $(seq 1 60); do
    out=$(docker compose run --rm tools node src/describe-group.ts "$1" 2>/dev/null || true)
    if grep -q committed <<<"$out" && ! grep -qE 'lag [1-9]' <<<"$out"; then return 0; fi
    sleep 2
  done
  echo "$out" >&2
  return 1
}

inject_bad_events() { # a free coffee (total 0) and a paid order whose JSON is cut off
  printf '%s\n' \
    'ord-free-coffee|{"orderId":"ord-free-coffee","type":"paid","seq":2,"totalCents":0,"producedAt":1}' \
    'ord-garbled|{"orderId":"ord-garbled","type":"paid","seq":2,"totalCents":"1250"' |
    docker exec -i $broker /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 \
      --topic orders --reader-property parse.key=true --reader-property key.separator='|' >/dev/null
}

dead_letters() {
  docker exec $broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 \
    --topic orders.dlt --from-beginning --timeout-ms 5000 2>/dev/null | wc -l | tr -d ' '
}

expect_count() { # expected count-invoices line, check name
  local got
  got=$(docker compose run --rm tools node src/count-invoices.ts 2>/dev/null)
  [ "$got" = "$1" ] || fail "$2: expected '$1', got '$got'"
}

# --- Part 1 -------------------------------------------------------------------------------------------------
cd "$root/part-1"
fresh
docker compose run --rm tools node src/create-topic.ts 3 >/dev/null
docker compose run --rm -e ORDERS=300 -e RATE=300 tools node src/produce-orders.ts >/dev/null
docker compose run -d --name verify-billing tools node src/billing-consumer.ts >/dev/null
wait_lag_zero billing || fail "part 1: the billing group never caught up"
docker stop verify-billing >/dev/null
logs=$(docker logs verify-billing 2>&1)
docker rm verify-billing >/dev/null
grep -q "left the group: 900 events processed, 0 out of order" <<<"$logs" \
  || fail "part 1: expected 900 events in order, got: $(tail -2 <<<"$logs")"
echo "part 1: one consumer processed all 900 events, none out of order"
dc down -v --remove-orphans >/dev/null 2>&1

# --- Part 2 -------------------------------------------------------------------------------------------------
cd "$root/part-2"
topics() { for t in "orders 3" "invoices 3" "orders.dlt 1"; do docker compose run --rm tools node src/create-topic.ts $t >/dev/null; done; }
produce() { docker compose run --rm -e ORDERS=300 -e RATE=300 tools node src/produce-orders.ts >/dev/null; }

# Auto-commit: every paid order invoiced exactly once.
fresh; topics; produce
docker compose up -d invoicer >/dev/null
wait_lag_zero invoicer || fail "part 2: the auto-commit invoicer never caught up"
docker compose stop invoicer >/dev/null
expect_count "read_committed: 300 paid orders, 300 invoiced once, 0 invoiced more than once (0 extra invoices), 0 never invoiced" "part 2, auto-commit"
echo "part 2: auto-commit invoiced 300 orders once each"

# Committing every 1,000 events: 900 events never reach a thousand on any partition, so only the timer commits them.
fresh; topics; produce
COMMIT_EVERY=1000 docker compose up -d invoicer >/dev/null
wait_lag_zero invoicer || fail "part 2: with COMMIT_EVERY=1000 the timer never committed the tail"
docker compose stop invoicer >/dev/null
docker compose logs invoicer | grep -q "left the group: 300 invoices written" || fail "part 2: the manual-commit invoicer did not shut down cleanly"
expect_count "read_committed: 300 paid orders, 300 invoiced once, 0 invoiced more than once (0 extra invoices), 0 never invoiced" "part 2, COMMIT_EVERY=1000"
echo "part 2: committing every 1,000 events, the timer committed a tail of fewer than 1,000"

# Dead-letter version: the free coffee and the unreadable event are parked, every other order is invoiced once.
fresh; topics; produce; inject_bad_events
VARIANT=invoicer-dlt.ts docker compose -f compose.yaml -f compose.variant.yaml up -d invoicer >/dev/null
wait_lag_zero invoicer || fail "part 2: the dead-letter invoicer stalled"
docker compose stop invoicer >/dev/null
expect_count "read_committed: 301 paid orders, 300 invoiced once, 0 invoiced more than once (0 extra invoices), 1 never invoiced" "part 2, dead-letter"
[ "$(dead_letters)" = 2 ] || fail "part 2: expected 2 events in orders.dlt"
echo "part 2: the dead-letter invoicer parked both bad events and invoiced the rest once"

# Transactions: the same, with the dead letters written inside the transactions.
fresh; topics; produce; inject_bad_events
docker compose --profile tx up -d invoicer-tx >/dev/null
wait_lag_zero invoicer || fail "part 2: the transactional invoicer stalled"
docker compose --profile tx stop invoicer-tx >/dev/null
expect_count "read_committed: 301 paid orders, 300 invoiced once, 0 invoiced more than once (0 extra invoices), 1 never invoiced" "part 2, transactions"
[ "$(dead_letters)" = 2 ] || fail "part 2: expected 2 events in orders.dlt from the transactional invoicer"
echo "part 2: the transactional invoicer parked both bad events and invoiced the rest once"

dc down -v --remove-orphans >/dev/null 2>&1
echo "all checks passed"
