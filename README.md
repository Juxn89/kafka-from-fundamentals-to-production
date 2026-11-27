# kafka-from-fundamentals-to-production

Companion code for the dev.to series *Kafka from Fundamentals to Production*. Each part is a self-contained
lab, a Kafka 4.4 broker and a few Node 24 services in Docker Compose, with the article's code verbatim.

| Part | Article | Folder |
|---|---|---|
| 1 | *Topics, Partitions, and Consumer Groups* ([English](https://dev.to/jgomezdev/kafka-from-fundamentals-to-production-part-1-topics-partitions-and-consumer-groups-37cl), [español](https://dev.to/jgomezdev/kafka-de-los-fundamentos-a-produccion-parte-1-topics-particiones-y-grupos-de-consumidores-361g)) | [`part-1/`](part-1) |
| 2 | *Building a Real Event Streaming Platform* | [`part-2/`](part-2) |

All you need is Docker with Compose. Node 24 on your machine is only for type-checking; the services run in
`node:24-alpine`, because the client's prebuilt binaries stop at Node 24.

The stack in both parts: Apache Kafka 4.4.0 in KRaft mode (one broker, no ZooKeeper),
`@confluentinc/kafka-javascript` 1.11.0 (librdkafka 2.16.0 underneath, with a KafkaJS-compatible API), and
TypeScript that Node 24 runs directly, with no build step.

## Part 1: topics, partitions and consumer groups

The checkout producer emits three events per order, keyed by order id, and a billing consumer prints one line a
second: the partitions it owns, the events it processed, how long the oldest one waited, and how many arrived
out of order.

```bash
cd part-1
docker compose up -d kafka
docker compose run --rm tools node src/create-topic.ts 3
docker compose run --rm -e ORDERS=3000 -e RATE=1000 tools node src/produce-orders.ts
docker compose up -d --scale billing=3 billing
docker compose logs -f billing
docker compose down -v   # -v also removes the three anonymous volumes the Kafka image declares
```

| File | What it is |
|---|---|
| `src/kafka.ts` | The broker address, the topic and the shape of an order event |
| `src/produce-orders.ts` | Checkout. `KEYED=false` drops the key, to watch ordering break |
| `src/billing-consumer.ts` | One member of the `billing` group. `GROUP_PROTOCOL=classic` and `ASSIGNOR=cooperative-sticky` compare rebalance protocols |
| `src/create-topic.ts`, `src/describe-group.ts`, `src/where-keys-land.ts` | Create the topic, show owners and lag, show which partition each key lands on |

## Part 2: delivery guarantees

An invoicer reads the orders and writes one invoice per paid order to an `invoices` topic, and
`count-invoices.ts` reports how many paid orders were invoiced once, more than once, or never. The article
measures each delivery guarantee by those counts.

```bash
cd part-2
docker compose up -d kafka
for t in "orders 3" "invoices 3" "orders.dlt 1"; do docker compose run --rm tools node src/create-topic.ts $t; done
docker compose up -d --scale invoicer=3 invoicer
docker compose run --rm -e ORDERS=16000 -e RATE=200 tools node src/produce-orders.ts
docker compose run --rm tools node src/count-invoices.ts
docker compose down -v
```

### The article's code

| File | What it is |
|---|---|
| `src/invoicer.ts` | The invoicer. Unset, `COMMIT_EVERY` leaves commits to the client; a number N commits every N events per partition, with a commit on revoke, a 5 s timer and a commit before `disconnect()` |
| `src/invoicer-tx.ts` | The transactional invoicer: one transaction per batch of up to 500, offsets and dead letters inside it (`docker compose --profile tx up invoicer-tx`) |
| `src/invoicer-dlt.ts` | The invoicer with a dead-letter topic for invalid orders and a per-partition backoff for everything else (shown in the article as an excerpt) |
| `src/kafka.ts` | Topics, event and invoice shapes, and `toInvoice`, which throws `InvalidOrderError` for an event it cannot read or bill |
| `src/produce-orders.ts` | Part 1's checkout, plus `IDEMPOTENT=false` and `SOCKET_TIMEOUT_MS` for the producer-retry experiment |
| `src/count-invoices.ts`, `src/invoice-reader.ts` | Count invoices per paid order; follow the invoices as a downstream reader. Both take `READ_UNCOMMITTED=true` |

### The variants

Versions the article describes without printing in full. Each file opens with a comment saying what changed.
`compose.variant.yaml` runs one in place of the invoicer: `VARIANT=invoicer-naive.ts docker compose -f
compose.yaml -f compose.variant.yaml up -d invoicer`.

| File | What changes |
|---|---|
| `src/invoicer-naive.ts` | Commits every N events and nothing else: no listener, no timer, no commit before `disconnect()` |
| `src/invoicer-listener-only.ts` | Adds the rebalance listener as first written, which tries to commit during `disconnect()` and is refused |
| `src/invoicer-dlt-outage.ts` | `invoicer-dlt.ts` with every invoice write failing for `OUTAGE_FOR_S` seconds, to watch the backoff |
| `src/invoicer-tx-batch.ts` | `invoicer-tx.ts` with `MAX_BATCH` as the batch size (run with `compose.txbatch.yaml`) |

### The measurements

`part-2/scripts/` holds the scripts that produced the article's numbers, one experiment each. They write what
they captured to `part-2/captures/`, which git ignores.

| Script | What it does |
|---|---|
| `run-leave.sh <stop\|kill\|join> <label>` | Three invoicers, 16,000 orders; one leaves with `SIGTERM`, is killed, or joins |
| `run-throughput.sh <invoicer\|invoicer-tx> <label>` | One invoicer catches up on a backlog of 90,000 events |
| `run-producer.sh <true\|false> <label>` | Freezes the broker for 8 s under a producer with or without idempotence |
| `run-poison.sh <label>` | A free coffee (total 0) in the middle of a run; `VARIANT` picks the invoicer |
| `run-tx.sh`, `run-tx-faults.sh` | Kills a transactional invoicer; aborts with `orders.dlt` missing; fences one with a duplicate id |

### Two things the code does not hide

- `await consumer.commitOffsets()` in this client resolves once librdkafka has queued the commit, not once the
  broker has accepted it, so the `catch` blocks around commits only see local errors.
- A restart of `invoicer-tx` keeps its transactional id only if the container keeps its hostname, as with
  `docker restart`. The labs have no restart policy.

## Both parts share an image name

Both `compose.yaml` files tag their image `aurora-kafka-lab`, as the articles do. When you switch from one
part to the other, run `docker compose build` before anything else, or Compose reuses the other part's image.

## Checking it yourself

`scripts/verify.sh` runs each part on a fresh broker and checks what the articles rely on: one consumer
reading every event in order, every paid order invoiced exactly once, the commit timer committing a tail that
never reaches the count, and both bad events in the dead-letter topic. It never checks timings. CI runs it on
every push and pull request.

## License

[MIT](LICENSE)
