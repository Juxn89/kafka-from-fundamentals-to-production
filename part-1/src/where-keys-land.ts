// src/where-keys-land.ts: sends one event for each of 1,000 fixed order ids and prints, as JSON on stdout,
// the partition each one landed on. With --compare, it reads an earlier run's JSON from stdin and
// reports on stderr how many keys moved.
import { text } from 'node:stream/consumers';
import { kafka, TOPIC, type OrderEvent } from './kafka.ts';

const compare = process.argv.includes('--compare');
const previous = compare ? (JSON.parse(await text(process.stdin)) as Record<string, number>) : null;

const keys = Array.from({ length: 1_000 }, (_, i) => `ord-key-${String(i).padStart(4, '0')}`);
const producer = kafka.producer({ kafkaJS: { idempotent: true } });
await producer.connect();

const landed: Record<string, number> = {};
try {
  await Promise.all(keys.map(async (orderId) => {
    const event: OrderEvent = { orderId, type: 'placed', seq: 1, totalCents: 1_000, producedAt: Date.now() };
    const [metadata] = await producer.send({ topic: TOPIC, messages: [{ key: orderId, value: JSON.stringify(event) }] });
    if (metadata === undefined) throw new Error(`no delivery report for ${orderId}`);
    landed[orderId] = metadata.partition;
  }));
} finally {
  await producer.disconnect();
}
process.stdout.write(`${JSON.stringify(landed)}\n`);

const perPartition = new Map<number, number>();
for (const partition of Object.values(landed)) perPartition.set(partition, (perPartition.get(partition) ?? 0) + 1);
const spread = [...perPartition].sort(([a], [b]) => a - b).map(([p, n]) => `p${p}=${n}`).join(' ');
console.error(`${keys.length} keys over ${perPartition.size} partitions: ${spread}`);

if (previous !== null) {
  const moved = keys.filter((key) => previous[key] !== landed[key]).length;
  console.error(`${moved} of ${keys.length} keys land on a different partition than in the earlier run`);
}
