// src/create-topic.ts: creates a topic with a fixed number of partitions, e.g. `node src/create-topic.ts orders 3`.
import { kafka } from './kafka.ts';

const topic = process.argv[2];
if (topic === undefined || topic === '') throw new Error('usage: node src/create-topic.ts <topic> [partitions] [replication factor]');
const partitions = Number(process.argv[3] ?? 3);
// One copy of each partition is all a one-broker lab can hold. On a real cluster use 3.
const replicationFactor = Number(process.argv[4] ?? 1);
for (const [name, value] of [['partition count', partitions], ['replication factor', replicationFactor]] as const) {
  if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer, got ${value}`);
}

const admin = kafka.admin();
await admin.connect();
try {
  const created = await admin.createTopics({
    topics: [{ topic, numPartitions: partitions, replicationFactor }],
  });
  console.log(created
    ? `created "${topic}" with ${partitions} partitions`
    : `"${topic}" already exists; partition count left as it was`);
} finally {
  await admin.disconnect();
}
