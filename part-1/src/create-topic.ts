// src/create-topic.ts: creates the orders topic with a fixed number of partitions.
import { kafka, TOPIC } from './kafka.ts';

const partitions = Number(process.argv[2] ?? 3);
// One copy of each partition is all a one-broker lab can hold. On a real cluster use 3.
const replicationFactor = Number(process.argv[3] ?? 1);
for (const [name, value] of [['partition count', partitions], ['replication factor', replicationFactor]] as const) {
  if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer, got ${value}`);
}

const admin = kafka.admin();
await admin.connect();
try {
  const created = await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: partitions, replicationFactor }],
  });
  console.log(created
    ? `created "${TOPIC}" with ${partitions} partitions`
    : `"${TOPIC}" already exists; partition count left as it was`);
} finally {
  await admin.disconnect();
}
