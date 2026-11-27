// src/billing-consumer.ts: one member of a consumer group. Prints one line per second.
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { kafka, parseOrderEvent, TOPIC } from './kafka.ts';

const GROUP_ID = process.env.GROUP_ID ?? 'billing';
const PROTOCOL = process.env.GROUP_PROTOCOL === 'classic' ? 'classic' : 'consumer';
// Classic protocol only: the members pick the assignor, and this client's default is eager round-robin.
// Under the consumer protocol the broker assigns partitions itself.
const COOPERATIVE = process.env.ASSIGNOR === 'cooperative-sticky';

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: GROUP_ID,
    fromBeginning: true,
    allowAutoTopicCreation: false, // this client defaults it to true: a typo would create a topic
    ...(PROTOCOL === 'classic' && COOPERATIVE
      ? { partitionAssigners: [KafkaJS.PartitionAssigners.cooperativeSticky] }
      : {}),
  },
  'group.protocol': PROTOCOL,
});

// Per-order progress: an event is out of order when it is not the step after the last one seen.
const lastSeq = new Map<string, number>();
const MAX_TRACKED_ORDERS = 100_000;
let outOfOrder = 0;
let total = 0;
let processed = 0;
let maxWaitMs = 0;
let skipped = 0;

function track(orderId: string, seq: number): void {
  const previous = lastSeq.get(orderId) ?? 0;
  if (seq !== previous + 1) outOfOrder++;
  if (seq === 3 && previous === 2) {
    lastSeq.delete(orderId); // finished in order: nothing left to check
    return;
  }
  lastSeq.set(orderId, Math.max(previous, seq));
  if (lastSeq.size > MAX_TRACKED_ORDERS) {
    lastSeq.delete(lastSeq.keys().next().value as string); // oldest entry
  }
}

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

const ticker = setInterval(() => {
  const partitions = consumer.assignment().map(({ partition }) => partition).sort((a, b) => a - b);
  console.log(
    `${new Date().toISOString().slice(11, 23)}  partitions ${partitions.join(',') || '-'}` +
    `  processed ${processed}  max wait ${maxWaitMs} ms  out of order ${outOfOrder}` +
    (skipped > 0 ? `  skipped ${skipped}` : ''),
  );
  processed = 0;
  maxWaitMs = 0;
}, 1_000);

await consumer.run({
  eachMessage: async ({ message }) => {
    const event = parseOrderEvent(message.value);
    if (event === null) {
      skipped++; // not an order event; a real service would park it on a dead-letter topic
      return;
    }
    track(event.orderId, event.seq);
    processed++;
    total++;
    maxWaitMs = Math.max(maxWaitMs, Date.now() - event.producedAt);
  },
});

let stopping = false;
async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(ticker);
  console.log(`${signal}: finishing the current event, committing offsets, leaving the group`);
  try {
    await consumer.disconnect(); // an event already in eachMessage completes before this resolves
    console.log(`left the group: ${total} events processed, ${outOfOrder} out of order`);
    process.exit(0);
  } catch (error) {
    console.error('disconnect failed:', error);
    process.exit(1);
  }
}
process.once('SIGTERM', () => void shutdown('SIGTERM'));
process.once('SIGINT', () => void shutdown('SIGINT'));
