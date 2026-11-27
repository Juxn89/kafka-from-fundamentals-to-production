// src/invoicer-listener-only.ts: LAB ONLY. invoicer.ts with the listener as first written: no shutdown check, no commit
// timer, no commit before disconnect(). Its commit on revoke is refused during disconnect().
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { INVOICES, kafka, TOPIC, toInvoice, type Invoice } from './kafka.ts';

const CLIENT_ID = process.env.CLIENT_ID ?? 'invoicer';
// Unset: the client commits for us. A number N: we commit ourselves, after every N events of a partition.
const COMMIT_EVERY = process.env.COMMIT_EVERY ? Number(process.env.COMMIT_EVERY) : null;
if (COMMIT_EVERY !== null && (!Number.isInteger(COMMIT_EVERY) || COMMIT_EVERY < 1)) {
  throw new Error(`COMMIT_EVERY must be a positive integer, got ${process.env.COMMIT_EVERY}`);
}

// Each send is awaited, so there is never a batch to wait for: send at once instead of after linger.ms.
const producer = kafka.producer({ 'linger.ms': 0, kafkaJS: { idempotent: true } });

let stopping = false; // set once shutdown begins

// Only used when we commit ourselves.
const nextOffset = new Map<number, string>(); // partition -> offset after the last event processed
const sinceCommit = new Map<number, number>(); // partition -> events processed since its last commit

async function commit(partitions: number[]): Promise<void> {
  const offsets = partitions.flatMap((partition) => {
    const offset = nextOffset.get(partition);
    return offset === undefined ? [] : [{ topic: TOPIC, partition, offset }];
  });
  if (offsets.length > 0) await consumer.commitOffsets(offsets);
  for (const partition of partitions) sinceCommit.set(partition, 0);
}

type Revoked = { code: number };
type Assignment = { topic: string; partition: number }[];

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: 'invoicer',
    fromBeginning: true,
    allowAutoTopicCreation: false,
    autoCommit: COMMIT_EVERY === null,
  },
  'group.protocol': 'consumer',
  // Runs when partitions are assigned or taken away. Returning nothing keeps the default handling.
  rebalance_cb: async (event: Revoked, assignment: Assignment, fns: { assignmentLost: () => boolean }) => {
    if (COMMIT_EVERY === null || event.code !== KafkaJS.ErrorCodes.ERR__REVOKE_PARTITIONS) return;
    const partitions = assignment.map(({ partition }) => partition);
    try {
      // Commit what was processed before the partitions go, or their next owner processes it again.
      // A lost assignment (this member was evicted) cannot commit: its successor has already started.
      if (!fns.assignmentLost()) await commit(partitions);
    } catch (error) {
      console.error(`commit on revoke failed, the next owner repeats what it missed: ${String(error)}`);
    } finally {
      for (const partition of partitions) {
        nextOffset.delete(partition);
        sinceCommit.delete(partition);
      }
    }
  },
});

let processed = 0;
let invoiced = 0;
let failed = 0;
let totalInvoiced = 0;

async function handle({ partition, message }: KafkaJS.EachMessagePayload): Promise<void> {
  let invoice: Invoice | null;
  try {
    invoice = toInvoice(message.value, CLIENT_ID);
  } catch (error) {
    failed++;
    throw error; // the client seeks back to this event and delivers it again
  }
  if (invoice !== null) {
    // Awaited: the client stores this event's offset only after eachMessage resolves.
    await producer.send({ topic: INVOICES, messages: [{ key: invoice.orderId, value: JSON.stringify(invoice) }] });
    invoiced++;
    totalInvoiced++;
  }
  processed++;

  if (COMMIT_EVERY !== null) {
    nextOffset.set(partition, String(Number(message.offset) + 1));
    const pending = (sinceCommit.get(partition) ?? 0) + 1;
    sinceCommit.set(partition, pending);
    if (pending >= COMMIT_EVERY) {
      try {
        await commit([partition]);
      } catch (error) {
        // The invoice is written: failing the event now would send it again. The next event retries the commit.
        console.error(`partition ${partition}: commit failed, retrying after the next event: ${String(error)}`);
      }
    }
  }
}

await producer.connect();
await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

const ticker = setInterval(() => {
  const partitions = consumer.assignment().map(({ partition }) => partition).sort((a, b) => a - b);
  console.log(
    `${new Date().toISOString().slice(11, 23)}  partitions ${partitions.join(',') || '-'}` +
    `  processed ${processed}  invoiced ${invoiced}` + (failed > 0 ? `  failed ${failed}` : ''),
  );
  processed = 0;
  invoiced = 0;
  failed = 0;
}, 1_000);

let current: Promise<void> = Promise.resolve(); // the event being processed, if any
await consumer.run({
  eachMessage: async (payload) => {
    current = handle(payload);
    await current;
  },
});

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(ticker);
  console.log(`${signal}: finishing the current event, committing, leaving the group`);
  try {
    await consumer.disconnect(); // waits for an event still in eachMessage
    await producer.disconnect();
    console.log(`left the group: ${totalInvoiced} invoices written`);
    process.exit(0);
  } catch (error) {
    console.error('shutdown failed:', error);
    process.exit(1);
  }
}
process.once('SIGTERM', () => void shutdown('SIGTERM'));
process.once('SIGINT', () => void shutdown('SIGINT'));
