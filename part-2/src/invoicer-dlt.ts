// src/invoicer-dlt.ts: invoicer.ts, plus a dead-letter topic for invalid orders and a backoff for every other failure.
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { InvalidOrderError, INVOICES, kafka, TOPIC, toInvoice, type Invoice } from './kafka.ts';

const CLIENT_ID = process.env.CLIENT_ID ?? 'invoicer';
const DEAD_LETTER = 'orders.dlt'; // create it up front: the broker does not create topics on demand
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
      // Shutting down, shutdown() has already committed, and disconnect() would refuse this commit.
      if (!fns.assignmentLost() && !stopping) await commit(partitions);
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
let deadLettered = 0;
let totalInvoiced = 0;

async function handle({ partition, message }: KafkaJS.EachMessagePayload): Promise<void> {
  let invoice: Invoice | null = null;
  try {
    invoice = toInvoice(message.value, CLIENT_ID);
  } catch (error) {
    if (!(error instanceof InvalidOrderError)) throw error;
    // No retry can fix the data: park the event where someone can look at it, and move on.
    await producer.send({
      topic: DEAD_LETTER,
      messages: [{
        key: message.key,
        value: message.value,
        headers: { error: error.message, 'source-partition': String(partition), 'source-offset': message.offset },
      }],
    });
    deadLettered++;
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
        // Only local errors land here (the commit is queued, not acknowledged); failing the event would invoice it again.
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
    `  processed ${processed}  invoiced ${invoiced}` + (failed > 0 ? `  failed ${failed}` : '') +
    (deadLettered > 0 ? `  dead-lettered ${deadLettered}` : ''),
  );
  processed = 0;
  invoiced = 0;
  failed = 0;
  deadLettered = 0;
}, 1_000);

// A count alone never commits the last events of a partition that goes quiet: commit those every 5 s as well.
const committer = COMMIT_EVERY === null ? undefined : setInterval(() => {
  const pending = [...sinceCommit].filter(([, events]) => events > 0).map(([partition]) => partition);
  commit(pending).catch((error: unknown) => console.error(`timed commit failed, retrying in 5 s: ${String(error)}`));
}, 5_000);

let current: Promise<void> = Promise.resolve(); // the event being processed, if any
const failing = new Map<number, { offset: string; attempts: number }>(); // partition -> the event that keeps failing
await consumer.run({
  eachMessage: async (payload) => {
    const { topic, partition, message } = payload;
    current = handle(payload);
    try {
      await current;
      failing.delete(partition);
    } catch (error) {
      // Stop this partition, not the others, and retry the same event after 1 s, 2 s, 4 s ... at most 30 s.
      failed++;
      const previous = failing.get(partition);
      const attempt = previous?.offset === message.offset ? previous.attempts + 1 : 1;
      failing.set(partition, { offset: message.offset, attempts: attempt });
      const delayMs = Math.min(1_000 * 2 ** (attempt - 1), 30_000);
      console.error(`partition ${partition} offset ${message.offset}: attempt ${attempt} failed, retrying in ${delayMs} ms: ${String(error)}`);
      consumer.pause([{ topic, partitions: [partition] }]);
      setTimeout(() => {
        try {
          consumer.resume([{ topic, partitions: [partition] }]);
        } catch {
          // resume() throws only once the consumer is disconnecting: there is nothing left to resume.
        }
      }, delayMs);
      throw error; // the client seeks back to this event and delivers it again once the partition resumes
    }
  },
});

async function shutdown(signal: NodeJS.Signals): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(ticker);
  clearInterval(committer);
  console.log(`${signal}: finishing the current event, committing, leaving the group`);
  try {
    if (COMMIT_EVERY !== null) {
      // disconnect() refuses commits once it starts, so stop fetching, let the event in hand finish,
      // and commit before leaving.
      consumer.pause([{ topic: TOPIC }]);
      await current.catch(() => undefined);
      await commit([...nextOffset.keys()]);
    }
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
