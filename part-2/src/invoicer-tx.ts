// src/invoicer-tx.ts: the invoicer with transactions. A batch's invoices and the offset after it are
// committed together, or not at all.
import { hostname } from 'node:os';
import { KafkaJS } from '@confluentinc/kafka-javascript';
import { InvalidOrderError, INVOICES, kafka, TOPIC, toInvoice } from './kafka.ts';

const CLIENT_ID = process.env.CLIENT_ID ?? 'invoicer';
const DEAD_LETTER = 'orders.dlt';
// Must be unique among running instances, and the same when this instance restarts.
const TRANSACTIONAL_ID = process.env.TRANSACTIONAL_ID ?? `invoicer-${hostname()}`;

const producer = kafka.producer({ kafkaJS: { transactionalId: TRANSACTIONAL_ID, idempotent: true } });
const consumer = kafka.consumer({
  kafkaJS: {
    groupId: 'invoicer',
    fromBeginning: true,
    allowAutoTopicCreation: false,
    autoCommit: false, // the offsets travel inside each transaction instead
  },
  'group.protocol': 'consumer',
  // One transaction per batch, and each costs several round trips however few events it holds: this client's default batch is 32.
  'js.consumer.max.batch.size': 500,
});

let processed = 0;
let invoiced = 0;
let deadLettered = 0;
let transactions = 0;
let totalInvoiced = 0;

await producer.connect(); // registers TRANSACTIONAL_ID, aborting any transaction its last run left open
await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

const ticker = setInterval(() => {
  const partitions = consumer.assignment().map(({ partition }) => partition).sort((a, b) => a - b);
  console.log(
    `${new Date().toISOString().slice(11, 23)}  partitions ${partitions.join(',') || '-'}` +
    `  processed ${processed}  invoiced ${invoiced}  transactions ${transactions}` +
    (deadLettered > 0 ? `  dead-lettered ${deadLettered}` : ''),
  );
  processed = 0;
  invoiced = 0;
  deadLettered = 0;
  transactions = 0;
}, 1_000);

await consumer.run({
  eachBatch: async ({ batch }) => {
    const last = batch.messages[batch.messages.length - 1];
    if (last === undefined) return;

    const transaction = await producer.transaction();
    try {
      const invoices: KafkaJS.Message[] = [];
      const parked: KafkaJS.Message[] = [];
      for (const message of batch.messages) {
        try {
          const invoice = toInvoice(message.value, CLIENT_ID);
          if (invoice !== null) invoices.push({ key: invoice.orderId, value: JSON.stringify(invoice) });
        } catch (error) {
          if (!(error instanceof InvalidOrderError)) throw error;
          // Parked in the same transaction: the dead letter and the offset past it are committed together.
          parked.push({
            key: message.key,
            value: message.value,
            headers: { error: error.message, 'source-partition': String(batch.partition), 'source-offset': message.offset },
          });
        }
      }
      if (invoices.length > 0) await transaction.send({ topic: INVOICES, messages: invoices });
      if (parked.length > 0) await transaction.send({ topic: DEAD_LETTER, messages: parked });
      // The group's offset is written by the transaction, so it moves only if the invoices are kept.
      await transaction.sendOffsets({
        consumer,
        topics: [{ topic: batch.topic, partitions: [{ partition: batch.partition, offset: String(Number(last.offset) + 1) }] }],
      });
      await transaction.commit();
      processed += batch.messages.length;
      invoiced += invoices.length;
      deadLettered += parked.length;
      totalInvoiced += invoices.length;
      transactions++;
    } catch (error) {
      console.error(`partition ${batch.partition}: transaction failed, aborting: ${String(error)}`);
      try {
        // Nothing from this batch becomes visible to read_committed readers. The throw below makes the client
        // seek back to the batch's first event and deliver it again, in a new transaction.
        await transaction.abort();
      } catch (abortError) {
        // This producer was fenced, hit a fatal error, or still has a commit in flight: no later transaction
        // can start here. Leave the group and exit; a restart with the same TRANSACTIONAL_ID finishes or
        // aborts whatever this run left open.
        console.error('the transaction cannot be aborted, exiting:', abortError);
        void shutdown('cannot abort', 1);
      }
      throw error;
    }
  },
});

let stopping = false;
async function shutdown(reason: string, exitCode: number): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(ticker);
  console.log(`${reason}: finishing the current batch, leaving the group`);
  try {
    await consumer.disconnect(); // waits for the batch in eachBatch, transaction included
    await producer.disconnect();
    console.log(`left the group: ${totalInvoiced} invoices written`);
    process.exit(exitCode);
  } catch (error) {
    console.error('shutdown failed:', error);
    process.exit(1);
  }
}
process.once('SIGTERM', () => void shutdown('SIGTERM', 0));
process.once('SIGINT', () => void shutdown('SIGINT', 0));
