// src/invoice-reader.ts: a downstream service that reads invoices. Prints one line per second.
// READ_UNCOMMITTED=true also reads invoices from transactions that are still open or were aborted.
import { INVOICES, kafka, parseInvoice } from './kafka.ts';

const READ_UNCOMMITTED = process.env.READ_UNCOMMITTED === 'true';

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: process.env.GROUP_ID ?? 'invoice-reader',
    fromBeginning: true,
    allowAutoTopicCreation: false,
    readUncommitted: READ_UNCOMMITTED,
  },
  'group.protocol': 'consumer',
});

let received = 0;
let maxDelayMs = 0;
let total = 0;

await consumer.connect();
await consumer.subscribe({ topics: [INVOICES] });

const ticker = setInterval(() => {
  console.log(`${new Date().toISOString().slice(11, 23)}  invoices ${received}  max delay ${maxDelayMs} ms`);
  received = 0;
  maxDelayMs = 0;
}, 1_000);

await consumer.run({
  eachMessage: async ({ message }) => {
    const invoice = parseInvoice(message.value);
    if (invoice === null) return;
    received++;
    total++;
    maxDelayMs = Math.max(maxDelayMs, Date.now() - invoice.invoicedAt); // how long since it was written
  },
});

let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(ticker);
  try {
    await consumer.disconnect();
    console.log(`left the group: ${total} invoices read`);
    process.exit(0);
  } catch (error) {
    console.error('disconnect failed:', error);
    process.exit(1);
  }
}
process.once('SIGTERM', () => void shutdown());
process.once('SIGINT', () => void shutdown());
