// src/count-invoices.ts: reads every paid order and every invoice from the beginning and compares them.
// READ_UNCOMMITTED=true also reads invoices that belong to aborted transactions. A read_committed count
// stops at an open transaction, so it reports only what has been committed when it runs.
import { randomUUID } from 'node:crypto';
import { INVOICES, kafka, parseInvoice, parseOrderEvent, TOPIC } from './kafka.ts';

const READ_UNCOMMITTED = process.env.READ_UNCOMMITTED === 'true';
const IDLE_MS = 3_000; // stop once nothing new has arrived for this long

const paid = new Set<string>();
const invoices = new Map<string, number>(); // order id -> invoices seen for it

const consumer = kafka.consumer({
  kafkaJS: {
    groupId: `count-invoices-${randomUUID()}`, // a fresh group: always from offset 0, never commits
    fromBeginning: true,
    autoCommit: false,
    allowAutoTopicCreation: false,
    readUncommitted: READ_UNCOMMITTED,
  },
});
await consumer.connect();
await consumer.subscribe({ topics: [TOPIC, INVOICES] });

let lastMessageAt = 0; // the idle clock starts at the first message, not before the group has joined
await consumer.run({
  eachMessage: async ({ topic, message }) => {
    lastMessageAt = Date.now();
    if (topic === TOPIC) {
      const event = parseOrderEvent(message.value);
      if (event?.type === 'paid') paid.add(event.orderId);
    } else {
      const invoice = parseInvoice(message.value);
      if (invoice !== null) invoices.set(invoice.orderId, (invoices.get(invoice.orderId) ?? 0) + 1);
    }
  },
});

while (lastMessageAt === 0 || Date.now() - lastMessageAt < IDLE_MS) {
  await new Promise((resolve) => setTimeout(resolve, 250));
}
await consumer.disconnect();

let once = 0;
let duplicated = 0;
let extra = 0;
let missing = 0;
for (const orderId of paid) {
  const count = invoices.get(orderId) ?? 0;
  if (count === 0) missing++;
  else if (count === 1) once++;
  else {
    duplicated++;
    extra += count - 1;
  }
}
const orphans = [...invoices.keys()].filter((orderId) => !paid.has(orderId)).length;
console.log(
  `${READ_UNCOMMITTED ? 'read_uncommitted' : 'read_committed'}: ${paid.size} paid orders, ` +
  `${once} invoiced once, ${duplicated} invoiced more than once (${extra} extra invoices), ` +
  `${missing} never invoiced` + (orphans > 0 ? `, ${orphans} invoices for unknown orders` : ''),
);
