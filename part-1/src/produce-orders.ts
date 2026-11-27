// src/produce-orders.ts: simulates checkout. Each order emits placed, paid, shipped, STEP_MS apart.
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { kafka, STEPS, TOPIC, type OrderEvent } from './kafka.ts';

const ORDERS = Number(process.env.ORDERS ?? 3_000);
const RATE = Number(process.env.RATE ?? 200);       // new orders per second
const STEP_MS = Number(process.env.STEP_MS ?? 50);  // gap between one order's events
const KEYED = process.env.KEYED !== 'false';        // KEYED=false drops the key, on purpose

const producer = kafka.producer({ kafkaJS: { idempotent: true } });
await producer.connect();

let stopping = false;
process.once('SIGINT', () => { stopping = true; });
process.once('SIGTERM', () => { stopping = true; });

let sent = 0;
let failedOrders = 0;

async function runOrder(orderId: string, totalCents: number): Promise<void> {
  for (const [index, type] of STEPS.entries()) {
    if (index > 0) await sleep(STEP_MS);
    const event: OrderEvent = { orderId, type, seq: index + 1, totalCents, producedAt: Date.now() };
    // send() resolves once the broker has the event (acks=all, idempotent).
    await producer.send({
      topic: TOPIC,
      messages: [{ key: KEYED ? orderId : null, value: JSON.stringify(event) }],
    });
    sent++;
  }
}

const started = performance.now();
const inFlight = new Set<Promise<void>>();
let orders = 0;

for (; orders < ORDERS && !stopping; orders++) {
  const wait = started + (orders * 1_000) / RATE - performance.now();
  if (wait > 0) await sleep(wait);
  const order: Promise<void> = runOrder(`ord-${randomUUID().slice(-12)}`, 900 + Math.floor(Math.random() * 4_000))
    .catch((error: unknown) => {
      failedOrders++;
      console.error('order not fully published:', error);
    })
    .finally(() => inFlight.delete(order));
  inFlight.add(order);
}

await Promise.all(inFlight);
await producer.disconnect();

const seconds = (performance.now() - started) / 1_000;
console.log(`${sent} events for ${orders} orders in ${seconds.toFixed(1)} s, ${KEYED ? 'keyed by order id' : 'no key'}`);
if (failedOrders > 0) {
  console.error(`${failedOrders} orders were not fully published`);
  process.exitCode = 1;
}
