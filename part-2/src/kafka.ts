// src/kafka.ts: the broker address, the topics, and the shape of an order event and of its invoice.
import { KafkaJS } from '@confluentinc/kafka-javascript';

export const TOPIC = 'orders';
export const INVOICES = 'invoices';

export type OrderEventType = 'placed' | 'paid' | 'shipped';
export const STEPS: readonly OrderEventType[] = ['placed', 'paid', 'shipped'];

export type OrderEvent = {
  orderId: string;
  type: OrderEventType;
  seq: number;        // 1, 2, 3: the order in which checkout emitted them
  totalCents: number;
  producedAt: number; // epoch ms, so a consumer can tell how long the event waited
};

export function parseOrderEvent(value: Buffer | null): OrderEvent | null {
  if (value === null) return null;
  try {
    const event = JSON.parse(value.toString('utf8')) as Partial<OrderEvent>;
    const valid =
      typeof event.orderId === 'string' &&
      STEPS.includes(event.type as OrderEventType) &&
      Number.isInteger(event.seq) &&
      Number.isInteger(event.totalCents) &&
      typeof event.producedAt === 'number';
    return valid ? (event as OrderEvent) : null;
  } catch {
    return null;
  }
}

// One invoice per paid order: the side effect whose duplicates and gaps this lab counts.
export type Invoice = {
  orderId: string;
  totalCents: number;
  invoicedBy: string; // the invoicer instance that wrote it
  invoicedAt: number; // epoch ms
};

// Data no retry can fix, as opposed to a failure that may go away if the event is tried again later.
export class InvalidOrderError extends Error {}

// The invoice an order event calls for: null when it needs none, InvalidOrderError when it cannot be read or billed.
export function toInvoice(value: Buffer | null, invoicedBy: string): Invoice | null {
  const event = parseOrderEvent(value);
  if (event === null) throw new InvalidOrderError(`not a readable order event: ${String(value).slice(0, 200)}`);
  if (event.type !== 'paid') return null;
  if (event.totalCents <= 0) {
    throw new InvalidOrderError(`order ${event.orderId}: an invoice needs a positive total, got ${event.totalCents}`);
  }
  return { orderId: event.orderId, totalCents: event.totalCents, invoicedBy, invoicedAt: Date.now() };
}

export function parseInvoice(value: Buffer | null): Invoice | null {
  if (value === null) return null;
  try {
    const invoice = JSON.parse(value.toString('utf8')) as Partial<Invoice>;
    const valid =
      typeof invoice.orderId === 'string' &&
      Number.isInteger(invoice.totalCents) &&
      typeof invoice.invoicedBy === 'string' &&
      typeof invoice.invoicedAt === 'number';
    return valid ? (invoice as Invoice) : null;
  } catch {
    return null;
  }
}

export const kafka = new KafkaJS.Kafka({
  kafkaJS: {
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    clientId: process.env.CLIENT_ID ?? 'aurora',
    logLevel: KafkaJS.logLevel.WARN,
  },
});
