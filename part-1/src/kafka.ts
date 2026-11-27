// src/kafka.ts: the broker address, the topic, and the shape of an order event.
import { KafkaJS } from '@confluentinc/kafka-javascript';

export const TOPIC = 'orders';

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

export const kafka = new KafkaJS.Kafka({
  kafkaJS: {
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    clientId: process.env.CLIENT_ID ?? 'aurora',
    logLevel: KafkaJS.logLevel.WARN,
  },
});
