// src/describe-group.ts: which member owns which partition, and how far behind each partition is.
import { kafka, TOPIC } from './kafka.ts';

const groupId = process.argv[2] ?? 'billing';
const STATES = ['unknown', 'preparing rebalance', 'completing rebalance', 'stable', 'dead', 'empty'];
const TYPES = ['unknown', 'consumer', 'classic'];

const admin = kafka.admin();
await admin.connect();
try {
  const { groups: [group] } = await admin.describeGroups([groupId]);
  if (group === undefined || group.error) throw new Error(`cannot describe ${groupId}: ${group?.error?.message}`);
  console.log(`group ${groupId}: ${STATES[group.state]}, ${TYPES[group.type]} protocol, ${group.members.length} members`);
  for (const member of group.members) {
    const owned = member.assignment.topicPartitions
      .filter(({ topic }) => topic === TOPIC)
      .map(({ partition }) => partition)
      .sort((a, b) => a - b);
    console.log(`  member ${member.memberId.slice(-8)}  partitions ${owned.length > 0 ? owned.join(',') : 'none'}`);
  }

  const [committed] = await admin.fetchOffsets({ groupId, topics: [TOPIC] });
  const ends = await admin.fetchTopicOffsets(TOPIC);
  for (const { partition, high } of ends.sort((a, b) => a.partition - b.partition)) {
    const offset = committed?.partitions.find((p) => p.partition === partition)?.offset ?? '-1';
    const done = Number(offset) < 0 ? 0 : Number(offset);
    console.log(`  partition ${partition}  committed ${done}  end ${high}  lag ${Number(high) - done}`);
  }
} finally {
  await admin.disconnect();
}
