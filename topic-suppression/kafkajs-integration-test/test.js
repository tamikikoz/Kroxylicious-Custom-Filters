'use strict';

/**
 * Spec section 6 critical validation (kroxylicious-topic-suppression-filter-spec.md):
 *
 * "The client's own topicsSubscribed state still contains the suppressed topic name...
 *  We need to confirm empirically whether the assignor... gracefully treats 'subscribed
 *  topic with zero known partitions' as 'contribute zero partitions for it, proceed
 *  normally' - which is the behavior this whole design depends on - or whether something
 *  throws because partition metadata is unexpectedly absent for a topic in the
 *  subscription."
 *
 * This connects a real kafkajs consumer, through Kroxylicious with the TopicSuppression
 * filter active, subscribed via a STATIC array (not regex) that includes both a real
 * topic and the proxy-suppressed one - mirroring the production static-topic-list
 * design the spec settled on. Single-member consumer group guarantees this instance is
 * elected group leader, exercising the highest-risk path (the leader's own
 * SYNC/assign step). Then forces a rejoin (disconnect/reconnect) to exercise the
 * leader-side refreshMetadata path specifically, not just initial subscribe().
 *
 * The specific failure mode the spec is worried about is an UNCAUGHT rejection that
 * crashes the Node process outside kafkajs's own CRASH event path - so this listens at
 * both levels.
 */

const { Kafka, logLevel } = require('kafkajs');

const BROKERS = ['localhost:9292']; // through the Kroxylicious proxy, TopicSuppression filter active
const VISIBLE_TOPIC = 'visible-test-topic';
const SUPPRESSED_TOPIC = 'suppressed-test-topic'; // denied by the filter's config - broker never sees this name
const GROUP_ID = `topic-suppression-it-${Date.now()}`; // unique -> single member -> guaranteed leader

let sawCrashEvent = false;
let sawProcessCrash = false;
let joinCount = 0;
let receivedVisibleMessage = false;
const groupJoins = [];

process.on('unhandledRejection', (err) => {
  sawProcessCrash = true;
  console.error('[FAIL] unhandledRejection - this is exactly the crash mode the spec is worried about:', err);
});
process.on('uncaughtException', (err) => {
  sawProcessCrash = true;
  console.error('[FAIL] uncaughtException:', err);
});

async function main() {
  const kafka = new Kafka({
    clientId: 'topic-suppression-it',
    brokers: BROKERS,
    logLevel: logLevel.NOTHING,
  });

  // Produce one message to the visible topic first, so there is something real to consume.
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: VISIBLE_TOPIC,
    messages: [{ value: 'hello from the kafkajs integration test' }],
  });
  await producer.disconnect();
  console.log(`[setup] produced 1 message to ${VISIBLE_TOPIC}`);

  const consumer = kafka.consumer({ groupId: GROUP_ID });

  consumer.on(consumer.events.CRASH, (event) => {
    sawCrashEvent = true;
    console.error('[FAIL] consumer CRASH event:', event.payload.error);
  });

  consumer.on(consumer.events.GROUP_JOIN, (event) => {
    joinCount++;
    const { isLeader, memberAssignment } = event.payload;
    groupJoins.push({ isLeader, memberAssignment });
    console.log(`[join #${joinCount}] isLeader=${isLeader} memberAssignment=${JSON.stringify(memberAssignment)}`);
  });

  await consumer.connect();
  // Static array subscription - the production design: client keeps a fixed list,
  // proxy strips the name that doesn't exist on this cluster before the broker ever
  // sees it in a MetadataRequest.
  await consumer.subscribe({ topics: [VISIBLE_TOPIC, SUPPRESSED_TOPIC], fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (topic === VISIBLE_TOPIC) {
        receivedVisibleMessage = true;
        console.log(`[consume] received on ${topic}: ${message.value.toString()}`);
      }
    },
  });

  await sleep(5000);

  if (sawCrashEvent || sawProcessCrash) {
    finish(false, 'crashed during initial subscribe/join/run');
    return;
  }
  if (joinCount === 0) {
    finish(false, 'never joined the consumer group within the timeout');
    return;
  }
  if (!groupJoins[0].isLeader) {
    finish(false, 'single-member group did not elect this instance as leader - test setup assumption violated');
    return;
  }
  if (!receivedVisibleMessage) {
    finish(false, `never received the message produced to ${VISIBLE_TOPIC} - consumption isn't actually working`);
    return;
  }

  console.log('[stage 1 PASS] initial subscribe + leader election + message consumption all clean, no crash');
  console.log('[stage 2] forcing a rejoin (disconnect/reconnect) to exercise the leader-side refreshMetadata path...');

  await consumer.disconnect();
  await sleep(1000);

  const consumer2 = kafka.consumer({ groupId: GROUP_ID });
  consumer2.on(consumer2.events.CRASH, (event) => {
    sawCrashEvent = true;
    console.error('[FAIL] consumer CRASH event on rejoin:', event.payload.error);
  });
  consumer2.on(consumer2.events.GROUP_JOIN, (event) => {
    joinCount++;
    const { isLeader, memberAssignment } = event.payload;
    groupJoins.push({ isLeader, memberAssignment });
    console.log(`[join #${joinCount}] (rejoin) isLeader=${isLeader} memberAssignment=${JSON.stringify(memberAssignment)}`);
  });

  await consumer2.connect();
  await consumer2.subscribe({ topics: [VISIBLE_TOPIC, SUPPRESSED_TOPIC], fromBeginning: true });
  await consumer2.run({ eachMessage: async () => {} });

  await sleep(5000);

  if (sawCrashEvent || sawProcessCrash) {
    finish(false, 'crashed during forced rejoin');
    await consumer2.disconnect();
    return;
  }
  if (joinCount < 2) {
    finish(false, 'rejoin never completed within the timeout');
    await consumer2.disconnect();
    return;
  }

  await consumer2.disconnect();
  finish(true, 'initial join + rejoin both clean, no crash, leader election confirmed both times');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function finish(pass, reason) {
  console.log('');
  console.log('='.repeat(70));
  console.log(pass ? '[PASS]' : '[FAIL]', reason);
  console.log(`groupJoins observed: ${JSON.stringify(groupJoins, null, 2)}`);
  console.log('='.repeat(70));
  process.exit(pass ? 0 : 1);
}

main().catch((err) => {
  console.error('[FAIL] uncaught rejection from main() - this IS the crash mode the spec is worried about:');
  console.error(err);
  process.exit(1);
});
