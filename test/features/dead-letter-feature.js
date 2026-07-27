import { startServer } from '@aller/pubsub-emulator';
import { PubSub, v1 } from '@google-cloud/pubsub';
import * as grpc from '@grpc/grpc-js';

Feature('dead-letter policy', () => {
  const subscriptionName = 'projects/test-project/subscriptions/jobs-worker';
  const morgueSubscriptionName = 'projects/test-project/subscriptions/morgue-worker';
  const deadLetterTopicName = 'projects/test-project/topics/jobs-morgue';

  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  /** @type {v1.SubscriberClient} */
  let subscriberClient;
  before('fake pubsub server with a dead-letter topic and subscriptions', async () => {
    server = await startServer();
    pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });
    subscriberClient = new v1.SubscriberClient({
      servicePath: 'localhost',
      port: server.origin.port,
      sslCreds: grpc.credentials.createInsecure(),
      projectId: 'test-project',
    });

    await pubsub.createTopic('jobs');
    await pubsub.createTopic('jobs-morgue');
    await pubsub.topic('jobs-morgue').createSubscription('morgue-worker');
  });
  after(async () => {
    await pubsub.close();
    await subscriberClient.close();
    server?.forceShutdown();
  });

  /**
   * Pull one message and nack it
   * @param {string} subscription
   */
  async function pullAndNack(subscription) {
    const [{ receivedMessages }] = await subscriberClient.pull({ subscription, maxMessages: 1 });
    if (!receivedMessages.length) return undefined;

    await subscriberClient.modifyAckDeadline({ subscription, ackIds: [receivedMessages[0].ackId], ackDeadlineSeconds: 0 });
    return receivedMessages[0];
  }

  Scenario('message exceeding max delivery attempts is forwarded to the dead-letter topic', () => {
    Given('a subscription with a dead-letter policy', async () => {
      const [subscription] = await pubsub.topic('jobs').createSubscription('jobs-worker', {
        deadLetterPolicy: { deadLetterTopic: deadLetterTopicName, maxDeliveryAttempts: 5 },
      });

      expect(subscription.metadata.deadLetterPolicy).to.deep.include({ deadLetterTopic: deadLetterTopicName, maxDeliveryAttempts: 5 });
    });

    And('a published message', async () => {
      await pubsub.topic('jobs').publishMessage({ data: Buffer.from('poison'), attributes: { flavor: 'bitter' } });
    });

    const deliveries = [];
    When('the message is pulled and nacked until max delivery attempts is reached', async () => {
      for (let i = 0; i < 5; i++) {
        deliveries.push(await pullAndNack(subscriptionName));
      }
    });

    Then('each delivery reported an incrementing delivery attempt', () => {
      expect(deliveries.map((d) => d.deliveryAttempt)).to.deep.equal([1, 2, 3, 4, 5]);
    });

    And('the message is no longer redelivered on the subscription', async () => {
      const [{ receivedMessages }] = await subscriberClient.pull({
        subscription: subscriptionName,
        maxMessages: 1,
        returnImmediately: true,
      });
      expect(receivedMessages.length).to.equal(0);
    });

    let deadLettered;
    And('the message arrived on the dead-letter topic subscription', async () => {
      const [{ receivedMessages }] = await subscriberClient.pull({ subscription: morgueSubscriptionName, maxMessages: 1 });
      expect(receivedMessages.length).to.equal(1);
      deadLettered = receivedMessages[0];

      expect(Buffer.from(deadLettered.message.data).toString()).to.equal('poison');
    });

    And('original attributes are kept and dead-letter source attributes added', () => {
      expect(deadLettered.message.attributes).to.deep.include({
        flavor: 'bitter',
        CloudPubSubDeadLetterSourceSubscription: subscriptionName,
        CloudPubSubDeadLetterSourceSubscriptionProject: 'projects/test-project',
        CloudPubSubDeadLetterSourceDeliveryCount: '5',
      });
      expect(deadLettered.message.attributes.CloudPubSubDeadLetterSourceTopicPublishTime).to.be.a('string').that.is.not.empty;
    });

    And('the forwarded message got a new message id', () => {
      expect(deadLettered.message.messageId).to.be.a('string').that.does.not.equal(deliveries[0].message.messageId);
    });
  });

  Scenario('subscription without dead-letter policy redelivers forever', () => {
    Given('a subscription without dead-letter policy and a published message', async () => {
      await pubsub.topic('jobs').createSubscription('jobs-audit');
      await pubsub.topic('jobs').publishMessage({ data: Buffer.from('sticky') });
    });

    let lastDelivery;
    When('the message is pulled and nacked more times than any max delivery attempts', async () => {
      for (let i = 0; i < 7; i++) {
        lastDelivery = await pullAndNack('projects/test-project/subscriptions/jobs-audit');
      }
    });

    Then('the message is still redelivered with an ever increasing delivery attempt', async () => {
      expect(lastDelivery.deliveryAttempt).to.equal(7);

      const [{ receivedMessages }] = await subscriberClient.pull({
        subscription: 'projects/test-project/subscriptions/jobs-audit',
        maxMessages: 1,
      });
      expect(receivedMessages.length).to.equal(1);
      expect(receivedMessages[0].deliveryAttempt).to.equal(8);
    });
  });

  Scenario('dead-letter policy without max delivery attempts defaults to 5', () => {
    Given('a subscription with a dead-letter policy without max delivery attempts, and a published message', async () => {
      await pubsub.topic('jobs').createSubscription('jobs-defaulted', {
        deadLetterPolicy: { deadLetterTopic: deadLetterTopicName },
      });
      await pubsub.topic('jobs').publishMessage({ data: Buffer.from('defaulted') });
    });

    When('the message is pulled and nacked five times', async () => {
      for (let i = 0; i < 5; i++) {
        await pullAndNack('projects/test-project/subscriptions/jobs-defaulted');
      }
    });

    Then('the message was forwarded to the dead-letter topic', async () => {
      const [{ receivedMessages }] = await subscriberClient.pull({ subscription: morgueSubscriptionName, maxMessages: 1 });
      expect(receivedMessages.length).to.equal(1);
      expect(Buffer.from(receivedMessages[0].message.data).toString()).to.equal('defaulted');
      expect(receivedMessages[0].message.attributes.CloudPubSubDeadLetterSourceDeliveryCount).to.equal('5');
    });
  });

  Scenario('dead-letter topic that does not exist keeps the message retrying', () => {
    Given('a subscription with a dead-letter policy pointing to a missing topic, and a published message', async () => {
      await pubsub.topic('jobs').createSubscription('jobs-doomed', {
        deadLetterPolicy: { deadLetterTopic: 'projects/test-project/topics/void', maxDeliveryAttempts: 5 },
      });
      await pubsub.topic('jobs').publishMessage({ data: Buffer.from('stuck') });
    });

    When('the message is pulled and nacked past max delivery attempts', async () => {
      for (let i = 0; i < 6; i++) {
        await pullAndNack('projects/test-project/subscriptions/jobs-doomed');
      }
    });

    Then('the message is still redelivered', async () => {
      const [{ receivedMessages }] = await subscriberClient.pull({
        subscription: 'projects/test-project/subscriptions/jobs-doomed',
        maxMessages: 1,
      });
      expect(receivedMessages.length).to.equal(1);
      expect(Buffer.from(receivedMessages[0].message.data).toString()).to.equal('stuck');
    });
  });
});
