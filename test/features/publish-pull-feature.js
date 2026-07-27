import { startServer } from '@aller/pubsub-emulator';
import { PubSub, v1 } from '@google-cloud/pubsub';
import * as grpc from '@grpc/grpc-js';

Feature('publish and pull', () => {
  const subscriptionName = 'projects/test-project/subscriptions/mail-worker';

  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  /** @type {v1.SubscriberClient} */
  let subscriberClient;
  before('fake pubsub server with topic and subscription', async () => {
    server = await startServer();
    pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });
    subscriberClient = new v1.SubscriberClient({
      servicePath: 'localhost',
      port: server.origin.port,
      sslCreds: grpc.credentials.createInsecure(),
      projectId: 'test-project',
    });

    await pubsub.createTopic('mail');
    await pubsub.topic('mail').createSubscription('mail-worker');
  });
  after(async () => {
    await pubsub.close();
    await subscriberClient.close();
    server?.forceShutdown();
  });

  Scenario('publish messages and pull them in order', () => {
    Given('two published messages', async () => {
      await pubsub.topic('mail').publishMessage({ data: Buffer.from('first'), attributes: { tag: 'greeting' } });
      await pubsub.topic('mail').publishMessage({ data: Buffer.from('second') });
    });

    And('messages are queued on the subscription', () => {
      expect(server.getSubscription(subscriptionName).queue.messageCount).to.equal(2);
    });

    let received;
    When('pulling messages', async () => {
      [{ receivedMessages: received }] = await subscriberClient.pull({ subscription: subscriptionName, maxMessages: 10 });
    });

    Then('both messages are received in published FIFO order', () => {
      expect(received.length).to.equal(2);
      expect(Buffer.from(received[0].message.data).toString()).to.equal('first');
      expect(Buffer.from(received[1].message.data).toString()).to.equal('second');
    });

    And('message attributes, id and publish time are preserved', () => {
      expect(received[0].message.attributes).to.deep.equal({ tag: 'greeting' });
      expect(received[0].message.messageId).to.be.a('string').that.is.not.empty;
      expect(received[0].message.publishTime.seconds).to.be.ok;
    });

    When('acknowledging the first message and nacking the second', async () => {
      await subscriberClient.acknowledge({ subscription: subscriptionName, ackIds: [received[0].ackId] });
      await subscriberClient.modifyAckDeadline({
        subscription: subscriptionName,
        ackIds: [received[1].ackId],
        ackDeadlineSeconds: 0,
      });
    });

    let redelivered;
    Then('only the nacked message is redelivered', async () => {
      [{ receivedMessages: redelivered }] = await subscriberClient.pull({ subscription: subscriptionName, maxMessages: 10 });
      expect(redelivered.length).to.equal(1);
      expect(Buffer.from(redelivered[0].message.data).toString()).to.equal('second');
    });

    And('redelivery is flagged with a bumped delivery attempt', () => {
      expect(redelivered[0].deliveryAttempt).to.equal(2);
    });

    When('acknowledging the redelivered message', async () => {
      await subscriberClient.acknowledge({ subscription: subscriptionName, ackIds: [redelivered[0].ackId] });
    });

    Then('the subscription queue is empty', () => {
      expect(server.getSubscription(subscriptionName).queue.messageCount).to.equal(0);
    });
  });
});
