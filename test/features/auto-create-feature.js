import { startServer } from '@aller/pubsub-emulator';
import { PubSub, v1 } from '@google-cloud/pubsub';
import * as grpc from '@grpc/grpc-js';

Feature('auto-create entities', () => {
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  /** @type {v1.SubscriberClient} */
  let subscriberClient;
  before('fake pubsub server with default options', async () => {
    server = await startServer();
    pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });
    subscriberClient = new v1.SubscriberClient({
      servicePath: 'localhost',
      port: server.origin.port,
      sslCreds: grpc.credentials.createInsecure(),
      projectId: 'test-project',
    });
  });
  after(async () => {
    await pubsub.close();
    await subscriberClient.close();
    server?.forceShutdown();
  });

  Scenario('publish to a topic that does not exist', () => {
    let messageIds;
    When('publishing to a topic that was never created', async () => {
      messageIds = await pubsub.topic('order-events').publishMessage({ data: Buffer.from('auto') });
    });

    Then('the publish succeeds and the topic now exists', () => {
      expect(messageIds).to.be.ok;
      expect(server.getTopic('projects/test-project/topics/order-events').name).to.equal('projects/test-project/topics/order-events');
    });
  });

  Scenario('streaming subscribe to a subscription that does not exist', () => {
    /** @type {import('@google-cloud/pubsub').Subscription} */
    let subscription;
    let waitForMessage;
    Given('a listener on a subscription that was never created', () => {
      subscription = pubsub.subscription('news-events');
      waitForMessage = new Promise((resolve, reject) => {
        subscription.on('error', reject);
        subscription.on('message', (message) => {
          message.ack();
          resolve(message);
        });
      });
    });

    Then('the subscription is auto-created, bound to a same-named auto-created topic', async () => {
      // the client opens its streams in the background, await the first one reaching the server
      let fake;
      for (let i = 0; i < 100 && !(fake = server.getSubscription('projects/test-project/subscriptions/news-events')); i++) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }

      expect(fake, 'auto-created subscription').to.be.ok;
      expect(fake.subscription.topic).to.equal('projects/test-project/topics/news-events');
      expect(server.getTopic('projects/test-project/topics/news-events'), 'same-named topic').to.be.ok;

      const [exists] = await pubsub.subscription('news-events').exists();
      expect(exists, 'subscription exists').to.be.true;
    });

    let message;
    When('a message is published to the same-named topic', async () => {
      await pubsub.topic('news-events').publishMessage({ data: Buffer.from('breaking') });
      message = await waitForMessage;
      await subscription.close();
    });

    Then('the message is delivered to the listener', () => {
      expect(message.data.toString()).to.equal('breaking');
    });
  });

  Scenario('unary pull from a subscription that does not exist', () => {
    const subscriptionName = 'projects/test-project/subscriptions/job-events';

    let received;
    When('pulling from a subscription that was never created', async () => {
      [{ receivedMessages: received }] = await subscriberClient.pull({ subscription: subscriptionName, maxMessages: 1 });
    });

    Then('the pull succeeds with no messages and the subscription now exists', () => {
      expect(received).to.deep.equal([]);
      expect(server.getSubscription(subscriptionName).subscription.topic).to.equal('projects/test-project/topics/job-events');
    });

    When('a message is published to the same-named topic', async () => {
      await pubsub.topic('job-events').publishMessage({ data: Buffer.from('work') });
    });

    Then('the message can be pulled', async () => {
      const [{ receivedMessages }] = await subscriberClient.pull({ subscription: subscriptionName, maxMessages: 1 });
      expect(receivedMessages.length).to.equal(1);
      expect(Buffer.from(receivedMessages[0].message.data).toString()).to.equal('work');
      await subscriberClient.acknowledge({ subscription: subscriptionName, ackIds: [receivedMessages[0].ackId] });
    });
  });

  Scenario('auto-create can be disabled', () => {
    /** @type {Awaited<ReturnType<typeof startServer>>} */
    let strictServer;
    /** @type {PubSub} */
    let strictPubsub;
    Given('a server started with auto-create disabled', async () => {
      strictServer = await startServer({ autoCreate: false });
      strictPubsub = new PubSub({ apiEndpoint: `localhost:${strictServer.origin.port}`, projectId: 'test-project' });
    });

    let error;
    When('publishing to a topic that does not exist', async () => {
      try {
        await strictPubsub.topic('order-events').publishMessage({ data: Buffer.from('strict') });
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', async () => {
      expect(error.code).to.equal(5);

      await strictPubsub.close();
      strictServer.forceShutdown();
    });
  });

  Scenario('invalid names are not auto-created', () => {
    let error;
    When('pulling with an invalid subscription name', async () => {
      try {
        await subscriberClient.pull({ subscription: 'projects/test-project/subscriptions/bad name!', maxMessages: 1 });
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });

    When('publishing with an invalid topic name', async () => {
      // the high-level client normalizes names client-side, go through the api directly
      const publisherClient = new v1.PublisherClient({
        servicePath: 'localhost',
        port: server.origin.port,
        sslCreds: grpc.credentials.createInsecure(),
        projectId: 'test-project',
      });
      try {
        await publisherClient.publish({ topic: 'projects/test-project/topics/bad name!', messages: [{ data: Buffer.from('x') }] });
      } catch (err) {
        error = err;
      } finally {
        await publisherClient.close();
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });
  });
});
