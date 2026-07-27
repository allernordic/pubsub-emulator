import { startServer } from '@aller/pubsub-emulator';
import { PubSub } from '@google-cloud/pubsub';

Feature('prefill topics and subscriptions via the server broker', () => {
  const topicName = 'projects/test-project/topics/seeded';
  const subscriptionName = 'projects/test-project/subscriptions/seeded-worker';

  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  after(async () => {
    await pubsub?.close();
    server?.forceShutdown();
  });

  Scenario('seed topic, subscription and a message before any client connects', () => {
    Given('a started server with a prefilled topic exchange, bound subscription queue and one published message', async () => {
      server = await startServer();

      server.broker.assertExchange(topicName, 'topic');
      server.broker.assertQueue(subscriptionName);
      server.broker.bindQueue(subscriptionName, topicName, '#');
      server.broker.publish(topicName, 'message', Buffer.from('seeded message'));

      expect(server.broker.getQueue(subscriptionName).messageCount).to.equal(1);
    });

    When('a client connects', () => {
      pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });
    });

    Then('the seeded topic is visible to the client', async () => {
      const [topic] = await pubsub.topic('seeded').get();
      expect(topic.name).to.equal(topicName);
    });

    And('the seeded subscription is visible, bound to the seeded topic', async () => {
      const [subscription] = await pubsub.subscription('seeded-worker').get();
      expect(subscription.metadata.topic).to.equal(topicName);
    });

    let message;
    And('the seeded message is delivered to a listener', async () => {
      const subscription = pubsub.subscription('seeded-worker');
      message = await new Promise((resolve, reject) => {
        subscription.on('error', reject);
        subscription.on('message', (msg) => {
          msg.ack();
          resolve(msg);
        });
      });
      await subscription.close();

      expect(message.data.toString()).to.equal('seeded message');
      expect(message.id).to.be.a('string').that.is.not.empty;
    });

    And('the seeded message is gone after ack', () => {
      expect(server.getSubscription(subscriptionName).queue.messageCount).to.equal(0);
    });

    And('creating the seeded topic via the client returns already exists', async () => {
      let error;
      try {
        await pubsub.createTopic('seeded');
      } catch (err) {
        error = err;
      }
      expect(error.code).to.equal(6);
    });
  });
});
