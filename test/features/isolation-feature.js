import { Broker, startServer } from '@aller/pubsub-emulator';
import { PubSub } from '@google-cloud/pubsub';

Feature('server isolation', () => {
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server1;
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server2;
  /** @type {PubSub} */
  let pubsub1;
  /** @type {PubSub} */
  let pubsub2;
  after(async () => {
    await pubsub1?.close();
    await pubsub2?.close();
    server1?.forceShutdown();
    server2?.forceShutdown();
  });

  Scenario('two servers get separate brokers', () => {
    Given('two running fake servers', async () => {
      server1 = await startServer();
      server2 = await startServer();

      pubsub1 = new PubSub({ apiEndpoint: `localhost:${server1.origin.port}`, projectId: 'test-project' });
      pubsub2 = new PubSub({ apiEndpoint: `localhost:${server2.origin.port}`, projectId: 'test-project' });
    });

    Then('each server exposes its own broker', () => {
      expect(server1.broker, 'server1.broker').to.be.ok;
      expect(server2.broker, 'server2.broker').to.be.ok;
      expect(server1.broker).to.not.equal(server2.broker);
    });

    When('a topic is created on the first server', async () => {
      await pubsub1.createTopic('solitary');
    });

    Then('the topic is not visible on the second server', async () => {
      let error;
      try {
        await pubsub2.topic('solitary').get();
      } catch (err) {
        error = err;
      }
      expect(error.code).to.equal(5);
    });

    And('the same topic name can be created on the second server', async () => {
      const [topic] = await pubsub2.createTopic('solitary');
      expect(topic.name).to.equal('projects/test-project/topics/solitary');
    });

    And('messages published on the first server stay there', async () => {
      await pubsub1.topic('solitary').createSubscription('solitary-worker');
      await pubsub1.topic('solitary').publishMessage({ data: Buffer.from('lonely') });

      expect(server1.getSubscription('projects/test-project/subscriptions/solitary-worker').queue.messageCount).to.equal(1);
      expect(server2.getSubscription('projects/test-project/subscriptions/solitary-worker')).to.be.undefined;
    });
  });

  Scenario('a prefilled broker is passed to the server', () => {
    /** @type {Broker} */
    let broker;
    Given('a broker seeded with a topic, subscription and message', () => {
      broker = new Broker();
      broker.assertExchange('projects/test-project/topics/seeded', 'topic');
      broker.assertQueue('projects/test-project/subscriptions/seeded-worker');
      broker.bindQueue('projects/test-project/subscriptions/seeded-worker', 'projects/test-project/topics/seeded', '#');
      broker.publish('projects/test-project/topics/seeded', 'message', Buffer.from('seeded message'));
    });

    /** @type {Awaited<ReturnType<typeof startServer>>} */
    let server;
    /** @type {PubSub} */
    let pubsub;
    When('a server is started with the broker', async () => {
      server = await startServer({ broker });
      pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });
    });

    Then('the server uses that broker', () => {
      expect(server.broker).to.equal(broker);
    });

    And('the seeded message is delivered to a listener', async () => {
      const subscription = pubsub.subscription('seeded-worker');
      const message = await new Promise((resolve, reject) => {
        subscription.on('error', reject);
        subscription.on('message', (msg) => {
          msg.ack();
          resolve(msg);
        });
      });
      await subscription.close();
      await pubsub.close();
      server.forceShutdown();

      expect(message.data.toString()).to.equal('seeded message');
    });
  });
});
