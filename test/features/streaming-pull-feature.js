import { startServer } from '@aller/pubsub-emulator';
import { PubSub } from '@google-cloud/pubsub';

Feature('streaming pull', () => {
  const subscriptionName = 'projects/test-project/subscriptions/news-worker';

  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  before('fake pubsub server with topic and subscription', async () => {
    server = await startServer();
    pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });

    await pubsub.createTopic('news');
    await pubsub.topic('news').createSubscription('news-worker');
  });
  after(async () => {
    await pubsub.close();
    server?.forceShutdown();
  });

  Scenario('subscription message events with ack', () => {
    /** @type {import('@google-cloud/pubsub').Subscription} */
    let subscription;
    let waitForMessage;
    Given('a message listener that acks received messages', () => {
      subscription = pubsub.subscription('news-worker');
      waitForMessage = new Promise((resolve, reject) => {
        subscription.on('error', reject);
        subscription.on('message', (message) => {
          message.ack();
          resolve(message);
        });
      });
    });

    When('a message is published', async () => {
      await pubsub.topic('news').publishMessage({ data: Buffer.from('breaking'), attributes: { prio: 'high' } });
    });

    let message;
    Then('the listener receives the message', async () => {
      message = await waitForMessage;
      expect(message.data.toString()).to.equal('breaking');
      expect(message.attributes).to.deep.equal({ prio: 'high' });
      expect(message.id).to.be.a('string').that.is.not.empty;
    });

    When('the listener is closed', async () => {
      await subscription.close();
    });

    Then('no message remains on the subscription', () => {
      const fake = server.getSubscription(subscriptionName);
      expect(fake.queue.messageCount, 'queued message count').to.equal(0);
    });
  });

  Scenario('nacked message is redelivered', () => {
    /** @type {import('@google-cloud/pubsub').Subscription} */
    let subscription;
    const seen = [];
    let waitForRedelivery;
    Given('a message listener that nacks the first delivery and acks the redelivery', () => {
      subscription = pubsub.subscription('news-worker');
      waitForRedelivery = new Promise((resolve, reject) => {
        subscription.on('error', reject);
        subscription.on('message', (message) => {
          seen.push(message);
          if (seen.length === 1) return message.nack();
          message.ack();
          resolve(message);
        });
      });
    });

    When('a message is published', async () => {
      await pubsub.topic('news').publishMessage({ data: Buffer.from('rumour') });
    });

    let redelivered;
    Then('the message is redelivered after nack and acked', async () => {
      redelivered = await waitForRedelivery;
      expect(redelivered.data.toString()).to.equal('rumour');
      expect(seen.length).to.equal(2);
    });

    When('the listener is closed', async () => {
      await subscription.close();
    });

    Then('no message remains on the subscription', () => {
      const fake = server.getSubscription(subscriptionName);
      expect(fake.queue.messageCount, 'queued message count').to.equal(0);
    });
  });

  Scenario('closing a listener requeues unacked messages', () => {
    /** @type {import('@google-cloud/pubsub').Subscription} */
    let subscription;
    let waitForMessage;
    Given('a message listener that never acks', () => {
      subscription = pubsub.subscription('news-worker');
      waitForMessage = new Promise((resolve, reject) => {
        subscription.on('error', reject);
        subscription.on('message', resolve);
      });
    });

    When('a message is published and received but not acked', async () => {
      await pubsub.topic('news').publishMessage({ data: Buffer.from('limbo') });
      await waitForMessage;
    });

    And('the listener is closed', async () => {
      await subscription.close();
    });

    Then('the message is back on the subscription queue awaiting redelivery', () => {
      const fake = server.getSubscription(subscriptionName);
      expect(fake.queue.messageCount, 'queued message count').to.equal(1);
    });
  });
});
