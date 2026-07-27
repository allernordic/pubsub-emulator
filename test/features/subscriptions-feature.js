import { startServer } from '@aller/pubsub-emulator';
import { PubSub } from '@google-cloud/pubsub';

Feature('subscription management', () => {
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  before('fake pubsub server', async () => {
    server = await startServer();
    pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });
  });
  after(async () => {
    await pubsub.close();
    server?.forceShutdown();
  });

  Scenario('create a subscription', () => {
    Given('a topic', async () => {
      await pubsub.createTopic('orders');
    });

    let subscription;
    When('creating a subscription on the topic', async () => {
      [subscription] = await pubsub.topic('orders').createSubscription('order-worker');
    });

    Then('subscription is created with full resource name and defaults', () => {
      expect(subscription.name).to.equal('projects/test-project/subscriptions/order-worker');
      expect(subscription.metadata.topic).to.equal('projects/test-project/topics/orders');
      expect(subscription.metadata.ackDeadlineSeconds).to.equal(10);
    });

    And('a backing queue is bound to the topic', () => {
      const fake = server.getSubscription('projects/test-project/subscriptions/order-worker');
      expect(fake, 'fake subscription').to.be.ok;
      expect(fake.queue.messageCount).to.equal(0);
    });

    let error;
    When('creating the same subscription again', async () => {
      try {
        await pubsub.topic('orders').createSubscription('order-worker');
      } catch (err) {
        error = err;
      }
    });

    Then('an already exists error is returned', () => {
      expect(error.code).to.equal(6);
    });
  });

  Scenario('subscription on missing topic', () => {
    let error;
    When('creating a subscription on a topic that does not exist', async () => {
      try {
        await pubsub.topic('nope').createSubscription('never-mind');
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });
  });

  Scenario('list subscriptions', () => {
    Given('a second subscription', async () => {
      await pubsub.topic('orders').createSubscription('order-audit');
    });

    let subscriptions;
    When('listing subscriptions', async () => {
      [subscriptions] = await pubsub.getSubscriptions();
    });

    Then('all subscriptions are listed', () => {
      const names = subscriptions.map((s) => s.name);
      expect(names).to.include('projects/test-project/subscriptions/order-worker');
      expect(names).to.include('projects/test-project/subscriptions/order-audit');
    });

    let topicSubscriptions;
    And('subscriptions can be listed by topic', async () => {
      [topicSubscriptions] = await pubsub.topic('orders').getSubscriptions();
      expect(topicSubscriptions.length).to.equal(2);
    });
  });

  Scenario('delete a subscription', () => {
    When('deleting a subscription', async () => {
      await pubsub.subscription('order-audit').delete();
    });

    let error;
    Then('fetching the deleted subscription returns not found', async () => {
      try {
        await pubsub.subscription('order-audit').get();
      } catch (err) {
        error = err;
      }
      expect(error.code).to.equal(5);
    });
  });

  Scenario('deleting a topic detaches its subscriptions', () => {
    When('the topic is deleted', async () => {
      await pubsub.topic('orders').delete();
    });

    let subscription;
    Then('remaining subscription survives pointing to _deleted-topic_', async () => {
      [subscription] = await pubsub.subscription('order-worker').get();
      expect(subscription.metadata.topic).to.equal('_deleted-topic_');
    });
  });
});
