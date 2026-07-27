import { Broker, FakePublisher, FakeSubscriber, startServer } from '@aller/pubsub-emulator';
import { PubSub, v1 } from '@google-cloud/pubsub';
import * as grpc from '@grpc/grpc-js';

Feature('api validation and edge cases', () => {
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let server;
  /** @type {PubSub} */
  let pubsub;
  /** @type {v1.SubscriberClient} */
  let subscriberClient;
  before('fake pubsub server without auto-create', async () => {
    server = await startServer({ autoCreate: false });
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

  Scenario('fresh server is empty', () => {
    Then('listing topics returns an empty list', async () => {
      const [topics] = await pubsub.getTopics();
      expect(topics).to.deep.equal([]);
    });

    And('listing subscriptions returns an empty list', async () => {
      const [subscriptions] = await pubsub.getSubscriptions();
      expect(subscriptions).to.deep.equal([]);
    });

    When('a queue is asserted without binding it to any exchange', () => {
      server.broker.assertQueue('projects/test-project/subscriptions/orphan-worker');
    });

    Then('the synthesized subscription points to _deleted-topic_', async () => {
      const [subscription] = await pubsub.subscription('orphan-worker').get();
      expect(subscription.metadata.topic).to.equal('_deleted-topic_');
    });
  });

  Scenario('invalid resource names', () => {
    let error;
    When('getting a topic with an invalid name', async () => {
      try {
        await pubsub.topic('bad topic name!').get();
      } catch (err) {
        error = err;
      }
    });

    Then('an invalid argument error is returned', () => {
      expect(error.code).to.equal(3);
    });

    When('creating a subscription with an invalid name', async () => {
      await pubsub.createTopic('valid');
      try {
        await pubsub.topic('valid').createSubscription('bad sub name!');
      } catch (err) {
        error = err;
      }
    });

    Then('an invalid argument error is returned', () => {
      expect(error.code).to.equal(3);
    });

    When('getting a subscription with an invalid name', async () => {
      try {
        await pubsub.subscription('bad sub name!').get();
      } catch (err) {
        error = err;
      }
    });

    Then('an invalid argument error is returned', () => {
      expect(error.code).to.equal(3);
    });
  });

  Scenario('operations on missing resources', () => {
    let error;
    When('publishing to a topic that does not exist', async () => {
      try {
        await pubsub.topic('ghost').publishMessage({ data: Buffer.from('boo') });
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });

    When('listing subscriptions of a topic that does not exist', async () => {
      try {
        await pubsub.topic('ghost').getSubscriptions();
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });

    When('deleting a subscription that does not exist', async () => {
      try {
        await pubsub.subscription('ghost-worker').delete();
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });

    When('acknowledging on a subscription that does not exist', async () => {
      try {
        await subscriberClient.acknowledge({ subscription: 'projects/test-project/subscriptions/ghost-worker', ackIds: ['1'] });
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });

    When('modifying ack deadline on a subscription that does not exist', async () => {
      try {
        await subscriberClient.modifyAckDeadline({
          subscription: 'projects/test-project/subscriptions/ghost-worker',
          ackIds: ['1'],
          ackDeadlineSeconds: 0,
        });
      } catch (err) {
        error = err;
      }
    });

    Then('a not found error is returned', () => {
      expect(error.code).to.equal(5);
    });

    let streamError;
    When('listening on a subscription that does not exist', async () => {
      const subscription = pubsub.subscription('ghost-worker');
      streamError = await new Promise((resolve) => {
        subscription.on('error', resolve);
        subscription.on('message', () => {});
      });
      await subscription.close();
    });

    Then('the listener emits not found', () => {
      expect(streamError.code).to.equal(5);
    });
  });

  Scenario('unknown ack ids are ignored', () => {
    const subscriptionName = 'projects/test-project/subscriptions/valid-worker';
    let received;
    Given('a subscription with a pulled message', async () => {
      await pubsub.topic('valid').createSubscription('valid-worker');
      await pubsub.topic('valid').publishMessage({ data: Buffer.from('for real') });
      [
        {
          receivedMessages: [received],
        },
      ] = await subscriberClient.pull({ subscription: subscriptionName, maxMessages: 1 });
    });

    When('acknowledging and nacking unknown ack ids', async () => {
      await subscriberClient.acknowledge({ subscription: subscriptionName, ackIds: ['bogus'] });
      await subscriberClient.modifyAckDeadline({ subscription: subscriptionName, ackIds: ['bogus'], ackDeadlineSeconds: 0 });
    });

    Then('the pulled message is still outstanding and can be acked', async () => {
      expect(server.getSubscription(subscriptionName).outstanding.size).to.equal(1);

      await subscriberClient.acknowledge({ subscription: subscriptionName, ackIds: [received.ackId] });
      expect(server.getSubscription(subscriptionName).queue.messageCount).to.equal(0);
    });
  });

  Scenario('listing subscriptions of a topic without subscriptions', () => {
    let subscriptions;
    When('listing subscriptions of a topic that has none', async () => {
      await pubsub.createTopic('lonely');
      [subscriptions] = await pubsub.topic('lonely').getSubscriptions();
    });

    Then('an empty list is returned', () => {
      expect(subscriptions).to.deep.equal([]);
    });
  });

  Scenario('foreign project resources are not listed', () => {
    Given('a prefilled topic and queue belonging to another project', () => {
      server.broker.assertExchange('projects/other-project/topics/foreign', 'topic');
      server.broker.assertQueue('projects/other-project/subscriptions/foreign-worker');
    });

    Then('listing topics excludes the foreign topic', async () => {
      const [topics] = await pubsub.getTopics();
      expect(topics.map((t) => t.name)).to.not.include('projects/other-project/topics/foreign');
      expect(topics.length).to.be.at.least(1);
    });

    And('listing subscriptions excludes the foreign queue', async () => {
      const [subscriptions] = await pubsub.getSubscriptions();
      expect(subscriptions.map((s) => s.name)).to.not.include('projects/other-project/subscriptions/foreign-worker');
    });

    And('an unbound queue among existing exchanges still synthesizes _deleted-topic_', async () => {
      server.broker.assertQueue('projects/test-project/subscriptions/unbound-worker');
      const [subscription] = await pubsub.subscription('unbound-worker').get();
      expect(subscription.metadata.topic).to.equal('_deleted-topic_');
    });
  });

  Scenario('pull without max messages', () => {
    const subscriptionName = 'projects/test-project/subscriptions/single-worker';
    Given('a subscription with two published messages', async () => {
      await pubsub.topic('valid').createSubscription('single-worker');
      await pubsub.topic('valid').publishMessage({ data: Buffer.from('one') });
      await pubsub.topic('valid').publishMessage({ data: Buffer.from('two') });
    });

    let received;
    When('pulling without max messages', async () => {
      [{ receivedMessages: received }] = await subscriberClient.pull({ subscription: subscriptionName });
    });

    Then('a single message is returned', () => {
      expect(received.length).to.equal(1);
      expect(Buffer.from(received[0].message.data).toString()).to.equal('one');
    });
  });

  Scenario('raw broker publish without content', () => {
    const subscriptionName = 'projects/test-project/subscriptions/lonely-worker';
    Given('a subscription and a raw broker publish without content', async () => {
      await pubsub.topic('lonely').createSubscription('lonely-worker');
      server.broker.publish('projects/test-project/topics/lonely', 'message');
    });

    let received;
    When('pulling the message', async () => {
      [
        {
          receivedMessages: [received],
        },
      ] = await subscriberClient.pull({ subscription: subscriptionName, maxMessages: 1 });
    });

    Then('the message has empty data and a message id', async () => {
      expect(Buffer.from(received.message.data).length).to.equal(0);
      expect(received.message.messageId).to.be.a('string').that.is.not.empty;

      await subscriberClient.acknowledge({ subscription: subscriptionName, ackIds: [received.ackId] });
    });
  });

  Scenario('server inspection helpers', () => {
    Then('server.getTopic returns the topic resource', () => {
      expect(server.getTopic('projects/test-project/topics/valid').name).to.equal('projects/test-project/topics/valid');
      expect(server.getTopic('projects/test-project/topics/ghost')).to.be.undefined;
    });
  });

  Scenario('fake service classes constructed without options', () => {
    Then('auto-create defaults to on', () => {
      const broker = new Broker();
      const publisher = new FakePublisher(broker);
      const subscriber = new FakeSubscriber(broker);

      expect(publisher.autoCreate).to.be.true;
      expect(subscriber.autoCreate).to.be.true;
    });
  });

  Scenario('occupied port', () => {
    let error;
    When('starting a server on a port that is taken', async () => {
      try {
        await startServer({ port: server.origin.port });
      } catch (err) {
        error = err;
      }
    });

    Then('starting the server fails', () => {
      expect(error).to.be.an('error');
    });
  });
});
