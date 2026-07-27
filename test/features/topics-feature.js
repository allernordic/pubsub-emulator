import { startServer } from '@aller/pubsub-emulator';
import { PubSub } from '@google-cloud/pubsub';

Feature('topic management', () => {
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

  Scenario('create a topic', () => {
    let topic;
    When('creating a topic', async () => {
      [topic] = await pubsub.createTopic('my-topic');
    });

    Then('topic is created with full resource name', () => {
      expect(topic.name).to.equal('projects/test-project/topics/my-topic');
    });

    let fetched;
    And('topic can be fetched', async () => {
      [fetched] = await pubsub.topic('my-topic').get();
      expect(fetched.name).to.equal('projects/test-project/topics/my-topic');
    });

    When('creating the same topic again', async () => {
      try {
        await pubsub.createTopic('my-topic');
      } catch (err) {
        fetched = err;
      }
    });

    Then('an already exists error is returned', () => {
      expect(fetched.code).to.equal(6);
      expect(fetched.message).to.match(/already exists/i);
    });
  });

  Scenario('create a topic with invalid name', () => {
    let error;
    When('creating a topic with bad characters', async () => {
      try {
        await pubsub.createTopic('bad topic name!');
      } catch (err) {
        error = err;
      }
    });

    Then('an invalid argument error is returned', () => {
      expect(error.code).to.equal(3);
    });
  });

  Scenario('list topics', () => {
    Given('another topic exists', async () => {
      await pubsub.createTopic('my-other-topic');
    });

    let topics;
    When('listing topics', async () => {
      [topics] = await pubsub.getTopics();
    });

    Then('all created topics are listed', () => {
      const names = topics.map((t) => t.name);
      expect(names).to.include('projects/test-project/topics/my-topic');
      expect(names).to.include('projects/test-project/topics/my-other-topic');
    });
  });

  Scenario('delete a topic', () => {
    When('deleting the topic', async () => {
      await pubsub.topic('my-other-topic').delete();
    });

    let error;
    Then('fetching the deleted topic returns not found', async () => {
      try {
        await pubsub.topic('my-other-topic').get();
      } catch (err) {
        error = err;
      }
      expect(error.code).to.equal(5);
    });

    And('deleting a non-existing topic returns not found', async () => {
      let err2;
      try {
        await pubsub.topic('never-created').delete();
      } catch (err) {
        err2 = err;
      }
      expect(err2.code).to.equal(5);
    });
  });
});
