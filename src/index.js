import { createRequire } from 'node:module';
import path from 'node:path/posix';

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import Debug from 'debug';
import { Broker as SmqpBroker } from 'smqp';

import { RpcCodes } from './rpc-codes.js';

export { RpcCodes } from './rpc-codes.js';
/**
 * Backing message broker, subclassed from smqp so dts-buddy can emit it as a value export,
 * for constructing prefilled brokers to pass to startServer
 */
export class Broker extends SmqpBroker {}

const debug = Debug('aller:pubsub-emulator');

const validTopicNamePattern = /^projects\/[\w-]+\/topics\/[A-Za-z][\w\-.~+%]{2,254}$/;
const validSubscriptionNamePattern = /^projects\/[\w-]+\/subscriptions\/[A-Za-z][\w\-.~+%]{2,254}$/;

/**
 * Per-broker id counters, keyed by broker instance so servers stay isolated
 * @type {WeakMap<Broker, { messageId: number, ackId: number, stream: number }>}
 */
const brokerStates = new WeakMap();

/**
 * @param {Broker} broker
 */
function stateFor(broker) {
  let state = brokerStates.get(broker);
  if (!state) brokerStates.set(broker, (state = { messageId: 0, ackId: 0, stream: 0 }));
  return state;
}

class FakeRpcError extends Error {
  /**
   * @param {string} message
   * @param {number} code
   */
  constructor(message, code) {
    super(message);
    this.code = code;
  }
}

class FakeRpcNotFoundError extends FakeRpcError {
  /**
   * @param {string} resource Topic or Subscription
   * @param {string} name resource name
   */
  constructor(resource, name) {
    super(`${resource} not found: ${name}`, RpcCodes.NOT_FOUND);
  }
}

class FakeRpcInvalidNameError extends FakeRpcError {
  /**
   * @param {string} name resource name
   */
  constructor(name) {
    super(`Invalid resource name given (name=${name}).`, RpcCodes.INVALID_ARGUMENT);
  }
}

/**
 * Get topic resource for an existing exchange, synthesizing defaults for prefilled exchanges
 * @param {Broker} broker
 * @param {string} name topic name
 * @returns {import('@google-cloud/pubsub').protos.google.pubsub.v1.ITopic | undefined}
 */
function getFakeTopic(broker, name) {
  const exchange = broker.getExchange(name);
  if (!exchange) return undefined;

  const options = exchange.options;
  options.autoDelete = false;
  // @ts-ignore arbitrary exchange option
  return (options.topic = options.topic ?? {
    labels: {},
    messageStoragePolicy: null,
    kmsKeyName: '',
    schemaSettings: null,
    satisfiesPzs: false,
    messageRetentionDuration: null,
    name,
  });
}

/**
 * Get subscription with backing queue and outstanding ack registry for an existing
 * queue, synthesizing defaults for prefilled queues
 * @param {Broker} broker
 * @param {string} name subscription name
 * @returns {FakeSubscriptionData | undefined}
 */
function getFakeSubscription(broker, name) {
  const queue = broker.getQueue(name);
  if (!queue) return undefined;

  const options = queue.options;
  options.autoDelete = false;
  // @ts-ignore arbitrary queue option
  const subscription = (options.subscription = options.subscription ?? {
    pushConfig: {},
    labels: {},
    retainAckedMessages: false,
    messageRetentionDuration: null,
    enableMessageOrdering: false,
    expirationPolicy: null,
    filter: '',
    deadLetterPolicy: null,
    retryPolicy: null,
    detached: false,
    ackDeadlineSeconds: 10,
    topic: findBoundTopic(broker, name) ?? '_deleted-topic_',
    name,
  });

  // @ts-ignore arbitrary queue option
  const outstanding = (options.outstanding = options.outstanding ?? new Map());

  return { subscription, outstanding, queue };
}

/**
 * Find name of the exchange a queue is bound to
 * @param {Broker} broker
 * @param {string} queueName
 */
function findBoundTopic(broker, queueName) {
  for (const exchange of broker.getState().exchanges ?? []) {
    for (const binding of exchange.bindings ?? []) {
      if (binding.queueName === queueName) return exchange.name;
    }
  }
}

/**
 * Publish a pubsub message to a topic exchange with a broker-unique message id
 * @param {Broker} broker
 * @param {string} topicName
 * @param {import('@google-cloud/pubsub').protos.google.pubsub.v1.IPubsubMessage} message
 * @returns {string} message id
 */
function publishToTopic(broker, topicName, message) {
  const messageId = (++stateFor(broker).messageId).toString();

  broker.publish(
    topicName,
    message.orderingKey || 'message',
    {
      data: message.data,
      attributes: message.attributes ?? {},
      messageId,
      publishTime: message.publishTime ?? toTimestamp(new Date()),
      orderingKey: message.orderingKey ?? '',
    },
    { messageId }
  );

  return messageId;
}

/**
 * Fake Publisher service implementation
 */
export class FakePublisher {
  /**
   * @param {Broker} broker backing message broker
   * @param {fakeServiceOptions} [options] service options
   */
  constructor(broker, options) {
    this.broker = broker;
    this.autoCreate = options?.autoCreate ?? true;
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  CreateTopic(req, respond) {
    const broker = this.broker;
    const payload = req.request;
    const name = payload.name;

    if (!validTopicNamePattern.test(name)) {
      return respond(new FakeRpcInvalidNameError(name));
    }

    if (broker.getExchange(name)) {
      return respond(new FakeRpcError(`Topic already exists: ${name}`, RpcCodes.ALREADY_EXISTS));
    }

    debug('create topic %s', name);

    /** @type {import('@google-cloud/pubsub').protos.google.pubsub.v1.ITopic} */
    const topic = {
      labels: {},
      messageStoragePolicy: null,
      kmsKeyName: '',
      schemaSettings: null,
      satisfiesPzs: false,
      messageRetentionDuration: null,
      ...payload,
      name,
    };

    broker.assertExchange(name, 'topic', { durable: true, autoDelete: false, topic });

    respond(null, topic);
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  GetTopic(req, respond) {
    const name = req.request.topic;

    if (!validTopicNamePattern.test(name)) {
      return respond(new FakeRpcInvalidNameError(name));
    }

    const topic = getFakeTopic(this.broker, name);
    if (!topic) {
      return respond(new FakeRpcNotFoundError('Topic', name));
    }

    respond(null, { ...topic });
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  ListTopics(req, respond) {
    const project = req.request.project;
    const prefix = `${project}/topics/`;

    const list = [];
    for (const exchange of this.broker.getState().exchanges ?? []) {
      if (!exchange.name.startsWith(prefix)) continue;
      list.push(getFakeTopic(this.broker, exchange.name));
    }

    respond(null, { topics: list, nextPageToken: '' });
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  ListTopicSubscriptions(req, respond) {
    const name = req.request.topic;

    const exchange = this.broker.getExchange(name);
    if (!exchange) {
      return respond(new FakeRpcNotFoundError('Topic', name));
    }

    const list = (exchange.getState().bindings ?? []).map((binding) => binding.queueName);

    respond(null, { subscriptions: list, nextPageToken: '' });
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  DeleteTopic(req, respond) {
    const broker = this.broker;
    const name = req.request.topic;

    const exchange = broker.getExchange(name);
    if (!exchange) {
      return respond(new FakeRpcNotFoundError('Topic', name));
    }

    debug('delete topic %s', name);

    // Detached subscriptions survive their topic, as in the real API
    for (const binding of exchange.getState().bindings ?? []) {
      const fakeSubscription = getFakeSubscription(broker, binding.queueName);
      if (fakeSubscription) fakeSubscription.subscription.topic = '_deleted-topic_';
    }

    broker.deleteExchange(name);

    respond(null, {});
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  Publish(req, respond) {
    const payload = req.request;
    const name = payload.topic;

    if (!this.broker.getExchange(name)) {
      if (!this.autoCreate || !validTopicNamePattern.test(name)) {
        return respond(new FakeRpcNotFoundError('Topic', name));
      }

      debug('auto-create topic %s', name);
      this.broker.assertExchange(name, 'topic', { durable: true, autoDelete: false });
    }

    const messageIds = payload.messages.map(
      /** @param {import('@google-cloud/pubsub').protos.google.pubsub.v1.IPubsubMessage} message */
      (message) => publishToTopic(this.broker, name, message)
    );

    debug('published %d message(s) to %s', messageIds.length, name);

    respond(null, { messageIds });
  }
}

/**
 * Fake Subscriber service implementation
 */
export class FakeSubscriber {
  /**
   * @param {Broker} broker backing message broker
   * @param {fakeServiceOptions} [options] service options
   */
  constructor(broker, options) {
    this.broker = broker;
    this.autoCreate = options?.autoCreate ?? true;
  }

  /**
   * Get subscription, auto-creating it on the fly when enabled: the pull request only
   * carries the subscription name, so the subscription is bound to a same-named topic,
   * auto-created as well. Differently named bindings are a prefill concern
   * @param {string} name subscription name
   */
  _getOrCreateSubscription(name) {
    const broker = this.broker;

    const fakeSubscription = getFakeSubscription(broker, name);
    if (fakeSubscription || !this.autoCreate || !validSubscriptionNamePattern.test(name)) return fakeSubscription;

    const topicName = name.replace('/subscriptions/', '/topics/');

    debug('auto-create subscription %s bound to %s', name, topicName);

    broker.assertExchange(topicName, 'topic', { durable: true, autoDelete: false });
    broker.assertQueue(name, { durable: true, autoDelete: false });
    broker.bindQueue(name, topicName, '#');

    return getFakeSubscription(broker, name);
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  CreateSubscription(req, respond) {
    const broker = this.broker;
    const payload = req.request;
    const name = payload.name;

    if (!validSubscriptionNamePattern.test(name)) {
      return respond(new FakeRpcInvalidNameError(name));
    }

    if (!broker.getExchange(payload.topic)) {
      return respond(new FakeRpcNotFoundError('Topic', payload.topic));
    }

    if (broker.getQueue(name)) {
      return respond(new FakeRpcError(`Subscription already exists: ${name}`, RpcCodes.ALREADY_EXISTS));
    }

    debug('create subscription %s on %s', name, payload.topic);

    /** @type {import('@google-cloud/pubsub').protos.google.pubsub.v1.ISubscription} */
    const subscription = {
      pushConfig: {},
      labels: {},
      retainAckedMessages: false,
      messageRetentionDuration: null,
      enableMessageOrdering: false,
      expirationPolicy: null,
      filter: '',
      deadLetterPolicy: null,
      retryPolicy: null,
      detached: false,
      ...payload,
      ackDeadlineSeconds: payload.ackDeadlineSeconds || 10,
      name,
    };

    broker.assertQueue(name, { durable: true, autoDelete: false, subscription, outstanding: new Map() });
    broker.bindQueue(name, payload.topic, '#');

    respond(null, subscription);
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  GetSubscription(req, respond) {
    const name = req.request.subscription;

    if (!validSubscriptionNamePattern.test(name)) {
      return respond(new FakeRpcInvalidNameError(name));
    }

    const fakeSubscription = getFakeSubscription(this.broker, name);
    if (!fakeSubscription) {
      return respond(new FakeRpcNotFoundError('Subscription', name));
    }

    respond(null, { ...fakeSubscription.subscription });
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  ListSubscriptions(req, respond) {
    const project = req.request.project;
    const prefix = `${project}/subscriptions/`;

    const list = [];
    for (const queue of this.broker.getState().queues ?? []) {
      if (!queue.name.startsWith(prefix)) continue;
      list.push(getFakeSubscription(this.broker, queue.name).subscription);
    }

    respond(null, { subscriptions: list, nextPageToken: '' });
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  DeleteSubscription(req, respond) {
    const name = req.request.subscription;

    if (!this.broker.getQueue(name)) {
      return respond(new FakeRpcNotFoundError('Subscription', name));
    }

    debug('delete subscription %s', name);

    this.broker.deleteQueue(name);

    respond(null, {});
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  Pull(req, respond) {
    const payload = req.request;
    const name = payload.subscription;

    const fakeSubscription = this._getOrCreateSubscription(name);
    if (!fakeSubscription) {
      return respond(new FakeRpcNotFoundError('Subscription', name));
    }

    const maxMessages = payload.maxMessages || 1;

    const receivedMessages = [];
    for (let i = 0; i < maxMessages; i++) {
      const message = fakeSubscription.queue.get();
      if (!message) break;

      receivedMessages.push(this._toReceivedMessage(fakeSubscription, message));
    }

    debug('pulled %d message(s) from %s', receivedMessages.length, name);

    respond(null, { receivedMessages });
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  Acknowledge(req, respond) {
    const payload = req.request;

    const fakeSubscription = getFakeSubscription(this.broker, payload.subscription);
    if (!fakeSubscription) {
      return respond(new FakeRpcNotFoundError('Subscription', payload.subscription));
    }

    debug('unary acknowledge %o', payload.ackIds);
    this._acknowledge(fakeSubscription, payload.ackIds);

    respond(null, {});
  }

  /**
   * @param {FakeRequest} req
   * @param {CallableFunction} respond
   */
  ModifyAckDeadline(req, respond) {
    const payload = req.request;

    const fakeSubscription = getFakeSubscription(this.broker, payload.subscription);
    if (!fakeSubscription) {
      return respond(new FakeRpcNotFoundError('Subscription', payload.subscription));
    }

    debug('unary modifyAckDeadline %o seconds %d', payload.ackIds, payload.ackDeadlineSeconds);
    this._modifyAckDeadline(fakeSubscription, payload.ackIds, payload.ackDeadlineSeconds);

    respond(null, {});
  }

  /**
   * Bidirectional streaming pull, used by the high-level subscription.on('message') client
   * @param {import('@grpc/grpc-js').ServerDuplexStream<any, any>} stream
   */
  StreamingPull(stream) {
    const self = this;
    const broker = this.broker;

    /** @type {FakeSubscriptionData} */
    let fakeSubscription;
    /** @type {import('smqp').Consumer} */
    let consumer;

    // acks and nacks arrive through the unary Acknowledge/ModifyAckDeadline RPCs, the
    // pubsub client (>=4) never sends stream-borne ackIds, so only the initial
    // subscription request is handled here
    stream.on('data', (data) => {
      if (!data.subscription || consumer) return;

      fakeSubscription = self._getOrCreateSubscription(data.subscription);
      if (!fakeSubscription) {
        return stream.emit('error', new FakeRpcNotFoundError('Subscription', data.subscription));
      }

      debug('streaming pull started on %s', data.subscription);

      consumer = broker.consume(
        data.subscription,
        (_routingKey, message) => {
          stream.write({ receivedMessages: [self._toReceivedMessage(fakeSubscription, message)] });
        },
        { consumerTag: `stream-${++stateFor(broker).stream}`, prefetch: data.maxOutstandingMessages || 1000, noAck: false }
      );
    });

    function teardown() {
      if (consumer) {
        debug('streaming pull stopped on %s', fakeSubscription.subscription.name);
        broker.cancel(consumer.consumerTag, true);
        consumer = null;
      }
    }

    stream.on('end', () => {
      teardown();
      stream.end();
    });
    stream.on('cancelled', teardown);
    stream.on('error', teardown);
  }

  /**
   * Map a consumed smqp message to a pubsub received message and register it for ack
   * @param {FakeSubscriptionData} fakeSubscription
   * @param {import('smqp').Message} message
   */
  _toReceivedMessage(fakeSubscription, message) {
    const ackId = (++stateFor(this.broker).ackId).toString();
    fakeSubscription.outstanding.set(ackId, message);

    return {
      ackId,
      message: toPubsubMessage(message),
      deliveryAttempt: deliveryAttempts(message),
    };
  }

  /**
   * Acks may arrive after the delivering stream was torn down and its messages requeued,
   * e.g. when the pubsub client closes streams before flushing acks. The requeued copy is
   * a new smqp message instance, so stale acks are matched by message id instead
   * @param {FakeSubscriptionData} fakeSubscription
   * @param {string[]} ackIds
   */
  _acknowledge(fakeSubscription, ackIds) {
    const { outstanding, queue } = fakeSubscription;

    for (const ackId of ackIds) {
      const message = outstanding.get(ackId);
      if (!message) continue;

      outstanding.delete(ackId);

      if (message.pending) {
        message.ack();
      } else {
        // Message.ack() refuses undelivered messages, queue.ack removes the copy regardless
        const requeuedCopy = queue.messages.find((m) => m.properties.messageId === message.properties.messageId);
        if (requeuedCopy) queue.ack(requeuedCopy);
      }
    }
  }

  /**
   * Nacked messages (deadline 0) are requeued for redelivery, or forwarded to the dead-letter
   * topic when max delivery attempts is exhausted. Other deadlines are ignored since the fake
   * server never expires outstanding messages
   * @param {FakeSubscriptionData} fakeSubscription
   * @param {string[]} ackIds
   * @param {number} ackDeadlineSeconds
   */
  _modifyAckDeadline(fakeSubscription, ackIds, ackDeadlineSeconds) {
    if (ackDeadlineSeconds > 0) return;

    for (const ackId of ackIds) {
      const message = fakeSubscription.outstanding.get(ackId);
      if (!message) continue;

      fakeSubscription.outstanding.delete(ackId);
      // a message that is no longer pending was already requeued when its stream went away
      if (!message.pending) continue;

      if (this._deadLetter(fakeSubscription, message)) {
        message.ack();
      } else {
        message.properties.nackCount = deliveryAttempts(message);
        message.nack(false, true);
      }
    }
  }

  /**
   * Forward a nacked message to the subscription's dead-letter topic when max delivery
   * attempts is exhausted. Forwarding is a new publish: new message id, original attributes
   * kept and dead-letter source attributes added, as in the real API. A missing dead-letter
   * topic keeps the message retrying, also as in the real API
   * @param {FakeSubscriptionData} fakeSubscription
   * @param {import('smqp').Message} message
   * @returns {boolean} true if the message was forwarded
   */
  _deadLetter(fakeSubscription, message) {
    const broker = this.broker;
    const { name, deadLetterPolicy } = fakeSubscription.subscription;
    const deadLetterTopic = deadLetterPolicy?.deadLetterTopic;
    if (!deadLetterTopic) return false;

    const attempts = deliveryAttempts(message);
    if (attempts < (deadLetterPolicy.maxDeliveryAttempts || 5)) return false;

    if (!broker.getExchange(deadLetterTopic)) return false;

    const source = toPubsubMessage(message);

    const messageId = publishToTopic(broker, deadLetterTopic, {
      data: source.data,
      attributes: {
        ...source.attributes,
        CloudPubSubDeadLetterSourceSubscription: name,
        CloudPubSubDeadLetterSourceSubscriptionProject: name.split('/subscriptions/')[0],
        CloudPubSubDeadLetterSourceDeliveryCount: attempts.toString(),
        CloudPubSubDeadLetterSourceTopicPublishTime: toPublishTimeString(source.publishTime),
      },
      orderingKey: source.orderingKey,
    });

    debug('message %s dead-lettered to %s as %s after %d attempts', source.messageId, deadLetterTopic, messageId, attempts);

    return true;
  }
}

/**
 * Delivery attempts so far, counted in nacks. Message properties survive smqp requeue,
 * unlike the message instance itself, so the nack count is tracked there
 * @param {import('smqp').Message} message
 */
function deliveryAttempts(message) {
  return (message.properties.nackCount ?? 0) + 1;
}

/**
 * Map smqp message content to a pubsub message, normalizing raw content published
 * directly via the broker, e.g. `broker.publish(topicName, 'message', Buffer.from('gospel'))`
 * @param {import('smqp').Message} message
 */
function toPubsubMessage(message) {
  const content = message.content;

  if (content && typeof content === 'object' && !Buffer.isBuffer(content) && 'data' in content) {
    return { ...content };
  }

  return {
    data: content === undefined || content === null ? Buffer.alloc(0) : Buffer.from(content),
    attributes: {},
    messageId: message.properties.messageId,
    publishTime: toTimestamp(new Date(message.properties.timestamp)),
    orderingKey: '',
  };
}

/**
 * @param {Date} date
 */
function toTimestamp(date) {
  return {
    nanos: date.getUTCMilliseconds() * 1e6,
    seconds: Math.floor(date.setUTCMilliseconds(0) / 1000),
  };
}

/**
 * @param {{ seconds: number, nanos: number }} publishTime
 */
function toPublishTimeString(publishTime) {
  return new Date(Number(publishTime.seconds) * 1000 + Number(publishTime.nanos ?? 0) / 1e6).toISOString();
}

const require = createRequire(import.meta.url);
const pubsubProtoDir = path.join(path.dirname(require.resolve('@google-cloud/pubsub/package.json')), 'build/protos');
// google-gax does not export its package.json, resolve protos relative to its main entry (build/src/index.js)
const gaxProtoDir = path.join(
  path.dirname(createRequire(require.resolve('@google-cloud/pubsub/package.json')).resolve('google-gax')),
  '../protos'
);

const servicePackageDefinition = protoLoader.loadSync(['google/pubsub/v1/pubsub.proto'], {
  includeDirs: [gaxProtoDir, pubsubProtoDir],
});

const serviceProto = grpc.loadPackageDefinition(servicePackageDefinition);

/**
 * Start fake server with its own broker, or a prefilled one passed in options
 * @param {startServerOptions} [options] Fake gRPC server options
 * @returns {Promise<FakePubSubServer>} Fake gRPC Google Pub/Sub server
 */
export async function startServer(options) {
  const {
    port: requestedPort,
    credentials,
    broker,
    autoCreate,
  } = {
    port: options?.port ?? 0,
    credentials: options?.credentials || grpc.ServerCredentials.createInsecure(),
    broker: options?.broker || new Broker(),
    autoCreate: options?.autoCreate ?? true,
  };

  debug('start server at port %d', requestedPort);
  const server = new grpc.Server();

  // @ts-ignore
  server.addService(serviceProto.google.pubsub.v1.Publisher.service, new FakePublisher(broker, { autoCreate }));
  // @ts-ignore
  server.addService(serviceProto.google.pubsub.v1.Subscriber.service, new FakeSubscriber(broker, { autoCreate }));
  debug('added publisher and subscriber fake implementations');

  const port = await new Promise((resolve, reject) => {
    server.bindAsync(`0.0.0.0:${requestedPort}`, credentials, (err, boundPort) => {
      if (err) {
        return reject(err);
      }
      debug('service started at %d', boundPort);
      resolve(boundPort);
    });
  });

  Object.defineProperties(server, {
    origin: {
      enumerable: true,
      get() {
        return { hostname: 'localhost', port };
      },
    },
    broker: {
      enumerable: true,
      value: broker,
    },
    getTopic: {
      /** @param {string} name topic name */
      value: function getTopic(name) {
        return getFakeTopic(broker, name);
      },
    },
    getSubscription: {
      /** @param {string} name subscription name */
      value: function getSubscription(name) {
        return getFakeSubscription(broker, name);
      },
    },
    forceShutdown: {
      value: function forceShutdown() {
        debug('force shutdown service at %d', port);
        return grpc.Server.prototype.forceShutdown.call(this);
      },
    },
  });

  return /** @type {FakePubSubServer} */ (server);
}

/**
 * @typedef {import('@grpc/grpc-js').Server & {
 *   origin: { hostname: string, port: number },
 *   broker: Broker,
 *   getTopic: (name: string) => import('@google-cloud/pubsub').protos.google.pubsub.v1.ITopic | undefined,
 *   getSubscription: (name: string) => FakeSubscriptionData | undefined,
 * }} FakePubSubServer
 *
 * @typedef {object} startServerOptions
 * @property {number} [port] gRPC server port, defaults to 0 which lets the OS assign a free port
 * @property {import('@grpc/grpc-js').ServerCredentials} [credentials] server credentials, defaults to insecure
 * @property {Broker} [broker] backing message broker, e.g. prefilled with topics and messages, defaults to a new broker
 * @property {boolean} [autoCreate] auto-create topics on publish and subscriptions (bound to a same-named topic) on pull/streaming pull, defaults to true
 *
 * @typedef {object} fakeServiceOptions
 * @property {boolean} [autoCreate] auto-create entities on publish and subscribe, defaults to true
 *
 * @typedef {object} FakeSubscriptionData
 * @property {import('@google-cloud/pubsub').protos.google.pubsub.v1.ISubscription} subscription Subscription
 * @property {Map<string, import('smqp').Message>} outstanding delivered but not yet acked messages by ack id
 * @property {import('smqp').Queue} queue backing smqp queue
 *
 * @typedef {object} FakeRequest
 * @property {any} request request payload
 * @property {import('@grpc/grpc-js').Metadata} metadata request metadata
 */
