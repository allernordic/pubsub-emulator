# @aller/pubsub-emulator

In-process [Google Cloud Pub/Sub](https://cloud.google.com/pubsub) emulator for tests. Run the real `@google-cloud/pubsub` client against a local gRPC server — no Java, no gcloud SDK, no Docker, no credentials, no TLS.

Message state is backed by an [smqp](https://github.com/paed01/smqp) broker: topics are exchanges, subscriptions are FIFO queues, so ordering, ack/nack and redelivery behave predictably — and the broker is exported so tests can prefill state directly.

[![Build](https://github.com/allernordic/pubsub-emulator/actions/workflows/build.yaml/badge.svg)](https://github.com/allernordic/pubsub-emulator/actions/workflows/build.yaml)

<!-- toc -->

- [Features](#features)
- [Versus the official emulator](#versus-the-official-emulator)
- [Usage](#usage)
  - [Prefill via the broker](#prefill-via-the-broker)
  - [Inspecting fake state](#inspecting-fake-state)
  - [API](#api)
- [Test](#test)

<!-- /toc -->

## Features

- `Publisher` service: create/get/list/delete topics, publish
- `Subscriber` service: create/get/list/delete subscriptions, unary pull, acknowledge, modifyAckDeadline (deadline `0` = nack + redelivery), streaming pull (`subscription.on('message')`)
- Real `pubsub.proto` serialization, real gRPC status codes (`NOT_FOUND`, `ALREADY_EXISTS`, `INVALID_ARGUMENT`, …)
- Unacked messages are requeued when a stream closes; late acks (flushed after stream close, as the Node client does) still land
- Dead-letter policy: nacks bump `deliveryAttempt`, and when `maxDeliveryAttempts` (default 5) is exhausted the message is forwarded to the `deadLetterTopic` with a new message id, original attributes kept and `CloudPubSubDeadLetterSource*` attributes added — a missing dead-letter topic keeps the message retrying, as in the real API
- Auto-creates entities by default, since production topology is usually pre-provisioned: publishing to a missing topic creates it, and subscribing (pull or streaming) to a missing subscription creates it bound to a same-named topic (created too) — disable with `autoCreate: false` to get strict `NOT_FOUND` behavior; differently named topic↔subscription bindings are a prefill concern
- Prefill topics, subscriptions and messages through the exported broker

## Versus the official emulator

Google's Cloud SDK ships a Pub/Sub emulator (`gcloud beta emulators pubsub start`) — a separate Java process implementing the same gRPC API. It is Google's own implementation, so its behavior tracks the real service most closely; prefer it for high-fidelity integration tests. This package targets fast, isolated test suites instead:

- In-process and dependency-free: `await startServer()` starts in milliseconds, no Java, gcloud SDK or Docker
- Seedable and inspectable: prefill state through the broker, assert on queues and outstanding acks — the official emulator can only be interacted with through the API itself
- Isolated: every server gets its own broker, so parallel suites never share state
- Auto-creates topology by default, the official emulator requires creating topics and subscriptions up front

Both are wire-compatible with the client's emulator conventions, so `PUBSUB_EMULATOR_HOST` works here too:

```javascript
import { PubSub } from '@google-cloud/pubsub';
import { startServer } from '@aller/pubsub-emulator';

const server = await startServer();
process.env.PUBSUB_EMULATOR_HOST = `localhost:${server.origin.port}`;

const pubsub = new PubSub({ projectId: 'test-project' });
const messageId = await pubsub.topic('env-configured').publishMessage({ data: Buffer.from('via env') });
console.log('published', messageId);

delete process.env.PUBSUB_EMULATOR_HOST;
await pubsub.close();
server.forceShutdown();
```

## Usage

```javascript
import { PubSub } from '@google-cloud/pubsub';
import { startServer } from '@aller/pubsub-emulator';

const server = await startServer();
const pubsub = new PubSub({ apiEndpoint: `localhost:${server.origin.port}`, projectId: 'test-project' });

await pubsub.createTopic('orders');
await pubsub.topic('orders').createSubscription('order-worker');

const subscription = pubsub.subscription('order-worker');
subscription.on('message', (message) => {
  console.log(message.data.toString());
  message.ack();
});

await pubsub.topic('orders').publishMessage({ data: Buffer.from('hello') });

// teardown, all state lives on the server's broker and dies with it
await subscription.close();
await pubsub.close();
server.forceShutdown();
```

The client switches to insecure channel credentials automatically for a non-GCP `apiEndpoint`, so no auth setup is required.

Each server gets its own broker, so parallel servers are fully isolated.

### Prefill via the broker

The smqp broker is the server's source of truth, so seeding state before any client connects is plain broker API — either pass a prefilled broker to `startServer`:

```javascript
import { Broker, startServer } from '@aller/pubsub-emulator';

const broker = new Broker();
broker.assertExchange('projects/test-project/topics/orders', 'topic');
broker.assertQueue('projects/test-project/subscriptions/order-worker');
broker.bindQueue('projects/test-project/subscriptions/order-worker', 'projects/test-project/topics/orders', '#');
broker.publish('projects/test-project/topics/orders', 'message', Buffer.from('seeded'));

const server = await startServer({ broker });
// clients now see the seeded topic, subscription and message
```

or seed the started server's own broker via `server.broker` before clients connect.

### Inspecting fake state

```javascript
import { startServer } from '@aller/pubsub-emulator';

const server = await startServer();
server.broker.assertExchange('projects/test-project/topics/orders', 'topic');
server.broker.assertQueue('projects/test-project/subscriptions/order-worker');
server.broker.bindQueue('projects/test-project/subscriptions/order-worker', 'projects/test-project/topics/orders', '#');
server.broker.publish('projects/test-project/topics/orders', 'message', Buffer.from('inspect me'));

server.getTopic('projects/test-project/topics/orders'); // stored topic resource
const { subscription, queue, outstanding } = server.getSubscription('projects/test-project/subscriptions/order-worker');
queue.messageCount; // queued + delivered-but-unacked messages, 1
outstanding.size; // delivered-but-unacked messages, 0
```

State lives on the server's broker — discard the server and the state is gone, no reset needed.

### API

- `startServer(options?)` → `Promise<grpc.Server & { origin: { hostname, port }, broker, getTopic, getSubscription }>` — options: `port` (default 0, OS-assigned), `credentials` (default insecure), `broker` (default a new broker per server, pass one to prefill), `autoCreate` (default `true`, auto-create topics on publish and subscriptions on subscribe).
- `server.broker` — the server's backing [smqp](https://github.com/paed01/smqp) broker, for prefilling and inspection.
- `server.getTopic(name)` / `server.getSubscription(name)` — inspect fake state by full resource name.
- `Broker` — re-exported from smqp, for constructing prefilled brokers.
- `FakePublisher` / `FakeSubscriber` — the gRPC service implementations, classed and constructed with a broker, for composing your own grpc-js server.
- `RpcCodes` — gRPC status code map.

Requires Node `>=22` (`require()` of the ESM source is supported natively — no CJS build is shipped).

## Test

```sh
npm i
npm test
```

Tests are written in BDD style with [mocha-cakes-2](https://www.npmjs.com/package/mocha-cakes-2), see `test/features/`.
