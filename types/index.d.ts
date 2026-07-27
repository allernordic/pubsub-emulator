declare module '@aller/pubsub-emulator' {
	import type { Broker as SmqpBroker } from 'smqp';
	/**
	 * Start fake server with its own broker, or a prefilled one passed in options
	 * @param options Fake gRPC server options
	 * @returns Fake gRPC Google Pub/Sub server
	 */
	export function startServer(options?: startServerOptions): Promise<FakePubSubServer>;
	/**
	 * Backing message broker, subclassed from smqp so dts-buddy can emit it as a value export,
	 * for constructing prefilled brokers to pass to startServer
	 */
	export class Broker extends SmqpBroker {
	}
	/**
	 * Fake Publisher service implementation
	 */
	export class FakePublisher {
		/**
		 * @param broker backing message broker
		 * @param options service options
		 */
		constructor(broker: Broker, options?: fakeServiceOptions);
		broker: Broker;
		autoCreate: boolean;
		
		CreateTopic(req: FakeRequest, respond: CallableFunction): any;
		
		GetTopic(req: FakeRequest, respond: CallableFunction): any;
		
		ListTopics(req: FakeRequest, respond: CallableFunction): void;
		
		ListTopicSubscriptions(req: FakeRequest, respond: CallableFunction): any;
		
		DeleteTopic(req: FakeRequest, respond: CallableFunction): any;
		
		Publish(req: FakeRequest, respond: CallableFunction): any;
	}
	/**
	 * Fake Subscriber service implementation
	 */
	export class FakeSubscriber {
		/**
		 * @param broker backing message broker
		 * @param options service options
		 */
		constructor(broker: Broker, options?: fakeServiceOptions);
		broker: Broker;
		autoCreate: boolean;
		/**
		 * Get subscription, auto-creating it on the fly when enabled: the pull request only
		 * carries the subscription name, so the subscription is bound to a same-named topic,
		 * auto-created as well. Differently named bindings are a prefill concern
		 * @param name subscription name
		 */
		_getOrCreateSubscription(name: string): FakeSubscriptionData;
		
		CreateSubscription(req: FakeRequest, respond: CallableFunction): any;
		
		GetSubscription(req: FakeRequest, respond: CallableFunction): any;
		
		ListSubscriptions(req: FakeRequest, respond: CallableFunction): void;
		
		DeleteSubscription(req: FakeRequest, respond: CallableFunction): any;
		
		Pull(req: FakeRequest, respond: CallableFunction): any;
		
		Acknowledge(req: FakeRequest, respond: CallableFunction): any;
		
		ModifyAckDeadline(req: FakeRequest, respond: CallableFunction): any;
		/**
		 * Bidirectional streaming pull, used by the high-level subscription.on('message') client
		 * */
		StreamingPull(stream: import("@grpc/grpc-js").ServerDuplexStream<any, any>): void;
		/**
		 * Map a consumed smqp message to a pubsub received message and register it for ack
		 * */
		_toReceivedMessage(fakeSubscription: FakeSubscriptionData, message: import("smqp").Message): {
			ackId: string;
			message: any;
			deliveryAttempt: any;
		};
		/**
		 * Acks may arrive after the delivering stream was torn down and its messages requeued,
		 * e.g. when the pubsub client closes streams before flushing acks. The requeued copy is
		 * a new smqp message instance, so stale acks are matched by message id instead
		 * */
		_acknowledge(fakeSubscription: FakeSubscriptionData, ackIds: string[]): void;
		/**
		 * Nacked messages (deadline 0) are requeued for redelivery, or forwarded to the dead-letter
		 * topic when max delivery attempts is exhausted. Other deadlines are ignored since the fake
		 * server never expires outstanding messages
		 * */
		_modifyAckDeadline(fakeSubscription: FakeSubscriptionData, ackIds: string[], ackDeadlineSeconds: number): void;
		/**
		 * Forward a nacked message to the subscription's dead-letter topic when max delivery
		 * attempts is exhausted. Forwarding is a new publish: new message id, original attributes
		 * kept and dead-letter source attributes added, as in the real API. A missing dead-letter
		 * topic keeps the message retrying, also as in the real API
		 * @returns true if the message was forwarded
		 */
		_deadLetter(fakeSubscription: FakeSubscriptionData, message: import("smqp").Message): boolean;
	}
	export type FakePubSubServer = import("@grpc/grpc-js").Server & {
		origin: {
			hostname: string;
			port: number;
		};
		broker: Broker;
		getTopic: (name: string) => import("@google-cloud/pubsub").protos.google.pubsub.v1.ITopic | undefined;
		getSubscription: (name: string) => FakeSubscriptionData | undefined;
	};
	export type startServerOptions = {
		/**
		 * gRPC server port, defaults to 0 which lets the OS assign a free port
		 */
		port?: number;
		/**
		 * server credentials, defaults to insecure
		 */
		credentials?: import("@grpc/grpc-js").ServerCredentials;
		/**
		 * backing message broker, e.g. prefilled with topics and messages, defaults to a new broker
		 */
		broker?: Broker;
		/**
		 * auto-create topics on publish and subscriptions (bound to a same-named topic) on pull/streaming pull, defaults to true
		 */
		autoCreate?: boolean;
	};
	export type fakeServiceOptions = {
		/**
		 * auto-create entities on publish and subscribe, defaults to true
		 */
		autoCreate?: boolean;
	};
	export type FakeSubscriptionData = {
		/**
		 * Subscription
		 */
		subscription: import("@google-cloud/pubsub").protos.google.pubsub.v1.ISubscription;
		/**
		 * delivered but not yet acked messages by ack id
		 */
		outstanding: Map<string, import("smqp").Message>;
		/**
		 * backing smqp queue
		 */
		queue: import("smqp").Queue;
	};
	export type FakeRequest = {
		/**
		 * request payload
		 */
		request: any;
		/**
		 * request metadata
		 */
		metadata: import("@grpc/grpc-js").Metadata;
	};
	export namespace RpcCodes {
		let OK: number;
		let CANCELLED: number;
		let UNKNOWN: number;
		let INVALID_ARGUMENT: number;
		let DEADLINE_EXCEEDED: number;
		let NOT_FOUND: number;
		let ALREADY_EXISTS: number;
		let PERMISSION_DENIED: number;
		let UNAUTHENTICATED: number;
		let RESOURCE_EXHAUSTED: number;
		let FAILED_PRECONDITION: number;
		let ABORTED: number;
		let OUT_OF_RANGE: number;
		let UNIMPLEMENTED: number;
		let INTERNAL: number;
		let UNAVAILABLE: number;
		let DATA_LOSS: number;
	}

	export {};
}

//# sourceMappingURL=index.d.ts.map