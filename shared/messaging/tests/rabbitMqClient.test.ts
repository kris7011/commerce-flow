import assert from "node:assert/strict";
import test from "node:test";
import type {
    ConsumeMessage
} from "amqplib";
import {
    RabbitMqClient,
    type RabbitMqChannel,
    type RabbitMqClientDependencies,
    type RabbitMqClientOptions,
    type RabbitMqConnection,
    type RabbitMqLogger
} from "../src/rabbitMqClient.js";

interface TestEvent {
    eventId: string;
    eventType: "TestEvent";
    correlationId: string;
    data: {
        value: string;
    };
}

const silentLogger:
    RabbitMqLogger = {
    info:
        () => undefined,

    warn:
        () => undefined,

    error:
        () => undefined,

    child:
        () =>
            silentLogger
};

test(
    "connects and declares the durable topic exchange only once",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        let connectCalls = 0;

        const client =
            createClient(
                connection,
                {},
                {
                    connect:
                        async () => {
                            connectCalls +=
                                1;

                            return connection;
                        }
                }
            );

        await client.connect();
        await client.connect();

        assert.equal(
            connectCalls,
            1
        );

        assert.equal(
            connection
                .createChannelCalls,
            1
        );

        assert.deepEqual(
            channel
                .assertExchangeCalls,
            [
                {
                    exchangeName:
                        "commerce.events",
                    exchangeType:
                        "topic",
                    options: {
                        durable: true
                    }
                }
            ]
        );
    }
);

test(
    "reports readiness across the connection lifecycle",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        assert.equal(
            client.isReady(),
            false
        );

        await client.connect();

        assert.equal(
            client.isReady(),
            true
        );

        await client.close();

        assert.equal(
            client.isReady(),
            false
        );
    }
);

test(
    "becomes unready when the RabbitMQ connection closes",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        await client.connect();

        assert.equal(
            client.isReady(),
            true
        );

        connection
            .triggerClose();

        assert.equal(
            client.isReady(),
            false
        );
    }
);

test(
    "becomes unready when the RabbitMQ channel closes",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        await client.connect();

        assert.equal(
            client.isReady(),
            true
        );

        channel.triggerClose();

        assert.equal(
            client.isReady(),
            false
        );
    }
);

test(
    "retries failed connections using the configured delay",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        let connectionAttempts =
            0;

        const delays:
            number[] = [];

        const client =
            new RabbitMqClient(
                "amqp://test",
                "commerce.events",
                {
                    maxConnectionRetries:
                        3,
                    retryDelayInMs:
                        25
                },
                {
                    connect:
                        async () => {
                            connectionAttempts +=
                                1;

                            if (
                                connectionAttempts <
                                3
                            ) {
                                throw new Error(
                                    "Broker unavailable"
                                );
                            }

                            return connection;
                        },

                    sleep:
                        async milliseconds => {
                            delays.push(
                                milliseconds
                            );
                        },

                    logger:
                        silentLogger
                }
            );

        await client.connect();

        assert.equal(
            connectionAttempts,
            3
        );

        assert.deepEqual(
            delays,
            [
                25,
                25
            ]
        );
    }
);

test(
    "throws after all connection attempts fail",
    async () => {
        let connectionAttempts =
            0;

        const delays:
            number[] = [];

        const client =
            new RabbitMqClient(
                "amqp://test",
                "commerce.events",
                {
                    maxConnectionRetries:
                        2,
                    retryDelayInMs:
                        10
                },
                {
                    connect:
                        async () => {
                            connectionAttempts +=
                                1;

                            throw new Error(
                                "Broker unavailable"
                            );
                        },

                    sleep:
                        async milliseconds => {
                            delays.push(
                                milliseconds
                            );
                        },

                    logger:
                        silentLogger
                }
            );

        await assert.rejects(
            async () => {
                await client
                    .connect();
            },
            {
                message:
                    "[messaging] Could not " +
                    "connect to RabbitMQ after " +
                    "2 attempts. Last error: " +
                    "Broker unavailable"
            }
        );

        assert.equal(
            connectionAttempts,
            2
        );

        assert.deepEqual(
            delays,
            [
                10
            ]
        );
    }
);

test(
    "publishes serialized events with persistent message metadata",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        const event =
            createTestEvent();

        await client.publish(
            "test.event",
            event
        );

        assert.equal(
            channel
                .publishCalls
                .length,
            1
        );

        const publishCall =
            channel
                .publishCalls[0];

        assert.ok(
            publishCall
        );

        assert.equal(
            publishCall
                .exchangeName,
            "commerce.events"
        );

        assert.equal(
            publishCall
                .routingKey,
            "test.event"
        );

        assert.deepEqual(
            JSON.parse(
                publishCall
                    .content
                    .toString()
            ),
            event
        );

        assert.deepEqual(
            publishCall.options,
            {
                contentType:
                    "application/json",
                deliveryMode:
                    2,
                messageId:
                    "event-001",
                correlationId:
                    "correlation-001"
            }
        );
    }
);

test(
    "configures durable queues and dead-letter routing",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        await client
            .subscribe<TestEvent>(
                "test-service.test-events",
                [
                    "test.created",
                    "test.updated"
                ],
                async () =>
                    undefined
            );

        assert.deepEqual(
            channel
                .assertExchangeCalls,
            [
                {
                    exchangeName:
                        "commerce.events",
                    exchangeType:
                        "topic",
                    options: {
                        durable:
                            true
                    }
                },
                {
                    exchangeName:
                        "commerce.events.dead-letter",
                    exchangeType:
                        "topic",
                    options: {
                        durable:
                            true
                    }
                }
            ]
        );

        assert.deepEqual(
            channel
                .assertQueueCalls,
            [
                {
                    queueName:
                        "test-service.test-events" +
                        ".dead-letter",
                    options: {
                        durable:
                            true
                    }
                },
                {
                    queueName:
                        "test-service.test-events",
                    options: {
                        durable:
                            true,
                        arguments: {
                            "x-dead-letter-exchange":
                                "commerce.events" +
                                ".dead-letter",
                            "x-dead-letter-routing-key":
                                "test-service" +
                                ".test-events" +
                                ".dead-letter"
                        }
                    }
                }
            ]
        );

        assert.deepEqual(
            channel
                .bindQueueCalls,
            [
                {
                    queueName:
                        "test-service.test-events" +
                        ".dead-letter",
                    exchangeName:
                        "commerce.events" +
                        ".dead-letter",
                    routingKey:
                        "test-service.test-events" +
                        ".dead-letter"
                },
                {
                    queueName:
                        "test-service.test-events",
                    exchangeName:
                        "commerce.events",
                    routingKey:
                        "test.created"
                },
                {
                    queueName:
                        "test-service.test-events",
                    exchangeName:
                        "commerce.events",
                    routingKey:
                        "test.updated"
                }
            ]
        );

        assert.equal(
            channel
                .consumeCalls
                .length,
            1
        );

        assert.equal(
            channel
                .consumeCalls[0]
                ?.queueName,
            "test-service.test-events"
        );
    }
);

test(
    "acknowledges successful messages and skips duplicate events",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        const handledEvents:
            TestEvent[] = [];

        await client
            .subscribe<TestEvent>(
                "test-service.test-events",
                [
                    "test.event"
                ],
                async event => {
                    handledEvents.push(
                        event
                    );
                }
            );

        const consume =
            getSingleConsumer(
                channel
            );

        const firstMessage =
            createMessage(
                createTestEvent()
            );

        const duplicateMessage =
            createMessage(
                createTestEvent()
            );

        await consume(
            firstMessage
        );

        await consume(
            duplicateMessage
        );

        assert.equal(
            handledEvents.length,
            1
        );

        assert.deepEqual(
            handledEvents[0],
            createTestEvent()
        );

        assert.deepEqual(
            channel
                .acknowledgedMessages,
            [
                firstMessage,
                duplicateMessage
            ]
        );

        assert.equal(
            channel
                .nackedMessages
                .length,
            0
        );
    }
);

test(
    "nacks messages without requeueing when the handler fails",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        await client
            .subscribe<TestEvent>(
                "test-service.test-events",
                [
                    "test.event"
                ],
                async () => {
                    throw new Error(
                        "Handler failed"
                    );
                }
            );

        const consume =
            getSingleConsumer(
                channel
            );

        const message =
            createMessage(
                createTestEvent()
            );

        await consume(
            message
        );

        assert.equal(
            channel
                .acknowledgedMessages
                .length,
            0
        );

        assert.deepEqual(
            channel
                .nackedMessages,
            [
                {
                    message,
                    allUpTo:
                        false,
                    requeue:
                        false
                }
            ]
        );
    }
);

test(
    "nacks messages that do not contain a valid event id",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const client =
            createClient(
                connection
            );

        let handlerCalls =
            0;

        await client
            .subscribe<TestEvent>(
                "test-service.test-events",
                [
                    "test.event"
                ],
                async () => {
                    handlerCalls +=
                        1;
                }
            );

        const consume =
            getSingleConsumer(
                channel
            );

        const message =
            createMessage({
                eventId: "",
                correlationId:
                    "correlation-001"
            });

        await consume(
            message
        );

        assert.equal(
            handlerCalls,
            0
        );

        assert.deepEqual(
            channel
                .nackedMessages,
            [
                {
                    message,
                    allUpTo:
                        false,
                    requeue:
                        false
                }
            ]
        );
    }
);

test(
    "closes resources and allows a later reconnect",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        let connectCalls =
            0;

        const client =
            createClient(
                connection,
                {},
                {
                    connect:
                        async () => {
                            connectCalls +=
                                1;

                            return connection;
                        }
                }
            );

        await client.connect();
        await client.close();
        await client.connect();

        assert.equal(
            connectCalls,
            2
        );

        assert.equal(
            connection
                .createChannelCalls,
            2
        );

        assert.equal(
            connection
                .closeCalls,
            1
        );

        assert.equal(
            channel
                .closeCalls,
            1
        );
    }
);

test(
    "logs published events with structured context",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const logger =
            new RecordingRabbitMqLogger();

        const client =
            createClient(
                connection,
                {},
                {
                    logger
                }
            );

        await client.publish(
            "test.event",
            createTestEvent()
        );

        const publishLog =
            logger.infoLogs.find(
                entry =>
                    entry.message ===
                    "Published event"
            );

        assert.deepEqual(
            publishLog,
            {
                message:
                    "Published event",
                context: {
                    exchangeName:
                        "commerce.events",
                    routingKey:
                        "test.event",
                    eventId:
                        "event-001",
                    correlationId:
                        "correlation-001"
                }
            }
        );
    }
);

test(
    "logs handler failures with structured error context",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const logger =
            new RecordingRabbitMqLogger();

        const client =
            createClient(
                connection,
                {},
                {
                    logger
                }
            );

        const handlerError =
            new Error(
                "Handler failed"
            );

        await client
            .subscribe<TestEvent>(
                "test-service.test-events",
                [
                    "test.event"
                ],
                async () => {
                    throw handlerError;
                }
            );

        const consume =
            getSingleConsumer(
                channel
            );

        await consume(
            createMessage(
                createTestEvent()
            )
        );

        assert.equal(
            logger
                .errorLogs
                .length,
            1
        );

        assert.deepEqual(
            logger.errorLogs[0],
            {
                message:
                    "Failed to process message",
                error:
                    handlerError,
                context: {
                    queueName:
                        "test-service.test-events",
                    deadLetterQueueName:
                        "test-service.test-events" +
                        ".dead-letter"
                }
            }
        );
    }
);

function createClient(
    connection:
        RabbitMqConnection,
    options:
        RabbitMqClientOptions = {},
    dependencies:
        Partial<
            RabbitMqClientDependencies
        > = {}
): RabbitMqClient {
    return new RabbitMqClient(
        "amqp://test",
        "commerce.events",
        options,
        {
            connect:
                async () =>
                    connection,

            sleep:
                async () =>
                    undefined,

            logger:
                silentLogger,

            ...dependencies
        }
    );
}

function createTestEvent():
    TestEvent {
    return {
        eventId:
            "event-001",
        eventType:
            "TestEvent",
        correlationId:
            "correlation-001",
        data: {
            value:
                "test-value"
        }
    };
}

function createMessage(
    body: unknown
): ConsumeMessage {
    return {
        content:
            Buffer.from(
                JSON.stringify(
                    body
                )
            )
    } as ConsumeMessage;
}

function getSingleConsumer(
    channel:
        FakeChannel
): (
    message:
        ConsumeMessage | null
) => void | Promise<void> {
    assert.equal(
        channel
            .consumeCalls
            .length,
        1
    );

    const consumeCall =
        channel
            .consumeCalls[0];

    assert.ok(
        consumeCall
    );

    return consumeCall
        .onMessage;
}

class RecordingRabbitMqLogger
    implements RabbitMqLogger {
    readonly infoLogs: {
        message: string;
        context?:
        Readonly<
            Record<
                string,
                unknown
            >
        >;
    }[] = [];

    readonly warningLogs: {
        message: string;
        context?:
        Readonly<
            Record<
                string,
                unknown
            >
        >;
    }[] = [];

    readonly errorLogs: {
        message: string;
        error?: unknown;
        context?:
        Readonly<
            Record<
                string,
                unknown
            >
        >;
    }[] = [];

    info(
        message: string,
        context?:
            Readonly<
                Record<
                    string,
                    unknown
                >
            >
    ): void {
        this.infoLogs.push({
            message,
            context
        });
    }

    warn(
        message: string,
        context?:
            Readonly<
                Record<
                    string,
                    unknown
                >
            >
    ): void {
        this.warningLogs.push({
            message,
            context
        });
    }

    error(
        message: string,
        error?: unknown,
        context?:
            Readonly<
                Record<
                    string,
                    unknown
                >
            >
    ): void {
        this.errorLogs.push({
            message,
            error,
            context
        });
    }

    child(
        _context:
            Readonly<
                Record<
                    string,
                    unknown
                >
            >
    ): RabbitMqLogger {
        return this;
    }
}

class FakeConnection
    implements RabbitMqConnection {
    createChannelCalls = 0;
    closeCalls = 0;

    private readonly closeHandlers:
        (() => void)[] = [];

    constructor(
        private readonly channel:
            RabbitMqChannel
    ) {
    }

    async createChannel():
        Promise<RabbitMqChannel> {
        this.createChannelCalls +=
            1;

        return this.channel;
    }

    onClose(
        handler: () => void
    ): void {
        this.closeHandlers.push(
            handler
        );
    }

    triggerClose(): void {
        for (
            const handler
            of this.closeHandlers
        ) {
            handler();
        }
    }

    async close():
        Promise<void> {
        this.closeCalls +=
            1;
    }
}

class FakeChannel
    implements RabbitMqChannel {
    readonly assertExchangeCalls: {
        exchangeName: string;
        exchangeType: string;
        options: {
            durable: boolean;
        };
    }[] = [];

    readonly assertQueueCalls: {
        queueName: string;
        options: {
            durable: boolean;
            arguments?:
            Record<
                string,
                unknown
            >;
        };
    }[] = [];

    readonly bindQueueCalls: {
        queueName: string;
        exchangeName: string;
        routingKey: string;
    }[] = [];

    readonly publishCalls: {
        exchangeName: string;
        routingKey: string;
        content: Buffer;
        options: {
            contentType: string;
            deliveryMode: number;
            messageId: string;
            correlationId: string;
        };
    }[] = [];

    readonly consumeCalls: {
        queueName: string;
        onMessage: (
            message:
                ConsumeMessage | null
        ) => void | Promise<void>;
    }[] = [];

    readonly acknowledgedMessages:
        ConsumeMessage[] = [];

    readonly nackedMessages: {
        message:
        ConsumeMessage;
        allUpTo:
        boolean | undefined;
        requeue:
        boolean | undefined;
    }[] = [];

    closeCalls = 0;

    private readonly closeHandlers:
        (() => void)[] = [];

    async assertExchange(
        exchangeName: string,
        exchangeType: string,
        options: {
            durable: boolean;
        }
    ): Promise<unknown> {
        this
            .assertExchangeCalls
            .push({
                exchangeName,
                exchangeType,
                options
            });

        return {};
    }

    async assertQueue(
        queueName: string,
        options: {
            durable: boolean;
            arguments?:
            Record<
                string,
                unknown
            >;
        }
    ): Promise<unknown> {
        this
            .assertQueueCalls
            .push({
                queueName,
                options
            });

        return {};
    }

    async bindQueue(
        queueName: string,
        exchangeName: string,
        routingKey: string
    ): Promise<unknown> {
        this
            .bindQueueCalls
            .push({
                queueName,
                exchangeName,
                routingKey
            });

        return {};
    }

    publish(
        exchangeName: string,
        routingKey: string,
        content: Buffer,
        options: {
            contentType: string;
            deliveryMode: number;
            messageId: string;
            correlationId: string;
        }
    ): boolean {
        this
            .publishCalls
            .push({
                exchangeName,
                routingKey,
                content,
                options
            });

        return true;
    }

    async consume(
        queueName: string,
        onMessage: (
            message:
                ConsumeMessage | null
        ) => void | Promise<void>
    ): Promise<unknown> {
        this
            .consumeCalls
            .push({
                queueName,
                onMessage
            });

        return {
            consumerTag:
                "fake-consumer"
        };
    }

    ack(
        message:
            ConsumeMessage
    ): void {
        this
            .acknowledgedMessages
            .push(
                message
            );
    }

    nack(
        message:
            ConsumeMessage,
        allUpTo?: boolean,
        requeue?: boolean
    ): void {
        this
            .nackedMessages
            .push({
                message,
                allUpTo,
                requeue
            });
    }

    onClose(
        handler: () => void
    ): void {
        this.closeHandlers.push(
            handler
        );
    }

    triggerClose(): void {
        for (
            const handler
            of this.closeHandlers
        ) {
            handler();
        }
    }

    async close():
        Promise<void> {
        this.closeCalls +=
            1;
    }
}