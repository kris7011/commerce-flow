import assert from "node:assert/strict";
import test from "node:test";
import type {
    ConsumeMessage
} from "amqplib";
import {
    PermanentMessageError,
    PublishTimeoutError
} from "../src/messageErrors.js";
import {
    RabbitMqClient,
    type RabbitMqChannel,
    type RabbitMqClientDependencies,
    type RabbitMqClientOptions,
    type RabbitMqConnection,
    type RabbitMqLogger,
    type RabbitMqPublishOptions
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
    "shares one in-flight connection attempt across concurrent callers",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        let connectCalls =
            0;

        let releaseConnection:
            () => void =
            () => undefined;

        const connectionGate =
            new Promise<void>(
                resolve => {
                    releaseConnection =
                        resolve;
                }
            );

        const client =
            createClient(
                connection,
                {},
                {
                    connect:
                        async () => {
                            connectCalls +=
                                1;

                            await connectionGate;

                            return connection;
                        }
                }
            );

        const firstConnect =
            client.connect();

        const secondConnect =
            client.connect();

        assert.equal(
            connectCalls,
            1
        );

        assert.equal(
            connection
                .createChannelCalls,
            0
        );

        releaseConnection();

        await Promise.all([
            firstConnect,
            secondConnect
        ]);

        assert.equal(
            connectCalls,
            1
        );

        assert.equal(
            connection
                .createChannelCalls,
            1
        );

        assert.equal(
            channel
                .assertExchangeCalls
                .length,
            1
        );

        assert.equal(
            client.isReady(),
            true
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
    "closes the stale connection before reconnecting after a channel close",
    async () => {
        const firstChannel =
            new FakeChannel();

        const firstConnection =
            new FakeConnection(
                firstChannel
            );

        const secondChannel =
            new FakeChannel();

        const secondConnection =
            new FakeConnection(
                secondChannel
            );

        const connections = [
            firstConnection,
            secondConnection
        ];

        let connectCalls =
            0;

        const client =
            new RabbitMqClient(
                "amqp://test",
                "commerce.events",
                {},
                {
                    connect:
                        async () => {
                            const connection =
                                connections[
                                connectCalls
                                ];

                            connectCalls +=
                                1;

                            assert.ok(
                                connection
                            );

                            return connection;
                        },

                    sleep:
                        async () =>
                            undefined,

                    logger:
                        silentLogger
                }
            );

        await client.connect();

        assert.equal(
            client.isReady(),
            true
        );

        firstChannel
            .triggerClose();

        assert.equal(
            client.isReady(),
            false
        );

        await client.connect();

        assert.equal(
            connectCalls,
            2
        );

        assert.equal(
            firstConnection
                .closeCalls,
            1
        );

        assert.equal(
            secondConnection
                .closeCalls,
            0
        );

        assert.equal(
            client.isReady(),
            true
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
    "cleans up an in-flight connection when initialization is aborted",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const controller =
            new AbortController();

        let releaseConnection:
            (() => void) | undefined;

        const connectionGate =
            new Promise<void>(
                resolve => {
                    releaseConnection =
                        resolve;
                }
            );

        let connectionStarted:
            (() => void) | undefined;

        const connectionStartedGate =
            new Promise<void>(
                resolve => {
                    connectionStarted =
                        resolve;
                }
            );

        const client =
            createClient(
                connection,
                {},
                {
                    connect:
                        async () => {
                            connectionStarted?.();

                            await connectionGate;

                            return connection;
                        }
                }
            );

        const connectPromise =
            client.connect(
                controller.signal
            );

        await connectionStartedGate;

        controller.abort();

        releaseConnection?.();

        await assert.rejects(
            async () => {
                await connectPromise;
            },
            {
                name:
                    "AbortError",
                message:
                    "RabbitMQ initialization aborted."
            }
        );

        assert.equal(
            connection
                .closeCalls,
            1
        );

        assert.equal(
            connection
                .createChannelCalls,
            0
        );

        assert.equal(
            client.isReady(),
            false
        );
    }
);

test(
    "stops retrying when initialization is aborted during the retry delay",
    async () => {
        const controller =
            new AbortController();

        let connectionAttempts =
            0;

        let receivedSignal:
            AbortSignal | undefined;

        const client =
            new RabbitMqClient(
                "amqp://test",
                "commerce.events",
                {
                    maxConnectionRetries:
                        5,
                    retryDelayInMs:
                        25
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
                        async (
                            _milliseconds,
                            signal
                        ) => {
                            receivedSignal =
                                signal;

                            controller.abort();
                        },

                    logger:
                        silentLogger
                }
            );

        await assert.rejects(
            async () => {
                await client.connect(
                    controller.signal
                );
            },
            {
                name:
                    "AbortError",
                message:
                    "RabbitMQ initialization aborted."
            }
        );

        assert.equal(
            connectionAttempts,
            1
        );

        assert.ok(
            receivedSignal
        );

        assert.equal(
            receivedSignal.aborted,
            true
        );

        assert.equal(
            controller.signal.aborted,
            true
        );

        assert.equal(
            client.isReady(),
            false
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
                    "correlation-001",
                type:
                    "TestEvent"
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

        await subscribeTestHandler(
                client,
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

        await subscribeTestHandler(
                client,
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
    "acknowledges an in-flight message on its original channel after reconnect",
    async () => {
        const firstChannel =
            new FakeChannel();

        const firstConnection =
            new FakeConnection(
                firstChannel
            );

        const secondChannel =
            new FakeChannel();

        const secondConnection =
            new FakeConnection(
                secondChannel
            );

        const connections = [
            firstConnection,
            secondConnection
        ];

        let connectCalls =
            0;

        const client =
            new RabbitMqClient(
                "amqp://test",
                "commerce.events",
                {},
                {
                    connect:
                        async () => {
                            const connection =
                                connections[
                                connectCalls
                                ];

                            connectCalls +=
                                1;

                            assert.ok(
                                connection
                            );

                            return connection;
                        },

                    sleep:
                        async () =>
                            undefined,

                    logger:
                        silentLogger
                }
            );

        let releaseHandler:
            (() => void) | undefined;

        const handlerGate =
            new Promise<void>(
                resolve => {
                    releaseHandler =
                        resolve;
                }
            );

        let markHandlerStarted:
            (() => void) | undefined;

        const handlerStarted =
            new Promise<void>(
                resolve => {
                    markHandlerStarted =
                        resolve;
                }
            );

        await subscribeTestHandler(
                client,
                [
                    "test.event"
                ],
                async () => {
                    markHandlerStarted?.();

                    await handlerGate;
                }
            );

        const firstConsumer =
            getSingleConsumer(
                firstChannel
            );

        const message =
            createMessage(
                createTestEvent()
            );

        const consumePromise =
            Promise.resolve(
                firstConsumer(
                    message
                )
            );

        await handlerStarted;

        firstChannel
            .triggerClose();

        assert.equal(
            client.isReady(),
            false
        );

        await client.connect();

        assert.equal(
            client.isReady(),
            true
        );

        assert.equal(
            connectCalls,
            2
        );

        releaseHandler?.();

        await consumePromise;

        assert.deepEqual(
            firstChannel
                .acknowledgedMessages,
            [
                message
            ]
        );

        assert.equal(
            secondChannel
                .acknowledgedMessages
                .length,
            0
        );

        assert.equal(
            firstChannel
                .nackedMessages
                .length,
            0
        );

        assert.equal(
            secondChannel
                .nackedMessages
                .length,
            0
        );
    }
);

test(
    "dead-letters messages after exhausting bounded handler retries",
    async () => {
        const channel =
            new FakeChannel();

        const connection =
            new FakeConnection(
                channel
            );

        const delays:
            number[] = [];

        const client =
            createClient(
                connection,
                {
                    handlerMaxAttempts:
                        4,
                    handlerRetryDelayInMs:
                        100,
                    handlerMaxRetryDelayInMs:
                        300
                },
                {
                    sleep:
                        async milliseconds => {
                            delays.push(
                                milliseconds
                            );
                        }
                }
            );

        let handlerCalls =
            0;

        await subscribeTestHandler(
                client,
                [
                    "test.event"
                ],
                async () => {
                    handlerCalls +=
                        1;

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
            handlerCalls,
            4
        );

        assert.deepEqual(
            delays,
            [
                100,
                200,
                300
            ]
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

        await subscribeTestHandler(
                client,
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
                    eventType:
                        "TestEvent",
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

        await subscribeTestHandler(
                client,
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
                    eventId:
                        "event-001",
                    eventType:
                        "TestEvent",
                    correlationId:
                        "correlation-001",
                    redelivered:
                        false,
                    attempt:
                        5,
                    maxAttempts:
                        5,
                    permanent:
                        false,
                    deadLetterQueueName:
                        "test-service.test-events" +
                        ".dead-letter",
                    action:
                        "dead-letter"
                }
            }
        );

        assert.equal(
            logger
                .warningLogs
                .filter(
                    entry =>
                        entry.message ===
                        "Retrying message after handler failure"
                )
                .length,
            4
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

test(
    "rejects publish when the broker nacks the message",
    async () => {
        const channel =
            new FakeChannel();

        channel.confirmPublication =
            async () => {
                throw new Error(
                    "message nacked"
                );
            };

        const logger =
            new RecordingRabbitMqLogger();

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {},
                {
                    logger
                }
            );

        await assert.rejects(
            client.publish(
                "test.event",
                createTestEvent()
            ),
            {
                message:
                    "message nacked"
            }
        );

        assert.equal(
            logger.infoLogs.some(
                entry =>
                    entry.message ===
                    "Published event"
            ),
            false
        );
    }
);

test(
    "rejects publish with a timeout when no confirmation arrives",
    async () => {
        const channel =
            new FakeChannel();

        channel.confirmPublication =
            () =>
                new Promise<void>(
                    () => undefined
                );

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {
                    publishTimeoutInMs:
                        20
                }
            );

        await assert.rejects(
            client.publish(
                "test.event",
                createTestEvent()
            ),
            (error: unknown) => {
                assert.ok(
                    error instanceof
                    PublishTimeoutError
                );

                assert.match(
                    error.message,
                    /did not confirm event event-001 within 20 ms/
                );

                return true;
            }
        );
    }
);

test(
    "rejects publish when the channel closes before confirmation",
    async () => {
        const channel =
            new FakeChannel();

        channel.confirmPublication =
            async () => {
                channel.triggerClose();

                throw new Error(
                    "channel closed"
                );
            };

        const client =
            createClient(
                new FakeConnection(
                    channel
                )
            );

        await assert.rejects(
            client.publish(
                "test.event",
                createTestEvent()
            ),
            {
                message:
                    "channel closed"
            }
        );

        assert.equal(
            client.isReady(),
            false
        );
    }
);

test(
    "limits unacknowledged deliveries with a configurable prefetch",
    async () => {
        const channel =
            new FakeChannel();

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {
                    prefetchCount:
                        3
                }
            );

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () =>
                undefined
        );

        assert.deepEqual(
            channel.prefetchCalls,
            [
                3
            ]
        );
    }
);

test(
    "dead-letters permanent handler failures without retrying",
    async () => {
        const channel =
            new FakeChannel();

        const client =
            createClient(
                new FakeConnection(
                    channel
                )
            );

        let handlerCalls =
            0;

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () => {
                handlerCalls +=
                    1;

                throw new PermanentMessageError(
                    "Unsupported business state"
                );
            }
        );

        const message =
            createMessage(
                createTestEvent()
            );

        await getSingleConsumer(
            channel
        )(
            message
        );

        assert.equal(
            handlerCalls,
            1
        );

        assert.deepEqual(
            channel.nackedMessages,
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
    "dead-letters messages whose body is not valid JSON",
    async () => {
        const channel =
            new FakeChannel();

        const logger =
            new RecordingRabbitMqLogger();

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {},
                {
                    logger
                }
            );

        let handlerCalls =
            0;

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () => {
                handlerCalls +=
                    1;
            }
        );

        const message =
            createRawMessage(
                "{not json"
            );

        await getSingleConsumer(
            channel
        )(
            message
        );

        assert.equal(
            handlerCalls,
            0
        );

        assert.equal(
            channel.nackedMessages.length,
            1
        );

        assert.equal(
            logger.errorLogs[0]?.message,
            "Rejected malformed message"
        );

        assert.ok(
            logger.errorLogs[0]?.error instanceof
            PermanentMessageError
        );
    }
);

test(
    "recovers from a transient handler failure on retry",
    async () => {
        const channel =
            new FakeChannel();

        const client =
            createClient(
                new FakeConnection(
                    channel
                )
            );

        let handlerCalls =
            0;

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () => {
                handlerCalls +=
                    1;

                if (handlerCalls < 3) {
                    throw new Error(
                        "Database temporarily unavailable"
                    );
                }
            }
        );

        const message =
            createMessage(
                createTestEvent()
            );

        await getSingleConsumer(
            channel
        )(
            message
        );

        assert.equal(
            handlerCalls,
            3
        );

        assert.deepEqual(
            channel.acknowledgedMessages,
            [
                message
            ]
        );

        assert.equal(
            channel.nackedMessages.length,
            0
        );
    }
);

test(
    "leaves a message unsettled when its channel closes during a retry delay",
    async () => {
        const channel =
            new FakeChannel();

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {},
                {
                    sleep:
                        async () => {
                            channel.triggerClose();
                        }
                }
            );

        let handlerCalls =
            0;

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () => {
                handlerCalls +=
                    1;

                throw new Error(
                    "Transient failure"
                );
            }
        );

        await getSingleConsumer(
            channel
        )(
            createMessage(
                createTestEvent()
            )
        );

        assert.equal(
            handlerCalls,
            1
        );

        assert.equal(
            channel.acknowledgedMessages.length,
            0
        );

        assert.equal(
            channel.nackedMessages.length,
            0
        );
    }
);

test(
    "does not crash when acknowledging on a closed channel",
    async () => {
        const channel =
            new FakeChannel();

        channel.ack =
            () => {
                throw new Error(
                    "Channel closed"
                );
            };

        const logger =
            new RecordingRabbitMqLogger();

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {},
                {
                    logger
                }
            );

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () =>
                undefined
        );

        await getSingleConsumer(
            channel
        )(
            createMessage(
                createTestEvent()
            )
        );

        assert.equal(
            logger.warningLogs.some(
                entry =>
                    entry.message ===
                    "Could not settle message on a closed channel"
            ),
            true
        );
    }
);

test(
    "drains in-flight handlers and cancels consumers on close",
    async () => {
        const channel =
            new FakeChannel();

        const client =
            new RabbitMqClient(
                "amqp://test",
                "commerce.events",
                {},
                {
                    connect:
                        async () =>
                            new FakeConnection(
                                channel
                            ),

                    // Never resolves: close() must wait for the
                    // handler rather than for the drain timeout.
                    sleep:
                        () =>
                            new Promise<void>(
                                () => undefined
                            ),

                    logger:
                        silentLogger
                }
            );

        const events: string[] = [];

        let releaseHandler:
            () => void =
            () => undefined;

        let markStarted:
            () => void =
            () => undefined;

        const handlerStarted =
            new Promise<void>(
                resolve => {
                    markStarted =
                        resolve;
                }
            );

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async () => {
                markStarted();

                await new Promise<void>(
                    resolve => {
                        releaseHandler =
                            resolve;
                    }
                );

                events.push(
                    "handler-finished"
                );
            }
        );

        const delivery =
            Promise.resolve(
                getSingleConsumer(
                    channel
                )(
                    createMessage(
                        createTestEvent()
                    )
                )
            );

        await handlerStarted;

        const closing =
            client
                .close()
                .then(
                    () => {
                        events.push(
                            "closed"
                        );
                    }
                );

        await Promise.resolve();

        assert.deepEqual(
            channel.cancelledConsumerTags,
            [
                "fake-consumer"
            ]
        );

        assert.equal(
            channel.closeCalls,
            0
        );

        releaseHandler();

        await Promise.all([
            delivery,
            closing
        ]);

        assert.deepEqual(
            events,
            [
                "handler-finished",
                "closed"
            ]
        );

        assert.equal(
            channel.acknowledgedMessages.length,
            1
        );

        assert.equal(
            channel.closeCalls,
            1
        );
    }
);

test(
    "bounds the in-memory duplicate cache",
    async () => {
        const channel =
            new FakeChannel();

        const client =
            createClient(
                new FakeConnection(
                    channel
                ),
                {
                    recentEventIdCacheSize:
                        1
                }
            );

        const handledEventIds:
            string[] = [];

        await subscribeTestHandler(
            client,
            [
                "test.event"
            ],
            async event => {
                handledEventIds.push(
                    event.eventId
                );
            }
        );

        const consume =
            getSingleConsumer(
                channel
            );

        for (
            const eventId of [
                "event-a",
                "event-a",
                "event-b",
                "event-a"
            ]
        ) {
            await consume(
                createMessage({
                    ...createTestEvent(),
                    eventId
                })
            );
        }

        // event-b evicted event-a, so the last delivery is
        // handled again. Handlers must be idempotent anyway.
        assert.deepEqual(
            handledEventIds,
            [
                "event-a",
                "event-b",
                "event-a"
            ]
        );
    }
);

async function subscribeTestHandler(
    client: RabbitMqClient,
    routingKeys: string[],
    handler: (
        event: TestEvent,
        message: ConsumeMessage
    ) => Promise<void>
): Promise<void> {
    await client.subscribe<TestEvent>({
        queueName:
            "test-service.test-events",
        routingKeys,
        parseEvent:
            parseTestEvent,
        handler
    });
}

function parseTestEvent(
    value: unknown
): TestEvent {
    if (
        typeof value !== "object" ||
        value === null
    ) {
        throw new Error(
            "Expected an object."
        );
    }

    const candidate =
        value as Partial<TestEvent>;

    if (
        typeof candidate.eventId !== "string" ||
        candidate.eventId.length === 0 ||
        candidate.eventType !== "TestEvent" ||
        typeof candidate.correlationId !== "string" ||
        typeof candidate.data?.value !== "string"
    ) {
        throw new Error(
            "Invalid TestEvent."
        );
    }

    return {
        eventId:
            candidate.eventId,
        eventType:
            candidate.eventType,
        correlationId:
            candidate.correlationId,
        data: {
            value:
                candidate.data.value
        }
    };
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
    return createRawMessage(
        JSON.stringify(
            body
        )
    );
}

function createRawMessage(
    content: string
): ConsumeMessage {
    return {
        content:
            Buffer.from(
                content
            ),
        fields: {
            redelivered:
                false
        },
        properties: {}
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
        options: RabbitMqPublishOptions;
    }[] = [];

    readonly prefetchCalls:
        number[] = [];

    readonly cancelledConsumerTags:
        string[] = [];

    /**
     * Simulates the broker's publisher confirmation. Tests can
     * replace it to model nacks, slow confirms or closed channels.
     */
    confirmPublication:
        () => Promise<void> =
        async () => undefined;

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

    async prefetch(
        count: number
    ): Promise<unknown> {
        this.prefetchCalls.push(
            count
        );

        return {};
    }

    async publish(
        exchangeName: string,
        routingKey: string,
        content: Buffer,
        options: RabbitMqPublishOptions
    ): Promise<void> {
        this
            .publishCalls
            .push({
                exchangeName,
                routingKey,
                content,
                options
            });

        await this.confirmPublication();
    }

    async consume(
        queueName: string,
        onMessage: (
            message:
                ConsumeMessage | null
        ) => void | Promise<void>
    ): Promise<{
        consumerTag: string;
    }> {
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

    async cancel(
        consumerTag: string
    ): Promise<unknown> {
        this.cancelledConsumerTags.push(
            consumerTag
        );

        return {};
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