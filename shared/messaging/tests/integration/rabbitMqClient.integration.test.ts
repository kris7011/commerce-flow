import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import * as amqp from "amqplib";
import type {
    ConsumeMessage
} from "amqplib";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    type RabbitMqClientOptions
} from "../../src/rabbitMqClient.js";

interface IntegrationTestEvent {
    eventId: string;
    eventType: "MessagingIntegrationTestEvent";
    correlationId: string;
    occurredAt: string;
    data: {
        value: string;
    };
}

interface TestTopology {
    readonly exchangeName: string;
    readonly queueName: string;
    readonly deadLetterQueueName: string;
    readonly routingKey: string;
}

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const silentLogger:
    AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

const fastClientOptions:
    RabbitMqClientOptions = {
    maxConnectionRetries: 20,
    retryDelayInMs: 250,
    handlerMaxAttempts: 3,
    handlerRetryDelayInMs: 10,
    handlerMaxRetryDelayInMs: 20
};

test(
    "publishes with broker confirmation and consumes through a real RabbitMQ broker",
    {
        timeout: 15_000
    },
    async () => {
        await withTopology(
            async (topology, clients) => {
                const subscriber =
                    clients.create();

                const publisher =
                    clients.create();

                const expectedEvent =
                    createEvent(
                        "published through RabbitMQ"
                    );

                const received =
                    createDeferred<{
                        event: IntegrationTestEvent;
                        message: ConsumeMessage;
                    }>();

                await subscriber.subscribe({
                    queueName:
                        topology.queueName,
                    routingKeys: [
                        topology.routingKey
                    ],
                    parseEvent:
                        parseIntegrationTestEvent,
                    handler:
                        async (event, message) => {
                            received.resolve({
                                event,
                                message
                            });
                        }
                });

                await publisher.publish(
                    topology.routingKey,
                    expectedEvent
                );

                const {
                    event,
                    message
                } =
                    await withTimeout(
                        received.promise,
                        5_000
                    );

                assert.deepEqual(
                    event,
                    expectedEvent
                );

                assert.equal(
                    message.properties.messageId,
                    expectedEvent.eventId
                );

                assert.equal(
                    message.properties.correlationId,
                    expectedEvent.correlationId
                );

                assert.equal(
                    message.properties.type,
                    expectedEvent.eventType
                );

                assert.equal(
                    message.properties.contentType,
                    "application/json"
                );

                assert.equal(
                    message.properties.deliveryMode,
                    2
                );
            }
        );
    }
);

test(
    "dead-letters a malformed message without invoking the handler",
    {
        timeout: 15_000
    },
    async () => {
        await withTopology(
            async (topology, clients) => {
                const subscriber =
                    clients.create();

                let handlerCalls =
                    0;

                await subscriber.subscribe({
                    queueName:
                        topology.queueName,
                    routingKeys: [
                        topology.routingKey
                    ],
                    parseEvent:
                        parseIntegrationTestEvent,
                    handler:
                        async () => {
                            handlerCalls +=
                                1;
                        }
                });

                await publishRaw(
                    topology,
                    Buffer.from(
                        "{this is not json"
                    )
                );

                const deadLetter =
                    await waitForQueueMessage(
                        topology.deadLetterQueueName,
                        5_000
                    );

                assert.equal(
                    deadLetter.content.toString(),
                    "{this is not json"
                );

                assert.equal(
                    handlerCalls,
                    0
                );
            }
        );
    }
);

test(
    "dead-letters a message after the handler exhausts its retries",
    {
        timeout: 15_000
    },
    async () => {
        await withTopology(
            async (topology, clients) => {
                const subscriber =
                    clients.create();

                const publisher =
                    clients.create();

                let handlerCalls =
                    0;

                await subscriber.subscribe({
                    queueName:
                        topology.queueName,
                    routingKeys: [
                        topology.routingKey
                    ],
                    parseEvent:
                        parseIntegrationTestEvent,
                    handler:
                        async () => {
                            handlerCalls +=
                                1;

                            throw new Error(
                                "Downstream dependency unavailable"
                            );
                        }
                });

                const event =
                    createEvent(
                        "always fails"
                    );

                await publisher.publish(
                    topology.routingKey,
                    event
                );

                const deadLetter =
                    await waitForQueueMessage(
                        topology.deadLetterQueueName,
                        5_000
                    );

                assert.deepEqual(
                    JSON.parse(
                        deadLetter.content.toString()
                    ),
                    event
                );

                assert.equal(
                    deadLetter.properties.messageId,
                    event.eventId
                );

                assert.equal(
                    handlerCalls,
                    fastClientOptions.handlerMaxAttempts
                );
            }
        );
    }
);

test(
    "rejects publish instead of reporting success when the broker is unreachable",
    {
        timeout: 15_000
    },
    async () => {
        const unreachableClient =
            new RabbitMqClient(
                "amqp://guest:guest@127.0.0.1:1",
                `commerce.tests.${randomUUID()}`,
                {
                    maxConnectionRetries: 2,
                    retryDelayInMs: 10,
                    publishTimeoutInMs: 5_000
                },
                {
                    logger:
                        silentLogger
                }
            );

        try {
            await assert.rejects(
                unreachableClient.publish(
                    "test.unreachable",
                    createEvent(
                        "never delivered"
                    )
                ),
                /Could not connect to RabbitMQ/
            );
        } finally {
            await unreachableClient.close();
        }
    }
);

test(
    "finishes in-flight work before closing so the message is not redelivered",
    {
        timeout: 15_000
    },
    async () => {
        await withTopology(
            async (topology, clients) => {
                const subscriber =
                    clients.create();

                const publisher =
                    clients.create();

                const handlerStarted =
                    createDeferred<void>();

                const releaseHandler =
                    createDeferred<void>();

                let completedHandlers =
                    0;

                await subscriber.subscribe({
                    queueName:
                        topology.queueName,
                    routingKeys: [
                        topology.routingKey
                    ],
                    parseEvent:
                        parseIntegrationTestEvent,
                    handler:
                        async () => {
                            handlerStarted.resolve();

                            await releaseHandler.promise;

                            completedHandlers +=
                                1;
                        }
                });

                await publisher.publish(
                    topology.routingKey,
                    createEvent(
                        "in flight during shutdown"
                    )
                );

                await withTimeout(
                    handlerStarted.promise,
                    5_000
                );

                const closing =
                    subscriber.close();

                releaseHandler.resolve();

                await closing;

                assert.equal(
                    completedHandlers,
                    1
                );

                assert.equal(
                    await getQueueMessageCount(
                        topology.queueName
                    ),
                    0,
                    "The acknowledged message must not be requeued."
                );
            }
        );
    }
);

async function withTopology(
    action: (
        topology: TestTopology,
        clients: {
            create(): RabbitMqClient;
        }
    ) => Promise<void>
): Promise<void> {
    const uniqueId =
        randomUUID().replaceAll(
            "-",
            ""
        );

    const topology: TestTopology = {
        exchangeName:
            `commerce.tests.${uniqueId}`,
        queueName:
            `commerce.tests.queue.${uniqueId}`,
        deadLetterQueueName:
            `commerce.tests.queue.${uniqueId}.dead-letter`,
        routingKey:
            `commerce.tests.event.${uniqueId}`
    };

    const createdClients:
        RabbitMqClient[] = [];

    try {
        await action(
            topology,
            {
                create() {
                    const client =
                        new RabbitMqClient(
                            rabbitMqUrl,
                            topology.exchangeName,
                            fastClientOptions,
                            {
                                logger:
                                    silentLogger
                            }
                        );

                    createdClients.push(
                        client
                    );

                    return client;
                }
            }
        );
    } finally {
        await Promise.allSettled(
            createdClients.map(
                client =>
                    client.close()
            )
        );

        await deleteTestTopology(
            topology
        );
    }
}

async function withChannel<T>(
    action: (
        channel: amqp.Channel
    ) => Promise<T>
): Promise<T> {
    const connection =
        await amqp.connect(
            rabbitMqUrl
        );

    try {
        const channel =
            await connection.createChannel();

        try {
            return await action(
                channel
            );
        } finally {
            await channel
                .close()
                .catch(
                    () => undefined
                );
        }
    } finally {
        await connection.close();
    }
}

async function publishRaw(
    topology: TestTopology,
    content: Buffer
): Promise<void> {
    const connection =
        await amqp.connect(
            rabbitMqUrl
        );

    try {
        const channel =
            await connection.createConfirmChannel();

        channel.publish(
            topology.exchangeName,
            topology.routingKey,
            content,
            {
                persistent: true
            }
        );

        await channel.waitForConfirms();

        await channel.close();
    } finally {
        await connection.close();
    }
}

async function waitForQueueMessage(
    queueName: string,
    timeoutInMs: number
): Promise<amqp.GetMessage> {
    const deadline =
        Date.now() +
        timeoutInMs;

    while (Date.now() < deadline) {
        const message =
            await withChannel(
                async channel => {
                    const received =
                        await channel.get(
                            queueName,
                            {
                                noAck: true
                            }
                        );

                    return received ===
                        false
                        ? undefined
                        : received;
                }
            );

        if (message) {
            return message;
        }

        await new Promise(
            resolve =>
                setTimeout(
                    resolve,
                    100
                )
        );
    }

    throw new Error(
        `Timed out waiting for a message on ${queueName}.`
    );
}

async function getQueueMessageCount(
    queueName: string
): Promise<number> {
    return await withChannel(
        async channel => {
            const queue =
                await channel.checkQueue(
                    queueName
                );

            return queue.messageCount;
        }
    );
}

async function deleteTestTopology(
    topology: TestTopology
): Promise<void> {
    await withChannel(
        async channel => {
            await channel.deleteQueue(
                topology.deadLetterQueueName
            );

            await channel.deleteQueue(
                topology.queueName
            );

            await channel.deleteExchange(
                `${topology.exchangeName}.dead-letter`
            );

            await channel.deleteExchange(
                topology.exchangeName
            );
        }
    );
}

function createEvent(
    value: string
): IntegrationTestEvent {
    return {
        eventId:
            randomUUID(),
        eventType:
            "MessagingIntegrationTestEvent",
        correlationId:
            randomUUID(),
        occurredAt:
            new Date().toISOString(),
        data: {
            value
        }
    };
}

function parseIntegrationTestEvent(
    value: unknown
): IntegrationTestEvent {
    if (
        typeof value !== "object" ||
        value === null
    ) {
        throw new Error(
            "Expected an object."
        );
    }

    const candidate =
        value as Partial<IntegrationTestEvent>;

    if (
        typeof candidate.eventId !== "string" ||
        candidate.eventType !==
        "MessagingIntegrationTestEvent" ||
        typeof candidate.correlationId !== "string" ||
        typeof candidate.occurredAt !== "string" ||
        typeof candidate.data?.value !== "string"
    ) {
        throw new Error(
            "Invalid integration test event."
        );
    }

    return {
        eventId:
            candidate.eventId,
        eventType:
            candidate.eventType,
        correlationId:
            candidate.correlationId,
        occurredAt:
            candidate.occurredAt,
        data: {
            value:
                candidate.data.value
        }
    };
}

function createDeferred<T>(): {
    promise: Promise<T>;
    resolve: (value: T) => void;
} {
    let resolvePromise:
        (value: T) => void =
        () => undefined;

    const promise =
        new Promise<T>(
            resolve => {
                resolvePromise =
                    resolve;
            }
        );

    return {
        promise,
        resolve:
            value =>
                resolvePromise(
                    value
                )
    };
}

function withTimeout<T>(
    promise: Promise<T>,
    timeoutInMs: number
): Promise<T> {
    return new Promise<T>(
        (
            resolve,
            reject
        ) => {
            const timeout =
                setTimeout(
                    () => {
                        reject(
                            new Error(
                                `Timed out after ${timeoutInMs} ms.`
                            )
                        );
                    },
                    timeoutInMs
                );

            promise.then(
                value => {
                    clearTimeout(timeout);
                    resolve(value);
                },
                error => {
                    clearTimeout(timeout);
                    reject(error);
                }
            );
        }
    );
}
