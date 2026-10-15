import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    createTestVirtualHost,
    type TestVirtualHost
} from "@commerce-flow/testing";
import * as amqp from "amqplib";
import {
    UnroutableMessageError
} from "../../src/messageErrors.js";
import {
    RabbitMqClient,
    type RabbitMqClientOptions
} from "../../src/rabbitMqClient.js";

/*
 * Routing guarantees against a real broker. Every test uses a fresh
 * virtual host, i.e. a broker with no queues and no bindings — the
 * situation a publisher faces when it starts before any consumer.
 */

const EXCHANGE =
    "commerce.events";

const silentLogger:
    AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

interface TestEvent {
    readonly eventId: string;
    readonly eventType: "RoutingTestEvent";
    readonly correlationId: string;
}

function createEvent(): TestEvent {
    return {
        eventId:
            randomUUID(),
        eventType:
            "RoutingTestEvent",
        correlationId:
            randomUUID()
    };
}

async function withVirtualHost(
    action: (
        virtualHost: TestVirtualHost,
        createClient: (options?: RabbitMqClientOptions) => RabbitMqClient
    ) => Promise<void>
): Promise<void> {
    const virtualHost =
        await createTestVirtualHost();

    const clients:
        RabbitMqClient[] = [];

    try {
        await action(
            virtualHost,
            (options = {}) => {
                const client =
                    new RabbitMqClient(
                        virtualHost.url,
                        EXCHANGE,
                        {
                            maxConnectionRetries: 5,
                            retryDelayInMs: 100,
                            publishTimeoutInMs: 5_000,
                            ...options
                        },
                        {
                            logger:
                                silentLogger
                        }
                    );

                clients.push(
                    client
                );

                return client;
            }
        );
    } finally {
        await Promise.allSettled(
            clients.map(
                client =>
                    client.close()
            )
        );

        await virtualHost.delete();
    }
}

async function withChannel<T>(
    url: string,
    action: (channel: amqp.Channel) => Promise<T>
): Promise<T> {
    const connection =
        await amqp.connect(
            url
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

async function drainQueue(
    url: string,
    queueName: string
): Promise<string[]> {
    return await withChannel(
        url,
        async channel => {
            const messageIds:
                string[] = [];

            for (;;) {
                const message =
                    await channel.get(
                        queueName,
                        {
                            noAck: true
                        }
                    );

                if (message === false) {
                    return messageIds;
                }

                messageIds.push(
                    String(
                        message.properties.messageId
                    )
                );
            }
        }
    );
}

function assertUnroutable(
    error: unknown,
    routingKey: string
): true {
    assert.ok(
        error instanceof Error
    );

    assert.ok(
        error.cause instanceof UnroutableMessageError,
        `Expected UnroutableMessageError, got: ${error.message}`
    );

    assert.equal(
        error.cause.routingKey,
        routingKey
    );

    assert.equal(
        error.cause.replyCode,
        312
    );

    return true;
}

test(
    "rejects a publication that no queue can receive instead of reporting success",
    {
        timeout: 20_000
    },
    async () => {
        await withVirtualHost(
            async (_virtualHost, createClient) => {
                await assert.rejects(
                    createClient().publish(
                        "inventory.reserved",
                        createEvent()
                    ),
                    error =>
                        assertUnroutable(
                            error,
                            "inventory.reserved"
                        )
                );
            }
        );
    }
);

test(
    "publishes successfully once the destination queue exists",
    {
        timeout: 20_000
    },
    async () => {
        await withVirtualHost(
            async (virtualHost, createClient) => {
                const publisher =
                    createClient();

                const event =
                    createEvent();

                await assert.rejects(
                    publisher.publish(
                        "inventory.reserved",
                        event
                    )
                );

                await withChannel(
                    virtualHost.url,
                    async channel => {
                        await channel.assertQueue(
                            "late-consumer",
                            {
                                durable: true
                            }
                        );

                        await channel.bindQueue(
                            "late-consumer",
                            EXCHANGE,
                            "inventory.reserved"
                        );
                    }
                );

                // The retry of the same event now succeeds, and the
                // failed attempt left nothing behind in the queue.
                await publisher.publish(
                    "inventory.reserved",
                    event
                );

                assert.deepEqual(
                    await drainQueue(
                        virtualHost.url,
                        "late-consumer"
                    ),
                    [
                        event.eventId
                    ]
                );
            }
        );
    }
);

test(
    "fails exactly the unroutable publications among concurrent ones",
    {
        timeout: 20_000
    },
    async () => {
        await withVirtualHost(
            async (virtualHost, createClient) => {
                const publisher =
                    createClient();

                await withChannel(
                    virtualHost.url,
                    async channel => {
                        await channel.assertExchange(
                            EXCHANGE,
                            "topic",
                            {
                                durable: true
                            }
                        );

                        await channel.assertQueue(
                            "routed",
                            {
                                durable: true
                            }
                        );

                        await channel.bindQueue(
                            "routed",
                            EXCHANGE,
                            "routable"
                        );
                    }
                );

                const publications =
                    Array.from(
                        {
                            length: 40
                        },
                        (_, index) => {
                            const event =
                                createEvent();

                            const routingKey =
                                index % 2 === 0
                                    ? "routable"
                                    : "unroutable";

                            return {
                                event,
                                routingKey,
                                result:
                                    publisher.publish(
                                        routingKey,
                                        event
                                    )
                            };
                        }
                    );

                const results =
                    await Promise.allSettled(
                        publications.map(
                            publication =>
                                publication.result
                        )
                    );

                publications.forEach(
                    (publication, index) => {
                        const result =
                            results[index];

                        if (publication.routingKey === "routable") {
                            assert.equal(
                                result?.status,
                                "fulfilled"
                            );
                        } else {
                            assert.equal(
                                result?.status,
                                "rejected"
                            );

                            if (result?.status === "rejected") {
                                assertUnroutable(
                                    result.reason,
                                    "unroutable"
                                );
                            }
                        }
                    }
                );

                assert.deepEqual(
                    (await drainQueue(
                        virtualHost.url,
                        "routed"
                    )).sort(),
                    publications
                        .filter(
                            publication =>
                                publication.routingKey === "routable"
                        )
                        .map(
                            publication =>
                                publication.event.eventId
                        )
                        .sort()
                );
            }
        );
    }
);

test(
    "rejects a publication that the broker nacks",
    {
        timeout: 20_000
    },
    async () => {
        await withVirtualHost(
            async (virtualHost, createClient) => {
                const publisher =
                    createClient();

                // A full queue with reject-publish makes the broker
                // nack further messages.
                await withChannel(
                    virtualHost.url,
                    async channel => {
                        await channel.assertExchange(
                            EXCHANGE,
                            "topic",
                            {
                                durable: true
                            }
                        );

                        await channel.assertQueue(
                            "bounded",
                            {
                                durable: true,
                                arguments: {
                                    "x-max-length": 1,
                                    "x-overflow": "reject-publish"
                                }
                            }
                        );

                        await channel.bindQueue(
                            "bounded",
                            EXCHANGE,
                            "bounded"
                        );
                    }
                );

                await publisher.publish(
                    "bounded",
                    createEvent()
                );

                await assert.rejects(
                    publisher.publish(
                        "bounded",
                        createEvent()
                    ),
                    /message nacked/
                );
            }
        );
    }
);

test(
    "provisioned topology lets a publisher run before any consumer has started",
    {
        timeout: 20_000
    },
    async () => {
        await withVirtualHost(
            async (_virtualHost, createClient) => {
                const topology = [
                    {
                        queueName:
                            "late-service.inventory-reserved",
                        routingKeys: [
                            "inventory.reserved"
                        ]
                    }
                ];

                const event =
                    createEvent();

                await createClient({
                    topology
                }).publish(
                    "inventory.reserved",
                    event
                );

                const received =
                    new Promise<TestEvent>(
                        resolve => {
                            void createClient({
                                topology
                            }).subscribe<TestEvent>({
                                ...topology[0]!,
                                parseEvent:
                                    value =>
                                        value as TestEvent,
                                handler:
                                    async receivedEvent => {
                                        resolve(
                                            receivedEvent
                                        );
                                    }
                            });
                        }
                    );

                assert.equal(
                    (await received).eventId,
                    event.eventId
                );
            }
        );
    }
);
