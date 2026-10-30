import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createTestVirtualHost
} from "@commerce-flow/testing";
import * as amqp from "amqplib";
import pg from "pg";
import {
    InventoryService
} from "../src/inventoryService.js";
import type {
    OutboxMessage,
    OutboxPublisher,
    OutboxStore
} from "@commerce-flow/persistence";
import {
    OutboxRelay
} from "@commerce-flow/persistence";
import {
    PostgresOutboxStore
} from "@commerce-flow/persistence";
import {
    RabbitMqOutboxPublisher
} from "@commerce-flow/persistence";
import {
    createPaymentAuthorizedHandler
} from "../src/paymentAuthorizedHandler.js";
import {
    PostgresInventoryStore
} from "../src/postgresInventoryStore.js";
import {
    createInventoryTestDatabase,
    createPaymentAuthorizedEvent,
    enqueueTestEvents,
    resetInventoryState,
    waitUntil
} from "./support/inventoryTestDatabase.js";

/*
 * Reliability tests for the transactional outbox against a real
 * PostgreSQL database and a real RabbitMQ broker. Each test file uses
 * its own database and each test its own exchange.
 */

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

const {
    database,
    pool
} = await createInventoryTestDatabase();

const outboxStore =
    new PostgresOutboxStore(
        pool,
        "inventory_outbox"
    );

test.beforeEach(
    async () => {
        await resetInventoryState(
            pool,
            {
                "dryer-01": 100
            }
        );
    }
);

test.after(
    async () => {
        await database.drop();
    }
);

interface ReceivedMessage {
    readonly messageId: string | undefined;
    readonly routingKey: string;
    readonly body: {
        eventId: string;
        eventType: string;
    };
}

interface TestBroker {
    readonly exchangeName: string;
    readonly received: ReceivedMessage[];

    createClient(
        url?: string
    ): RabbitMqClient;

    close(): Promise<void>;
}

/** Captures everything published to a throwaway exchange. */
async function createTestBroker(): Promise<TestBroker> {
    const exchangeName =
        `commerce.tests.outbox.${randomUUID()}`;

    const connection =
        await amqp.connect(
            rabbitMqUrl
        );

    const channel =
        await connection.createChannel();

    await channel.assertExchange(
        exchangeName,
        "topic",
        {
            durable: true
        }
    );

    const {
        queue
    } =
        await channel.assertQueue(
            "",
            {
                exclusive: true
            }
        );

    await channel.bindQueue(
        queue,
        exchangeName,
        "#"
    );

    const received:
        ReceivedMessage[] = [];

    await channel.consume(
        queue,
        message => {
            if (!message) {
                return;
            }

            received.push({
                messageId:
                    message.properties.messageId,
                routingKey:
                    message.fields.routingKey,
                body:
                    JSON.parse(
                        message.content.toString()
                    )
            });
        },
        {
            noAck: true
        }
    );

    const clients:
        RabbitMqClient[] = [];

    return {
        exchangeName,
        received,

        createClient(
            url = rabbitMqUrl
        ) {
            const client =
                new RabbitMqClient(
                    url,
                    exchangeName,
                    {
                        maxConnectionRetries: 1,
                        retryDelayInMs: 10,
                        publishTimeoutInMs: 3_000
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
        },

        async close() {
            await Promise.allSettled(
                clients.map(
                    client =>
                        client.close()
                )
            );

            await channel.deleteExchange(
                exchangeName
            );

            await connection.close();
        }
    };
}

function createRelay(
    publisher: OutboxPublisher,
    options: ConstructorParameters<typeof OutboxRelay>[1] = {},
    store: OutboxStore = outboxStore
): OutboxRelay {
    return new OutboxRelay(
        {
            store,
            publisher,
            logger:
                silentLogger
        },
        {
            claimantId:
                `relay-${randomUUID().slice(0, 8)}`,
            retryBaseDelayInMs: 20,
            retryMaxDelayInMs: 50,
            ...options
        }
    );
}

async function readOutboxState(): Promise<{
    pending: number;
    published: number;
}> {
    const result =
        await pool.query<{
            pending: string;
            published: string;
        }>(
            `
                SELECT
                    count(*) FILTER (WHERE published_at IS NULL) AS pending,
                    count(*) FILTER (WHERE published_at IS NOT NULL) AS published
                FROM inventory_outbox
            `
        );

    return {
        pending:
            Number(
                result.rows[0]?.pending
            ),
        published:
            Number(
                result.rows[0]?.published
            )
    };
}

test(
    "publishes committed reservations through RabbitMQ with confirms",
    {
        timeout: 20_000
    },
    async () => {
        const broker =
            await createTestBroker();

        try {
            const relay =
                createRelay(
                    new RabbitMqOutboxPublisher(
                        broker.createClient()
                    )
                );

            const handler =
                createPaymentAuthorizedHandler({
                    inventoryService:
                        new InventoryService(
                            new PostgresInventoryStore(
                                pool
                            )
                        ),
                    logger:
                        silentLogger,
                    onEventEnqueued:
                        () => relay.notify()
                });

            const controller =
                new AbortController();

            const running =
                relay.run(
                    controller.signal
                );

            await handler(
                createPaymentAuthorizedEvent(
                    "order-success",
                    [
                        {
                            productId: "dryer-01",
                            quantity: 1,
                            unitPrice: 2999
                        }
                    ]
                )
            );

            await handler(
                createPaymentAuthorizedEvent(
                    "order-failure",
                    [
                        {
                            productId: "dryer-01",
                            quantity: 1_000,
                            unitPrice: 2999
                        }
                    ]
                )
            );

            await waitUntil(
                () =>
                    broker.received.length === 2,
                10_000,
                "two published inventory events"
            );

            await waitUntil(
                async () =>
                    (await readOutboxState()).pending === 0,
                5_000,
                "the outbox to be drained"
            );

            controller.abort();

            await running;

            assert.deepEqual(
                broker.received
                    .map(
                        message => [
                            message.routingKey,
                            message.body.eventType
                        ]
                    )
                    .sort(),
                [
                    [
                        "inventory.reservation.failed",
                        "InventoryReservationFailed"
                    ],
                    [
                        "inventory.reserved",
                        "InventoryReserved"
                    ]
                ]
            );

            for (const message of broker.received) {
                assert.equal(
                    message.messageId,
                    message.body.eventId
                );
            }
        } finally {
            await broker.close();
        }
    }
);

test(
    "keeps events pending while RabbitMQ is unreachable and publishes them after a relay restart",
    {
        timeout: 30_000
    },
    async () => {
        const broker =
            await createTestBroker();

        try {
            const events =
                await enqueueTestEvents(
                    pool,
                    3
                );

            const unreachableRelay =
                createRelay(
                    new RabbitMqOutboxPublisher(
                        broker.createClient(
                            "amqp://guest:guest@127.0.0.1:1"
                        )
                    )
                );

            assert.equal(
                await unreachableRelay.processBatch(),
                3
            );

            const failed =
                await pool.query<{
                    attempts: number;
                    lastError: string | null;
                    claimedBy: string | null;
                }>(
                    `
                        SELECT
                            attempts,
                            last_error AS "lastError",
                            claimed_by AS "claimedBy"
                        FROM inventory_outbox
                        WHERE published_at IS NULL
                    `
                );

            assert.equal(
                failed.rows.length,
                3
            );

            for (const row of failed.rows) {
                assert.equal(
                    row.attempts,
                    1
                );

                assert.match(
                    row.lastError ?? "",
                    /Could not connect to RabbitMQ/
                );

                assert.equal(
                    row.claimedBy,
                    null
                );
            }

            // A new relay instance (e.g. after a process restart)
            // picks the events up once their backoff has elapsed.
            const restartedRelay =
                createRelay(
                    new RabbitMqOutboxPublisher(
                        broker.createClient()
                    )
                );

            await waitUntil(
                async () => {
                    await restartedRelay.processBatch();

                    return (await readOutboxState()).pending === 0;
                },
                10_000,
                "the restarted relay to publish the backlog"
            );

            await waitUntil(
                () =>
                    broker.received.length === 3,
                5_000,
                "the backlog to arrive at the consumer"
            );

            assert.deepEqual(
                broker.received
                    .map(
                        message =>
                            message.body.eventId
                    )
                    .sort(),
                events
                    .map(
                        event =>
                            event.eventId
                    )
                    .sort()
            );
        } finally {
            await broker.close();
        }
    }
);

test(
    "re-publishes an event whose relay crashed after publishing but before marking it",
    {
        timeout: 30_000
    },
    async () => {
        const broker =
            await createTestBroker();

        try {
            const [event] =
                await enqueueTestEvents(
                    pool,
                    1
                );

            const crashingStore:
                OutboxStore = {
                    claimBatch: (...args) =>
                        outboxStore.claimBatch(...args),
                    releaseForRetry: (...args) =>
                        outboxStore.releaseForRetry(...args),
                    getStatistics: () =>
                        outboxStore.getStatistics(),
                    markPublished: async () => {
                        throw new Error(
                            "Process killed before marking"
                        );
                    }
                };

            const crashingRelay =
                createRelay(
                    new RabbitMqOutboxPublisher(
                        broker.createClient()
                    ),
                    {
                        leaseInMs: 300
                    },
                    crashingStore
                );

            await assert.rejects(
                crashingRelay.processBatch(),
                /Process killed/
            );

            await waitUntil(
                () =>
                    broker.received.length === 1,
                5_000,
                "the first publication"
            );

            const survivingRelay =
                createRelay(
                    new RabbitMqOutboxPublisher(
                        broker.createClient()
                    )
                );

            // The event stays invisible until the crashed relay's
            // lease expires, then it is published again.
            await waitUntil(
                async () =>
                    (await survivingRelay.processBatch()) > 0,
                5_000,
                "the lease to expire"
            );

            await waitUntil(
                () =>
                    broker.received.length === 2,
                5_000,
                "the second publication"
            );

            assert.deepEqual(
                broker.received.map(
                    message =>
                        message.messageId
                ),
                [
                    event?.eventId,
                    event?.eventId
                ],
                "At-least-once: the duplicate keeps the same event id, " +
                "so idempotent consumers can discard it."
            );

            assert.deepEqual(
                await readOutboxState(),
                {
                    pending: 0,
                    published: 1
                }
            );
        } finally {
            await broker.close();
        }
    }
);

test(
    "competing relays publish every event exactly once when nothing fails",
    {
        timeout: 30_000
    },
    async () => {
        const events =
            await enqueueTestEvents(
                pool,
                200
            );

        const publishedEventIds:
            string[] = [];

        const recordingPublisher:
            OutboxPublisher = {
                publish: async (
                    message: OutboxMessage
                ) => {
                    // Yield so the relays genuinely interleave.
                    await new Promise(
                        resolve =>
                            setImmediate(
                                resolve
                            )
                    );

                    publishedEventIds.push(
                        message.eventId
                    );
                }
            };

        const relays =
            Array.from(
                {
                    length: 4
                },
                () =>
                    createRelay(
                        recordingPublisher,
                        {
                            batchSize: 7
                        }
                    )
            );

        // Each relay uses its own connections to make the
        // competition happen inside PostgreSQL.
        const claimsPerRelay =
            await Promise.all(
                relays.map(
                    async relay => {
                        let claimed = 0;

                        for (;;) {
                            const count =
                                await relay.processBatch();

                            if (count === 0) {
                                return claimed;
                            }

                            claimed +=
                                count;
                        }
                    }
                )
            );

        assert.equal(
            publishedEventIds.length,
            200
        );

        assert.equal(
            new Set(
                publishedEventIds
            ).size,
            200,
            "No event may be published twice."
        );

        assert.deepEqual(
            [
                ...publishedEventIds
            ].sort(),
            events
                .map(
                    event =>
                        event.eventId
                )
                .sort()
        );

        assert.ok(
            claimsPerRelay.filter(
                count =>
                    count > 0
            ).length > 1,
            `Expected work to be shared, got ${claimsPerRelay.join(", ")}`
        );

        assert.deepEqual(
            await readOutboxState(),
            {
                pending: 0,
                published: 200
            }
        );
    }
);

test(
    "fences a relay whose lease expired from rescheduling a re-claimed event",
    async () => {
        await enqueueTestEvents(
            pool,
            1
        );

        const [staleClaim] =
            await outboxStore.claimBatch(
                "relay-a",
                10,
                50
            );

        assert.ok(
            staleClaim
        );

        await waitUntil(
            async () =>
                (await outboxStore.claimBatch(
                    "relay-b",
                    10,
                    60_000
                )).length === 1,
            5_000,
            "relay-b to claim the expired event"
        );

        assert.equal(
            await outboxStore.releaseForRetry(
                staleClaim,
                "relay-a",
                0,
                "stale failure"
            ),
            false
        );

        const row =
            await pool.query<{
                claimedBy: string;
                attempts: number;
            }>(
                `
                    SELECT
                        claimed_by AS "claimedBy",
                        attempts
                    FROM inventory_outbox
                `
            );

        assert.deepEqual(
            row.rows,
            [
                {
                    claimedBy: "relay-b",
                    attempts: 2
                }
            ]
        );
    }
);

test(
    "finishes the in-flight batch before stopping on shutdown",
    {
        timeout: 15_000
    },
    async () => {
        await enqueueTestEvents(
            pool,
            2
        );

        let releasePublish:
            () => void =
            () => undefined;

        const publishGate =
            new Promise<void>(
                resolve => {
                    releasePublish =
                        resolve;
                }
            );

        let publishesStarted =
            0;

        const relay =
            createRelay({
                publish: async () => {
                    publishesStarted +=
                        1;

                    await publishGate;
                }
            });

        const controller =
            new AbortController();

        const running =
            relay.run(
                controller.signal
            );

        await waitUntil(
            () =>
                publishesStarted === 2,
            5_000,
            "the batch to start publishing"
        );

        controller.abort();

        releasePublish();

        await running;

        assert.deepEqual(
            await readOutboxState(),
            {
                pending: 0,
                published: 2
            }
        );
    }
);

test(
    "keeps relaying after PostgreSQL terminates its connections",
    {
        timeout: 20_000
    },
    async () => {
        const publishedEventIds =
            new Set<string>();

        const relay =
            createRelay(
                {
                    publish: async message => {
                        publishedEventIds.add(
                            message.eventId
                        );
                    }
                },
                {
                    pollIntervalInMs: 20
                }
            );

        const controller =
            new AbortController();

        const running =
            relay.run(
                controller.signal
            );

        const [beforeOutage] =
            await enqueueTestEvents(
                pool,
                1
            );

        await waitUntil(
            () =>
                publishedEventIds.has(
                    beforeOutage?.eventId ?? ""
                ),
            5_000,
            "the event before the outage"
        );

        const admin =
            new pg.Client({
                connectionString:
                    database.connectionString
            });

        await admin.connect();

        try {
            await admin.query(
                `
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE
                        datname = current_database()
                        AND pid <> pg_backend_pid()
                `
            );
        } finally {
            await admin.end();
        }

        const [afterOutage] =
            await enqueueTestEvents(
                pool,
                1
            );

        await waitUntil(
            () =>
                publishedEventIds.has(
                    afterOutage?.eventId ?? ""
                ),
            10_000,
            "the event after the outage"
        );

        controller.abort();

        await running;

        assert.equal(
            relay.getStatus().running,
            false
        );
    }
);

test(
    "keeps an unroutable event pending until its queue exists, then publishes it once",
    {
        timeout: 30_000
    },
    async () => {
        // A fresh virtual host: no queues, no bindings. The publisher
        // deliberately has no provisioned topology, which models a
        // missing or misconfigured consumer queue.
        const virtualHost =
            await createTestVirtualHost();

        const publisherClient =
            new RabbitMqClient(
                virtualHost.url,
                "commerce.events",
                {
                    maxConnectionRetries: 3,
                    retryDelayInMs: 50,
                    publishTimeoutInMs: 3_000
                },
                {
                    logger:
                        silentLogger
                }
            );

        try {
            const inventoryService =
                new InventoryService(
                    new PostgresInventoryStore(
                        pool
                    )
                );

            const sourceEvent =
                createPaymentAuthorizedEvent(
                    "order-unroutable",
                    [
                        {
                            productId: "dryer-01",
                            quantity: 2,
                            unitPrice: 2999
                        }
                    ]
                );

            const outcome =
                await inventoryService.reserveForPayment(
                    sourceEvent
                );

            assert.equal(
                outcome.status,
                "Processed"
            );

            const relay =
                createRelay(
                    new RabbitMqOutboxPublisher(
                        publisherClient
                    )
                );

            assert.equal(
                await relay.processBatch(),
                1
            );

            const pending =
                await pool.query<{
                    attempts: number;
                    lastError: string | null;
                }>(
                    `
                        SELECT
                            attempts,
                            last_error AS "lastError"
                        FROM inventory_outbox
                        WHERE published_at IS NULL
                    `
                );

            assert.equal(
                pending.rows.length,
                1,
                "An unroutable event must not be marked as published."
            );

            assert.match(
                pending.rows[0]?.lastError ?? "",
                /could not route the message.*inventory\.reserved.*312 NO_ROUTE/
            );

            // The consumer's queue appears later.
            const connection =
                await amqp.connect(
                    virtualHost.url
                );

            const received:
                string[] = [];

            try {
                const channel =
                    await connection.createChannel();

                await channel.assertQueue(
                    "delivery-service.inventory-reserved",
                    {
                        durable: true
                    }
                );

                await channel.bindQueue(
                    "delivery-service.inventory-reserved",
                    "commerce.events",
                    "inventory.reserved"
                );

                await channel.consume(
                    "delivery-service.inventory-reserved",
                    message => {
                        if (message) {
                            received.push(
                                String(
                                    message.properties.messageId
                                )
                            );
                        }
                    },
                    {
                        noAck: true
                    }
                );

                await waitUntil(
                    async () => {
                        await relay.processBatch();

                        return (await readOutboxState()).pending === 0;
                    },
                    10_000,
                    "the event to be published after its retry delay"
                );

                await waitUntil(
                    () =>
                        received.length === 1,
                    5_000,
                    "the event to reach the late queue"
                );
            } finally {
                await connection.close();
            }

            assert.deepEqual(
                received,
                [
                    outcome.status === "Processed"
                        ? outcome.event.eventId
                        : ""
                ]
            );

            // Retrying publication never touched the reservation.
            assert.equal(
                (await new PostgresInventoryStore(pool).getAllStock())["dryer-01"],
                98
            );

            const reservations =
                await pool.query<{
                    count: string;
                }>(
                    "SELECT count(*) AS count FROM inventory_reservations"
                );

            assert.equal(
                Number(
                    reservations.rows[0]?.count
                ),
                1
            );

            assert.deepEqual(
                await readOutboxState(),
                {
                    pending: 0,
                    published: 1
                }
            );
        } finally {
            await publisherClient.close();
            await virtualHost.delete();
        }
    }
);
