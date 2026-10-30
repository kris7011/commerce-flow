import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import {
    once
} from "node:events";
import type {
    AddressInfo
} from "node:net";
import test from "node:test";
import {
    COMMERCE_TOPOLOGY,
    QUEUES,
    type QueueBinding
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    OutboxRelay,
    PostgresOutboxStore,
    RabbitMqOutboxPublisher
} from "@commerce-flow/persistence";
import {
    createTestDatabase,
    createTestVirtualHost
} from "@commerce-flow/testing";
import * as amqp from "amqplib";
import {
    createOrderApp
} from "../src/app.js";
import {
    createOrderDatabasePool,
    ORDER_OUTBOX_TABLE
} from "../src/database.js";
import {
    migrateOrderDatabase,
    ORDER_MIGRATIONS
} from "../src/migrations.js";
import {
    OrderService
} from "../src/orderService.js";
import type {
    NewOrder
} from "../src/orderStore.js";
import {
    PostgresOrderStore
} from "../src/postgresOrderStore.js";

/*
 * The idempotency and timeline guarantees against a real PostgreSQL
 * database (one per test file) and, for publication, a real RabbitMQ
 * virtual host (one per test).
 */

const silentLogger:
    AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

const database =
    await createTestDatabase();

const pool =
    createOrderDatabasePool({
        connectionString:
            database.connectionString,
        maxConnections:
            20
    });

const store =
    new PostgresOrderStore(
        pool
    );

await migrateOrderDatabase(
    pool
);

test.after(
    async () => {
        await pool.end();
        await database.drop();
    }
);

const orderService =
    new OrderService();

function newOrder(
    idempotencyKey: string,
    quantity = 1
): NewOrder {
    const request = {
        customerId:
            "customer-integration",
        items: [
            {
                productId: "dryer-01",
                quantity,
                unitPrice: 2999
            }
        ]
    };

    const created =
        orderService.createOrder(
            request,
            `integration-${idempotencyKey}`
        );

    return {
        idempotencyKey,
        requestFingerprint:
            created.requestFingerprint,
        customerId:
            request.customerId,
        items:
            request.items,
        totalAmount:
            created.response.totalAmount,
        correlationId:
            created.response.correlationId,
        event:
            created.event
    };
}

async function countRows(
    sql: string,
    values: unknown[]
): Promise<number> {
    const result =
        await pool.query<{
            count: string;
        }>(
            sql,
            values
        );

    return Number(
        result.rows[0]?.count
    );
}

test(
    "migrations are recorded once and can run again",
    async () => {
        const again =
            await migrateOrderDatabase(
                pool
            );

        assert.deepEqual(
            again,
            {
                appliedVersions: [],
                currentVersion: ORDER_MIGRATIONS.length
            }
        );
    }
);

test(
    "a new order, its OrderCreated outbox event and the first timeline entry are stored together",
    async () => {
        const order =
            newOrder(`key-${randomUUID()}`);

        const outcome =
            await store.submitOrder(
                order
            );

        assert.equal(outcome.kind, "Created");

        const details =
            await store.getOrderDetails(
                order.event.data.orderId
            );

        assert.equal(details?.order.status, "Created");
        assert.equal(details?.order.totalAmount, 2999);
        assert.deepEqual(details?.order.items, order.items);
        assert.deepEqual(details?.timeline.map(row => row.kind), ["Accepted"]);
        assert.equal(details?.publication?.eventId, order.event.eventId);
        assert.equal(details?.publication?.publishedAt, null);

        const outbox =
            await pool.query<{
                payload: unknown;
                routingKey: string;
            }>(
                `
                    SELECT payload, routing_key AS "routingKey"
                    FROM order_outbox
                    WHERE event_id = $1
                `,
                [
                    order.event.eventId
                ]
            );

        assert.equal(outbox.rows[0]?.routingKey, "order.created");
        assert.deepEqual(outbox.rows[0]?.payload, order.event);
    }
);

test(
    "20 concurrent submissions with one key create exactly one order and one event",
    async () => {
        const key =
            `key-${randomUUID()}`;

        // Each attempt is a separate request: new generated ids, same content.
        const outcomes =
            await Promise.all(
                Array.from(
                    {
                        length: 20
                    },
                    () =>
                        store.submitOrder(
                            newOrder(key)
                        )
                )
            );

        const kinds =
            outcomes.map(outcome => outcome.kind).sort();

        assert.equal(kinds.filter(kind => kind === "Created").length, 1);
        assert.equal(kinds.filter(kind => kind === "Replayed").length, 19);

        const orderIds =
            new Set(
                outcomes.map(outcome =>
                    outcome.kind === "KeyReused"
                        ? undefined
                        : outcome.order.orderId
                )
            );

        assert.equal(orderIds.size, 1);

        const [orderId] =
            [...orderIds] as string[];

        assert.equal(
            await countRows(
                "SELECT count(*) FROM orders WHERE idempotency_key = $1",
                [key]
            ),
            1
        );
        assert.equal(
            await countRows(
                `
                    SELECT count(*)
                    FROM order_outbox
                    WHERE payload -> 'data' ->> 'orderId' = $1
                `,
                [orderId]
            ),
            1
        );
        assert.equal(
            await countRows(
                "SELECT count(*) FROM order_timeline WHERE order_id = $1 AND kind = 'Replayed'",
                [orderId]
            ),
            19
        );
    }
);

test(
    "a key reused for a different order is rejected, also under concurrency",
    async () => {
        const key =
            `key-${randomUUID()}`;

        const outcomes =
            await Promise.all([
                store.submitOrder(newOrder(key, 1)),
                store.submitOrder(newOrder(key, 2)),
                store.submitOrder(newOrder(key, 3))
            ]);

        assert.equal(outcomes.filter(outcome => outcome.kind === "Created").length, 1);
        assert.equal(outcomes.filter(outcome => outcome.kind === "KeyReused").length, 2);
        assert.equal(
            await countRows(
                "SELECT count(*) FROM orders WHERE idempotency_key = $1",
                [key]
            ),
            1
        );
    }
);

function workflowEvent(
    orderId: string,
    eventType: "PaymentAuthorized" | "InventoryReserved" | "InventoryReservationFailed" | "DeliveryBooked",
    eventId = randomUUID()
) {
    return {
        eventId,
        eventType,
        orderId,
        correlationId: "integration",
        occurredAt: new Date().toISOString(),
        data: {
            orderId
        }
    };
}

test(
    "concurrent redeliveries of one event are recorded once and counted",
    async () => {
        const order =
            newOrder(`key-${randomUUID()}`);

        await store.submitOrder(order);

        const orderId =
            order.event.data.orderId;

        const payment =
            workflowEvent(orderId, "PaymentAuthorized");

        const outcomes =
            await Promise.all(
                Array.from(
                    {
                        length: 10
                    },
                    () =>
                        store.recordWorkflowEvent(
                            payment
                        )
                )
            );

        assert.equal(outcomes.filter(outcome => outcome.kind === "Recorded").length, 1);
        assert.equal(outcomes.filter(outcome => outcome.kind === "Duplicate").length, 9);

        const details =
            await store.getOrderDetails(orderId);

        const events =
            details?.timeline.filter(row => row.kind === "Event") ?? [];

        assert.equal(events.length, 1);
        assert.equal(events[0]?.deliveries, 10);
        assert.equal(details?.order.status, "PaymentAuthorized");
    }
);

test(
    "out-of-order events never move the status backwards",
    async () => {
        const order =
            newOrder(`key-${randomUUID()}`);

        await store.submitOrder(order);

        const orderId =
            order.event.data.orderId;

        await Promise.all([
            store.recordWorkflowEvent(workflowEvent(orderId, "DeliveryBooked")),
            store.recordWorkflowEvent(workflowEvent(orderId, "PaymentAuthorized")),
            store.recordWorkflowEvent(workflowEvent(orderId, "InventoryReserved"))
        ]);

        const details =
            await store.getOrderDetails(orderId);

        assert.equal(details?.order.status, "DeliveryBooked");
        assert.equal(details?.timeline.filter(row => row.kind === "Event").length, 3);
    }
);

test(
    "events for unknown or malformed order ids are ignored",
    async () => {
        assert.deepEqual(
            await store.recordWorkflowEvent(
                workflowEvent(randomUUID(), "PaymentAuthorized")
            ),
            {
                kind: "UnknownOrder"
            }
        );

        assert.deepEqual(
            await store.recordWorkflowEvent(
                workflowEvent("not-a-uuid", "PaymentAuthorized")
            ),
            {
                kind: "UnknownOrder"
            }
        );

        assert.equal(await store.getOrderDetails("not-a-uuid"), undefined);
    }
);

async function postOrder(
    baseUrl: string,
    key: string
): Promise<Response> {
    return fetch(
        `${baseUrl}/orders`,
        {
            method: "POST",
            headers: {
                "content-type": "application/json",
                "idempotency-key": key
            },
            body:
                JSON.stringify({
                    customerId: "customer-http",
                    items: [
                        {
                            productId: "dishwasher-01",
                            quantity: 1,
                            unitPrice: 3999
                        }
                    ]
                })
        }
    );
}

test(
    "over HTTP, concurrent retries of one order answer 201 once and 200 for the rest",
    async () => {
        const app =
            createOrderApp({
                orderService:
                    new OrderService(),
                orderStore:
                    store,
                logger:
                    silentLogger,
                rabbitMqReadinessProbe: {
                    isReady: () => true
                },
                databaseReadinessProbe: {
                    isReady: () => true
                }
            });

        const server =
            app.listen(
                0,
                "127.0.0.1"
            );

        await once(
            server,
            "listening"
        );

        try {
            const {
                port
            } = server.address() as AddressInfo;

            const key =
                `key-${randomUUID()}`;

            const responses =
                await Promise.all(
                    Array.from(
                        {
                            length: 8
                        },
                        () =>
                            postOrder(
                                `http://127.0.0.1:${port}`,
                                key
                            )
                    )
                );

            const statuses =
                responses.map(response => response.status).sort();

            assert.deepEqual(statuses, [200, 200, 200, 200, 200, 200, 200, 201]);

            const bodies =
                await Promise.all(
                    responses.map(response => response.json())
                );

            assert.equal(
                new Set(bodies.map(body => body.orderId)).size,
                1
            );
        } finally {
            server.close();
        }
    }
);

async function countQueueMessages(
    url: string,
    queueName: string
): Promise<number> {
    const connection =
        await amqp.connect(url);

    try {
        const channel =
            await connection.createChannel();

        return (await channel.checkQueue(queueName)).messageCount;
    } finally {
        await connection.close();
    }
}

async function publishPendingOrders(
    url: string,
    topology: readonly QueueBinding[]
): Promise<number> {
    const rabbitMq =
        new RabbitMqClient(
            url,
            "commerce.events",
            {
                maxConnectionRetries: 3,
                retryDelayInMs: 50,
                publishTimeoutInMs: 3_000,
                topology
            },
            {
                logger:
                    silentLogger
            }
        );

    try {
        await rabbitMq.connect();

        const relay =
            new OutboxRelay(
                {
                    store:
                        new PostgresOutboxStore(
                            pool,
                            ORDER_OUTBOX_TABLE
                        ),
                    publisher:
                        new RabbitMqOutboxPublisher(
                            rabbitMq
                        ),
                    logger:
                        silentLogger
                },
                {
                    batchSize: 500,
                    retryBaseDelayInMs: 60_000,
                    retryMaxDelayInMs: 60_000
                }
            );

        return await relay.processBatch();
    } finally {
        await rabbitMq.close();
    }
}

test(
    "the outbox relay publishes OrderCreated to the Payment queue and the timeline shows the confirmation",
    {
        timeout: 30_000
    },
    async () => {
        const virtualHost =
            await createTestVirtualHost();

        try {
            const order =
                newOrder(`key-${randomUUID()}`);

            await store.submitOrder(order);

            await publishPendingOrders(
                virtualHost.url,
                COMMERCE_TOPOLOGY
            );

            const details =
                await store.getOrderDetails(
                    order.event.data.orderId
                );

            assert.notEqual(details?.publication?.publishedAt, null);
            assert.ok(
                (await countQueueMessages(
                    virtualHost.url,
                    QUEUES.paymentOrderCreated.queueName
                )) >= 1
            );
        } finally {
            await virtualHost.delete();
        }
    }
);

test(
    "an OrderCreated event the broker cannot route stays in the outbox with the error, and the order is kept",
    {
        timeout: 30_000
    },
    async () => {
        const virtualHost =
            await createTestVirtualHost();

        try {
            const order =
                newOrder(`key-${randomUUID()}`);

            await store.submitOrder(order);

            // No queues bound: the broker returns the mandatory message.
            await publishPendingOrders(
                virtualHost.url,
                []
            );

            const details =
                await store.getOrderDetails(
                    order.event.data.orderId
                );

            assert.equal(details?.order.status, "Created");
            assert.equal(details?.publication?.publishedAt, null);
            assert.ok((details?.publication?.attempts ?? 0) >= 1);
            assert.match(details?.publication?.lastError ?? "", /.+/);
        } finally {
            await virtualHost.delete();
        }
    }
);
