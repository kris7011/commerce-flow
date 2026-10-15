import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import {
    ROUTING_KEYS,
    type PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import * as amqp from "amqplib";
import {
    InventoryService
} from "../../services/inventory-service/src/inventoryService.js";
import {
    PostgresInventoryStore
} from "../../services/inventory-service/src/postgresInventoryStore.js";
import {
    createE2eEnvironment
} from "./support/environment.js";
import {
    ServiceCluster,
    createOrder,
    getNotifications,
    getStock,
    waitForNotification,
    waitUntil
} from "./support/serviceCluster.js";

const environment =
    await createE2eEnvironment();

const cluster =
    new ServiceCluster(
        environment,
        3300
    );

const pool =
    environment.database.createPool();

test.before(
    async () => {
        await cluster.startAll();
    }
);

test.after(
    async () => {
        await cluster.stopAll();
        await pool.end();
        await environment.dispose();
    }
);

async function withDiagnostics(
    description: string,
    action: () => Promise<void>
): Promise<void> {
    try {
        await action();
    } catch (error) {
        throw new Error(
            `${description} failed.\n\n${cluster.formatLogs()}`,
            {
                cause: error
            }
        );
    }
}

async function publishToBroker(
    routingKey: string,
    content: Buffer,
    messageId: string
): Promise<void> {
    const connection =
        await amqp.connect(
            environment.rabbitMqUrl
        );

    try {
        const channel =
            await connection.createConfirmChannel();

        channel.publish(
            "commerce.events",
            routingKey,
            content,
            {
                persistent: true,
                contentType: "application/json",
                messageId
            }
        );

        await channel.waitForConfirms();
        await channel.close();
    } finally {
        await connection.close();
    }
}

async function countPendingOutboxEvents(
    orderId: string
): Promise<number> {
    const result =
        await pool.query<{
            count: string;
        }>(
            `
                SELECT count(*) AS count
                FROM inventory_outbox
                WHERE
                    published_at IS NULL
                    AND payload -> 'data' ->> 'orderId' = $1
            `,
            [
                orderId
            ]
        );

    return Number(
        result.rows[0]?.count
    );
}

function createPaymentAuthorizedEvent(
    orderId: string,
    correlationId: string,
    eventId: string = randomUUID()
): PaymentAuthorizedEvent {
    return {
        eventId,
        eventType: "PaymentAuthorized",
        occurredAt: new Date().toISOString(),
        correlationId,
        data: {
            orderId,
            paymentId: `payment-${orderId}`,
            amount: 4999,
            items: [
                {
                    productId: "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                }
            ]
        }
    };
}

test(
    "never reserves stock twice for redelivered or re-emitted payment events",
    {
        timeout: 60_000
    },
    async () => {
        await withDiagnostics(
            "Duplicate delivery test",
            async () => {
                const correlationId =
                    `e2e-duplicate-${randomUUID()}`;

                const order =
                    await createOrder(
                        cluster.url(
                            "order-service"
                        ),
                        correlationId,
                        [
                            {
                                productId: "washing-machine-01",
                                quantity: 1,
                                unitPrice: 4999
                            }
                        ]
                    );

                await waitForNotification(
                    cluster.url(
                        "notification-service"
                    ),
                    correlationId,
                    "DeliveryBooked",
                    20_000
                );

                const stockAfterOrder =
                    await getStock(
                        cluster.url(
                            "inventory-service"
                        )
                    );

                // A logically duplicate event with a new event id, sent
                // twice (the second copy is a plain redelivery).
                const duplicate =
                    createPaymentAuthorizedEvent(
                        order.orderId,
                        correlationId
                    );

                for (let copy = 0; copy < 2; copy += 1) {
                    await publishToBroker(
                        ROUTING_KEYS.PaymentAuthorized,
                        Buffer.from(
                            JSON.stringify(
                                duplicate
                            )
                        ),
                        duplicate.eventId
                    );
                }

                await cluster.waitForLog(
                    "inventory-service",
                    "Ignored duplicate PaymentAuthorized for an already processed order",
                    context =>
                        context.eventId === duplicate.eventId,
                    15_000
                );

                assert.deepEqual(
                    await getStock(
                        cluster.url(
                            "inventory-service"
                        )
                    ),
                    stockAfterOrder,
                    "Duplicates must not change stock."
                );

                const notifications =
                    (await getNotifications(
                        cluster.url(
                            "notification-service"
                        )
                    )).filter(
                        notification =>
                            notification.orderId === order.orderId
                    );

                assert.equal(
                    notifications.length,
                    1,
                    "The customer must be notified exactly once."
                );
            }
        );
    }
);

test(
    "dead-letters a poison message without blocking later orders",
    {
        timeout: 60_000
    },
    async () => {
        await withDiagnostics(
            "Poison message test",
            async () => {
                const poisonMessageId =
                    randomUUID();

                await publishToBroker(
                    ROUTING_KEYS.PaymentAuthorized,
                    Buffer.from(
                        "{\"eventType\":\"PaymentAuthorized\",\"data\":"
                    ),
                    poisonMessageId
                );

                const connection =
                    await amqp.connect(
                        environment.rabbitMqUrl
                    );

                try {
                    const channel =
                        await connection.createChannel();

                    await waitUntil(
                        async () =>
                            (await channel.checkQueue(
                                "inventory-service.payment-authorized.dead-letter"
                            )).messageCount === 1,
                        15_000,
                        "the poison message in the dead-letter queue"
                    );

                    const deadLetter =
                        await channel.get(
                            "inventory-service.payment-authorized.dead-letter",
                            {
                                noAck: true
                            }
                        );

                    assert.ok(
                        deadLetter
                    );

                    assert.equal(
                        deadLetter.properties.messageId,
                        poisonMessageId
                    );

                    await channel.close();
                } finally {
                    await connection.close();
                }

                const correlationId =
                    `e2e-after-poison-${randomUUID()}`;

                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    correlationId,
                    [
                        {
                            productId: "dishwasher-01",
                            quantity: 1,
                            unitPrice: 3499
                        }
                    ]
                );

                await waitForNotification(
                    cluster.url(
                        "notification-service"
                    ),
                    correlationId,
                    "DeliveryBooked",
                    20_000
                );
            }
        );
    }
);

test(
    "publishes a reservation committed just before an Inventory Service crash",
    {
        timeout: 90_000
    },
    async () => {
        await withDiagnostics(
            "Outbox crash recovery test",
            async () => {
                // Simulate a crash: no shutdown hooks, nothing drained.
                // An earlier event may still be awaiting its "published"
                // mark; it is re-published after its lease expires,
                // which idempotent consumers absorb.
                await cluster.stop(
                    "inventory-service",
                    "SIGKILL"
                );

                const orderId =
                    `order-${randomUUID()}`;

                const correlationId =
                    `e2e-outbox-recovery-${randomUUID()}`;

                // The exact state a crash right after COMMIT leaves
                // behind: stock reserved, event only in the outbox.
                // Produced through the production transaction code.
                const outcome =
                    await new InventoryService(
                        new PostgresInventoryStore(
                            pool
                        )
                    ).reserveForPayment(
                        createPaymentAuthorizedEvent(
                            orderId,
                            correlationId
                        )
                    );

                assert.equal(
                    outcome.status,
                    "Processed"
                );

                assert.equal(
                    await countPendingOutboxEvents(
                        orderId
                    ),
                    1
                );

                cluster.start(
                    "inventory-service"
                );

                await cluster.waitForReadiness(
                    "inventory-service",
                    30_000
                );

                const notification =
                    await waitForNotification(
                        cluster.url(
                            "notification-service"
                        ),
                        correlationId,
                        "DeliveryBooked",
                        20_000
                    );

                assert.equal(
                    notification.orderId,
                    orderId
                );

                await waitUntil(
                    async () =>
                        (await countPendingOutboxEvents(
                        orderId
                    )) === 0,
                    10_000,
                    "the outbox to be drained"
                );
            }
        );
    }
);
