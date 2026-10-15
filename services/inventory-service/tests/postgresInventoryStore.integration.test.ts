import assert from "node:assert/strict";
import test from "node:test";
import {
    InventoryService
} from "../src/inventoryService.js";
import {
    PostgresInventoryStore
} from "../src/postgresInventoryStore.js";
import {
    createInventoryTestDatabase,
    createPaymentAuthorizedEvent,
    resetInventoryState
} from "./support/inventoryTestDatabase.js";

const {
    database,
    pool
} = await createInventoryTestDatabase();

const store =
    new PostgresInventoryStore(
        pool
    );

const service =
    new InventoryService(
        store
    );

test.beforeEach(
    async () => {
        await resetInventoryState(
            pool,
            {
                "washing-machine-01": 10,
                "dishwasher-01": 5,
                "dryer-01": 3
            }
        );
    }
);

test.after(
    async () => {
        await database.drop();
    }
);

interface OutboxRow {
    eventId: string;
    eventType: string;
    routingKey: string;
    payload: {
        eventId: string;
        data: {
            orderId: string;
        };
    };
    publishedAt: Date | null;
}

async function readOutbox(): Promise<OutboxRow[]> {
    const result =
        await pool.query<OutboxRow>(
            `
                SELECT
                    event_id::text AS "eventId",
                    event_type AS "eventType",
                    routing_key AS "routingKey",
                    payload,
                    published_at AS "publishedAt"
                FROM inventory_outbox
                ORDER BY sequence_id
            `
        );

    return result.rows;
}

async function countReservations(): Promise<number> {
    const result =
        await pool.query<{
            count: string;
        }>(
            "SELECT count(*) AS count FROM inventory_reservations"
        );

    return Number(
        result.rows[0]?.count
    );
}

test(
    "commits the stock change, reservation and outbox event together",
    async () => {
        const sourceEvent =
            createPaymentAuthorizedEvent(
                "order-atomic",
                [
                    {
                        productId: "washing-machine-01",
                        quantity: 2,
                        unitPrice: 4999
                    },
                    {
                        productId: "dryer-01",
                        quantity: 1,
                        unitPrice: 2999
                    }
                ]
            );

        const outcome =
            await service.reserveForPayment(
                sourceEvent
            );

        assert.equal(
            outcome.status,
            "Processed"
        );

        assert.deepEqual(
            await store.getAllStock(),
            {
                "dishwasher-01": 5,
                "dryer-01": 2,
                "washing-machine-01": 8
            }
        );

        const outbox =
            await readOutbox();

        assert.equal(
            outbox.length,
            1
        );

        assert.equal(
            outbox[0]?.eventType,
            "InventoryReserved"
        );

        assert.equal(
            outbox[0]?.routingKey,
            "inventory.reserved"
        );

        assert.equal(
            outbox[0]?.publishedAt,
            null
        );

        assert.equal(
            outbox[0]?.payload.data.orderId,
            "order-atomic"
        );

        const reservation =
            await pool.query<{
                status: string;
                sourceEventId: string;
                resultEventId: string;
            }>(
                `
                    SELECT
                        status,
                        source_event_id AS "sourceEventId",
                        result_event_id::text AS "resultEventId"
                    FROM inventory_reservations
                    WHERE order_id = 'order-atomic'
                `
            );

        assert.deepEqual(
            reservation.rows,
            [
                {
                    status: "Reserved",
                    sourceEventId: sourceEvent.eventId,
                    resultEventId: outbox[0]?.eventId
                }
            ]
        );
    }
);

test(
    "durably records InventoryReservationFailed without modifying stock",
    async () => {
        await service.reserveForPayment(
            createPaymentAuthorizedEvent(
                "order-insufficient",
                [
                    {
                        productId: "washing-machine-01",
                        quantity: 2,
                        unitPrice: 4999
                    },
                    {
                        productId: "dryer-01",
                        quantity: 4,
                        unitPrice: 2999
                    }
                ]
            )
        );

        assert.deepEqual(
            await store.getAllStock(),
            {
                "dishwasher-01": 5,
                "dryer-01": 3,
                "washing-machine-01": 10
            }
        );

        const outbox =
            await readOutbox();

        assert.deepEqual(
            outbox.map(
                row => [
                    row.eventType,
                    row.routingKey
                ]
            ),
            [
                [
                    "InventoryReservationFailed",
                    "inventory.reservation.failed"
                ]
            ]
        );
    }
);

test(
    "rolls back stock and outbox writes when the transaction fails",
    async () => {
        await assert.rejects(
            store.inTransaction(
                async transaction => {
                    await transaction.decrementStock(
                        new Map([
                            [
                                "dryer-01",
                                2
                            ]
                        ])
                    );

                    await transaction.enqueueEvent(
                        createPaymentAuthorizedEvent(
                            "unused",
                            []
                        ) as never
                    );

                    throw new Error(
                        "Simulated failure after writes"
                    );
                }
            ),
            /Simulated failure/
        );

        assert.equal(
            (await store.getAllStock())["dryer-01"],
            3
        );

        assert.deepEqual(
            await readOutbox(),
            []
        );
    }
);

test(
    "rejects a stock update that would go below zero",
    async () => {
        await assert.rejects(
            store.inTransaction(
                transaction =>
                    transaction.decrementStock(
                        new Map([
                            [
                                "dryer-01",
                                4
                            ]
                        ])
                    )
            ),
            /inventory_stock_quantity_nonnegative/
        );

        assert.equal(
            (await store.getAllStock())["dryer-01"],
            3
        );
    }
);

test(
    "never oversells under concurrent reservations for different orders",
    async () => {
        const outcomes =
            await Promise.all(
                Array.from(
                    {
                        length: 12
                    },
                    (_, index) =>
                        service.reserveForPayment(
                            createPaymentAuthorizedEvent(
                                `order-concurrent-${index}`,
                                [
                                    {
                                        productId: "dryer-01",
                                        quantity: 1,
                                        unitPrice: 2999
                                    },
                                    {
                                        productId: "dishwasher-01",
                                        quantity: 1,
                                        unitPrice: 3499
                                    }
                                ]
                            )
                        )
                )
            );

        const eventTypes =
            outcomes.map(
                outcome =>
                    outcome.status === "Processed"
                        ? outcome.event.eventType
                        : "Duplicate"
            );

        assert.equal(
            eventTypes.filter(
                type =>
                    type === "InventoryReserved"
            ).length,
            3
        );

        assert.equal(
            eventTypes.filter(
                type =>
                    type === "InventoryReservationFailed"
            ).length,
            9
        );

        assert.deepEqual(
            await store.getAllStock(),
            {
                "dishwasher-01": 2,
                "dryer-01": 0,
                "washing-machine-01": 10
            }
        );

        assert.equal(
            (await readOutbox()).length,
            12
        );
    }
);

test(
    "processes concurrent redeliveries of the same event exactly once",
    async () => {
        const sourceEvent =
            createPaymentAuthorizedEvent(
                "order-redelivered",
                [
                    {
                        productId: "dryer-01",
                        quantity: 1,
                        unitPrice: 2999
                    }
                ]
            );

        const outcomes =
            await Promise.all(
                Array.from(
                    {
                        length: 10
                    },
                    () =>
                        service.reserveForPayment(
                            sourceEvent
                        )
                )
            );

        assert.equal(
            outcomes.filter(
                outcome =>
                    outcome.status === "Processed"
            ).length,
            1
        );

        assert.equal(
            (await store.getAllStock())["dryer-01"],
            2
        );

        assert.equal(
            (await readOutbox()).length,
            1
        );

        assert.equal(
            await countReservations(),
            1
        );
    }
);

test(
    "deduplicates the same order even when duplicates carry different event ids",
    async () => {
        const items = [
            {
                productId: "dryer-01",
                quantity: 1,
                unitPrice: 2999
            }
        ];

        const outcomes =
            await Promise.all(
                Array.from(
                    {
                        length: 5
                    },
                    () =>
                        service.reserveForPayment(
                            createPaymentAuthorizedEvent(
                                "order-reemitted",
                                items
                            )
                        )
                )
            );

        assert.equal(
            outcomes.filter(
                outcome =>
                    outcome.status === "Duplicate"
            ).length,
            4
        );

        assert.equal(
            (await store.getAllStock())["dryer-01"],
            2
        );

        assert.equal(
            (await readOutbox()).length,
            1
        );
    }
);

test(
    "keeps the primary key as a backstop when the order lock is bypassed",
    async () => {
        const record = {
            orderId: "order-backstop",
            status: "Rejected" as const,
            reservationId: null,
            sourceEventId: "source-1",
            items: []
        };

        await store.inTransaction(
            transaction =>
                transaction.recordReservation({
                    ...record,
                    resultEventId:
                        "00000000-0000-4000-8000-000000000001"
                })
        );

        await assert.rejects(
            store.inTransaction(
                transaction =>
                    transaction.recordReservation({
                        ...record,
                        resultEventId:
                            "00000000-0000-4000-8000-000000000002"
                    })
            ),
            (error: unknown) =>
                typeof error === "object" &&
                error !== null &&
                Reflect.get(error, "code") === "23505"
        );
    }
);

test(
    "recovers after the database connection is terminated mid-transaction",
    async () => {
        await assert.rejects(
            store.inTransaction(
                async transaction => {
                    await transaction.decrementStock(
                        new Map([
                            [
                                "dryer-01",
                                1
                            ]
                        ])
                    );

                    // Simulates PostgreSQL dropping the connection.
                    await pool.query(
                        `
                            SELECT pg_terminate_backend(pid)
                            FROM pg_stat_activity
                            WHERE
                                datname = current_database()
                                AND pid <> pg_backend_pid()
                                AND state = 'idle in transaction'
                        `
                    );

                    await transaction.lockStock([
                        "dryer-01"
                    ]);
                }
            )
        );

        assert.equal(
            (await store.getAllStock())["dryer-01"],
            3,
            "The interrupted transaction must not be committed."
        );

        const outcome =
            await service.reserveForPayment(
                createPaymentAuthorizedEvent(
                    "order-after-interruption",
                    [
                        {
                            productId: "dryer-01",
                            quantity: 1,
                            unitPrice: 2999
                        }
                    ]
                )
            );

        assert.equal(
            outcome.status,
            "Processed"
        );

        assert.equal(
            (await store.getAllStock())["dryer-01"],
            2
        );
    }
);
