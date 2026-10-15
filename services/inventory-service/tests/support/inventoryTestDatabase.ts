import {
    randomUUID
} from "node:crypto";
import type {
    InventoryReservedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    assertConnectedToTestDatabase,
    createTestDatabase,
    type TestDatabase
} from "@commerce-flow/testing";
import type {
    Pool
} from "pg";
import {
    initializeInventoryDatabase
} from "../../src/database.js";
import {
    PostgresInventoryStore
} from "../../src/postgresInventoryStore.js";

export interface InventoryTestDatabase {
    readonly database: TestDatabase;
    readonly pool: Pool;
}

export async function createInventoryTestDatabase():
    Promise<InventoryTestDatabase> {
    const database =
        await createTestDatabase();

    const pool =
        database.createPool({
            max: 20
        });

    await initializeInventoryDatabase(
        pool
    );

    return {
        database,
        pool
    };
}

/**
 * Restores a known state. Refuses to run unless the pool is connected
 * to a dedicated test database.
 */
export async function resetInventoryState(
    pool: Pool,
    stock: Readonly<Record<string, number>>
): Promise<void> {
    await assertConnectedToTestDatabase(
        pool
    );

    await pool.query(
        "TRUNCATE inventory_reservations, inventory_outbox, inventory_stock"
    );

    const entries =
        Object.entries(
            stock
        );

    await pool.query(
        `
            INSERT INTO inventory_stock (product_id, quantity)
            SELECT * FROM UNNEST($1::text[], $2::integer[])
        `,
        [
            entries.map(
                ([productId]) =>
                    productId
            ),
            entries.map(
                ([, quantity]) =>
                    quantity
            )
        ]
    );
}

export function createPaymentAuthorizedEvent(
    orderId: string,
    items: PaymentAuthorizedEvent["data"]["items"],
    eventId: string = randomUUID()
): PaymentAuthorizedEvent {
    return {
        eventId,
        eventType:
            "PaymentAuthorized",
        occurredAt:
            new Date().toISOString(),
        correlationId:
            `correlation-${orderId}`,
        data: {
            orderId,
            paymentId:
                `payment-${orderId}`,
            amount:
                items.reduce(
                    (total, item) =>
                        total +
                        item.quantity *
                        item.unitPrice,
                    0
                ),
            items
        }
    };
}

/** Writes outbox events through the production transaction path. */
export async function enqueueTestEvents(
    pool: Pool,
    count: number
): Promise<InventoryReservedEvent[]> {
    const events =
        Array.from(
            {
                length: count
            },
            (_, index): InventoryReservedEvent => ({
                eventId:
                    randomUUID(),
                eventType:
                    "InventoryReserved",
                occurredAt:
                    new Date().toISOString(),
                correlationId:
                    `correlation-${index}`,
                data: {
                    orderId:
                        `order-${index}`,
                    reservationId:
                        randomUUID(),
                    items: [
                        {
                            productId:
                                "dryer-01",
                            quantity:
                                1,
                            unitPrice:
                                2999
                        }
                    ]
                }
            })
        );

    await new PostgresInventoryStore(
        pool
    ).inTransaction(
        async transaction => {
            for (const event of events) {
                await transaction.enqueueEvent(
                    event
                );
            }
        }
    );

    return events;
}

export async function waitUntil(
    condition: () => Promise<boolean> | boolean,
    timeoutInMs: number,
    description: string
): Promise<void> {
    const deadline =
        Date.now() +
        timeoutInMs;

    while (Date.now() < deadline) {
        if (await condition()) {
            return;
        }

        await new Promise(
            resolve =>
                setTimeout(
                    resolve,
                    25
                )
        );
    }

    throw new Error(
        `Timed out after ${timeoutInMs} ms waiting for ${description}.`
    );
}
