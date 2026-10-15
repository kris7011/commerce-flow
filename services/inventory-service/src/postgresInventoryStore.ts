import {
    getRoutingKey,
    type OrderItem
} from "@commerce-flow/contracts";
import type {
    Pool,
    PoolClient
} from "pg";
import type {
    InventoryResultEvent,
    InventoryStockReader,
    InventoryTransaction,
    InventoryUnitOfWork,
    ReservationRecord,
    ReservationStatus
} from "./inventoryStore.js";

interface InventoryStockRow {
    productId: string;
    quantity: number;
}

interface ReservationRow {
    orderId: string;
    status: ReservationStatus;
    reservationId: string | null;
    sourceEventId: string;
    resultEventId: string;
    items: OrderItem[];
}

const ORDER_LOCK_NAMESPACE =
    "commerce-flow.inventory-order:";

export class PostgresInventoryStore
    implements InventoryUnitOfWork, InventoryStockReader {
    constructor(
        private readonly pool: Pool
    ) {
    }

    async inTransaction<T>(
        work: (
            transaction: InventoryTransaction
        ) => Promise<T>
    ): Promise<T> {
        const client =
            await this.pool.connect();

        let connectionBroken:
            Error | undefined;

        // A checked-out client that loses its connection emits
        // "error"; without a listener that would crash the process.
        // The in-flight query rejects as well, which aborts the work.
        const onConnectionError =
            (error: Error): void => {
                connectionBroken =
                    error;
            };

        client.on(
            "error",
            onConnectionError
        );

        try {
            await client.query(
                "BEGIN"
            );

            const result =
                await work(
                    new PostgresInventoryTransaction(
                        client
                    )
                );

            await client.query(
                "COMMIT"
            );

            return result;
        } catch (error) {
            try {
                await client.query(
                    "ROLLBACK"
                );
            } catch (rollbackError) {
                // The connection is unusable; make sure the pool
                // discards it instead of handing it out again.
                connectionBroken =
                    rollbackError instanceof Error
                        ? rollbackError
                        : new Error(
                            "ROLLBACK failed"
                        );
            }

            throw error;
        } finally {
            client.off(
                "error",
                onConnectionError
            );

            client.release(
                connectionBroken
            );
        }
    }

    async getAllStock(): Promise<
        Readonly<Record<string, number>>
    > {
        const result =
            await this.pool
                .query<InventoryStockRow>(
                    `
                        SELECT
                            product_id AS "productId",
                            quantity
                        FROM inventory_stock
                        ORDER BY product_id
                    `
                );

        return Object.fromEntries(
            result.rows.map(
                row => [
                    row.productId,
                    row.quantity
                ]
            )
        );
    }
}

class PostgresInventoryTransaction
    implements InventoryTransaction {
    constructor(
        private readonly client: PoolClient
    ) {
    }

    async lockOrder(
        orderId: string
    ): Promise<ReservationRecord | null> {
        // Concurrent deliveries for the same order queue up here
        // until the first transaction commits or rolls back, so
        // they observe its outcome instead of racing it. The
        // primary key on inventory_reservations is the backstop.
        await this.client.query(
            `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`,
            [
                `${ORDER_LOCK_NAMESPACE}${orderId}`
            ]
        );

        const result =
            await this.client
                .query<ReservationRow>(
                    `
                        SELECT
                            order_id AS "orderId",
                            status,
                            reservation_id AS "reservationId",
                            source_event_id AS "sourceEventId",
                            result_event_id AS "resultEventId",
                            items
                        FROM inventory_reservations
                        WHERE order_id = $1
                    `,
                    [
                        orderId
                    ]
                );

        return result.rows[0] ?? null;
    }

    async lockStock(
        productIds: readonly string[]
    ): Promise<ReadonlyMap<string, number>> {
        // A consistent lock order prevents deadlocks between
        // reservations that touch overlapping products.
        const result =
            await this.client
                .query<InventoryStockRow>(
                    `
                        SELECT
                            product_id AS "productId",
                            quantity
                        FROM inventory_stock
                        WHERE product_id = ANY($1::text[])
                        ORDER BY product_id
                        FOR UPDATE
                    `,
                    [
                        productIds
                    ]
                );

        return new Map(
            result.rows.map(
                row => [
                    row.productId,
                    row.quantity
                ]
            )
        );
    }

    async decrementStock(
        quantityByProductId:
            ReadonlyMap<string, number>
    ): Promise<void> {
        const productIds = [
            ...quantityByProductId.keys()
        ];

        const result =
            await this.client.query(
                `
                    UPDATE inventory_stock AS stock
                    SET quantity =
                        stock.quantity - requested.quantity
                    FROM UNNEST(
                        $1::text[],
                        $2::integer[]
                    ) AS requested(product_id, quantity)
                    WHERE stock.product_id = requested.product_id
                `,
                [
                    productIds,
                    productIds.map(
                        productId =>
                            quantityByProductId.get(
                                productId
                            ) ??
                            0
                    )
                ]
            );

        if (
            result.rowCount !==
            productIds.length
        ) {
            throw new Error(
                `Expected to update ${productIds.length} stock rows, ` +
                `but updated ${String(result.rowCount)}.`
            );
        }
    }

    async recordReservation(
        record: ReservationRecord
    ): Promise<void> {
        await this.client.query(
            `
                INSERT INTO inventory_reservations (
                    order_id,
                    status,
                    reservation_id,
                    source_event_id,
                    result_event_id,
                    items
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            `,
            [
                record.orderId,
                record.status,
                record.reservationId,
                record.sourceEventId,
                record.resultEventId,
                JSON.stringify(
                    record.items
                )
            ]
        );
    }

    async enqueueEvent(
        event: InventoryResultEvent
    ): Promise<void> {
        await this.client.query(
            `
                INSERT INTO inventory_outbox (
                    event_id,
                    event_type,
                    routing_key,
                    payload,
                    occurred_at
                )
                VALUES ($1, $2, $3, $4::jsonb, $5)
            `,
            [
                event.eventId,
                event.eventType,
                getRoutingKey(
                    event.eventType
                ),
                JSON.stringify(
                    event
                ),
                event.occurredAt
            ]
        );
    }
}
