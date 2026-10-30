import {
    getRoutingKey,
    type OrderItem
} from "@commerce-flow/contracts";
import {
    withTransaction
} from "@commerce-flow/persistence";
import type {
    Pool,
    PoolClient
} from "pg";
import {
    ORDER_OUTBOX_TABLE
} from "./database.js";
import {
    applyWorkflowEvent,
    isOrderStatus,
    type OrderStatus
} from "./orderStatus.js";
import type {
    NewOrder,
    OrderDetails,
    OrderPublication,
    OrderStore,
    OrderWorkflowEventRecord,
    RecordWorkflowEventOutcome,
    StoredOrder,
    SubmitOrderOutcome,
    TimelineRow,
    TimelineRowKind
} from "./orderStore.js";

interface OrderRow {
    orderId: string;
    customerId: string;
    items: OrderItem[];
    totalAmount: string;
    correlationId: string;
    status: string;
    createdAt: Date;
    updatedAt: Date;
    requestFingerprint: string;
    createdEventId: string;
}

interface TimelineDatabaseRow {
    kind: TimelineRowKind;
    eventId: string | null;
    eventType: string | null;
    occurredAt: Date;
    recordedAt: Date;
    deliveries: number;
    details: unknown;
}

interface PublicationRow {
    eventId: string;
    createdAt: Date;
    publishedAt: Date | null;
    attempts: number;
    lastError: string | null;
}

const ORDER_COLUMNS = `
    order_id::text AS "orderId",
    customer_id AS "customerId",
    items,
    total_amount::text AS "totalAmount",
    correlation_id AS "correlationId",
    status,
    created_at AS "createdAt",
    updated_at AS "updatedAt",
    request_fingerprint AS "requestFingerprint",
    created_event_id::text AS "createdEventId"
`;

const UUID_PATTERN =
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export class PostgresOrderStore
    implements OrderStore {
    constructor(
        private readonly pool: Pool
    ) {
    }

    /**
     * One transaction: the order, its OrderCreated outbox event and the
     * first timeline entry are committed together or not at all.
     *
     * The unique index on idempotency_key decides between concurrent
     * submissions with the same key: INSERT ... ON CONFLICT DO NOTHING
     * waits for the other transaction, and then either inserts (the
     * other one rolled back) or finds the committed order.
     */
    async submitOrder(
        order: NewOrder
    ): Promise<SubmitOrderOutcome> {
        return withTransaction(
            this.pool,
            async client => {
                const inserted =
                    await client.query<OrderRow>(
                        `
                            INSERT INTO orders (
                                order_id,
                                idempotency_key,
                                request_fingerprint,
                                customer_id,
                                items,
                                total_amount,
                                correlation_id,
                                created_event_id,
                                status
                            )
                            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, 'Created')
                            ON CONFLICT (idempotency_key) DO NOTHING
                            RETURNING ${ORDER_COLUMNS}
                        `,
                        [
                            order.event.data.orderId,
                            order.idempotencyKey,
                            order.requestFingerprint,
                            order.customerId,
                            JSON.stringify(
                                order.items
                            ),
                            order.totalAmount,
                            order.correlationId,
                            order.event.eventId
                        ]
                    );

                const created =
                    inserted.rows[0];

                if (created) {
                    await insertOutboxEvent(
                        client,
                        order
                    );

                    await client.query(
                        `
                            INSERT INTO order_timeline (order_id, kind, occurred_at)
                            VALUES ($1, 'Accepted', $2)
                        `,
                        [
                            created.orderId,
                            created.createdAt
                        ]
                    );

                    return {
                        kind: "Created",
                        order:
                            toStoredOrder(
                                created
                            )
                    };
                }

                const existing =
                    await client.query<OrderRow>(
                        `
                            SELECT ${ORDER_COLUMNS}
                            FROM orders
                            WHERE idempotency_key = $1
                        `,
                        [
                            order.idempotencyKey
                        ]
                    );

                const stored =
                    existing.rows[0];

                if (!stored) {
                    // Only possible if the row disappeared between the
                    // conflict and this read; orders are never deleted.
                    throw new Error(
                        "The order for this Idempotency-Key could not be read."
                    );
                }

                if (stored.requestFingerprint !== order.requestFingerprint) {
                    return {
                        kind: "KeyReused"
                    };
                }

                await client.query(
                    `
                        INSERT INTO order_timeline (order_id, kind, occurred_at)
                        VALUES ($1, 'Replayed', clock_timestamp())
                    `,
                    [
                        stored.orderId
                    ]
                );

                return {
                    kind: "Replayed",
                    order:
                        toStoredOrder(
                            stored
                        )
                };
            }
        );
    }

    /**
     * Records an event once per event id and moves the status forward.
     * The order row is locked first, so concurrent events for the same
     * order are applied one after the other.
     */
    async recordWorkflowEvent(
        event: OrderWorkflowEventRecord
    ): Promise<RecordWorkflowEventOutcome> {
        if (!UUID_PATTERN.test(event.orderId)) {
            return {
                kind: "UnknownOrder"
            };
        }

        return withTransaction(
            this.pool,
            async client => {
                const locked =
                    await client.query<{
                        status: string;
                    }>(
                        `
                            SELECT status
                            FROM orders
                            WHERE order_id = $1
                            FOR UPDATE
                        `,
                        [
                            event.orderId
                        ]
                    );

                const current =
                    locked.rows[0]?.status;

                if (
                    current === undefined ||
                    !isOrderStatus(current)
                ) {
                    return {
                        kind: "UnknownOrder"
                    };
                }

                const recorded =
                    await client.query<{
                        deliveries: number;
                        inserted: boolean;
                    }>(
                        `
                            INSERT INTO order_timeline (
                                order_id,
                                kind,
                                event_id,
                                event_type,
                                occurred_at,
                                details
                            )
                            VALUES ($1, 'Event', $2, $3, $4, $5::jsonb)
                            ON CONFLICT (event_id) DO UPDATE
                                SET deliveries = order_timeline.deliveries + 1
                            RETURNING deliveries, (xmax = 0) AS inserted
                        `,
                        [
                            event.orderId,
                            event.eventId,
                            event.eventType,
                            event.occurredAt,
                            JSON.stringify(
                                event.data
                            )
                        ]
                    );

                const row =
                    recorded.rows[0]!;

                if (!row.inserted) {
                    return {
                        kind: "Duplicate",
                        deliveries:
                            row.deliveries
                    };
                }

                const status =
                    applyWorkflowEvent(
                        current,
                        event.eventType
                    );

                if (status !== current) {
                    await client.query(
                        `
                            UPDATE orders
                            SET status = $2, updated_at = clock_timestamp()
                            WHERE order_id = $1
                        `,
                        [
                            event.orderId,
                            status
                        ]
                    );
                }

                return {
                    kind: "Recorded",
                    status
                };
            }
        );
    }

    async getOrderDetails(
        orderId: string
    ): Promise<OrderDetails | undefined> {
        if (!UUID_PATTERN.test(orderId)) {
            return undefined;
        }

        const orderResult =
            await this.pool.query<OrderRow>(
                `
                    SELECT ${ORDER_COLUMNS}
                    FROM orders
                    WHERE order_id = $1
                `,
                [
                    orderId
                ]
            );

        const order =
            orderResult.rows[0];

        if (!order) {
            return undefined;
        }

        const [
            timelineResult,
            publicationResult
        ] =
            await Promise.all([
                this.pool.query<TimelineDatabaseRow>(
                    `
                        SELECT
                            kind,
                            event_id AS "eventId",
                            event_type AS "eventType",
                            occurred_at AS "occurredAt",
                            recorded_at AS "recordedAt",
                            deliveries,
                            details
                        FROM order_timeline
                        WHERE order_id = $1
                        ORDER BY entry_id
                    `,
                    [
                        orderId
                    ]
                ),
                this.pool.query<PublicationRow>(
                    `
                        SELECT
                            event_id::text AS "eventId",
                            created_at AS "createdAt",
                            published_at AS "publishedAt",
                            attempts,
                            last_error AS "lastError"
                        FROM ${ORDER_OUTBOX_TABLE}
                        WHERE event_id = $1
                    `,
                    [
                        order.createdEventId
                    ]
                )
            ]);

        return {
            order:
                toStoredOrder(
                    order
                ),
            timeline:
                timelineResult.rows.map(
                    toTimelineRow
                ),
            publication:
                toPublication(
                    publicationResult.rows[0]
                )
        };
    }

    async listRecentOrders(
        limit: number
    ): Promise<StoredOrder[]> {
        const result =
            await this.pool.query<OrderRow>(
                `
                    SELECT ${ORDER_COLUMNS}
                    FROM orders
                    ORDER BY created_at DESC, order_id
                    LIMIT $1
                `,
                [
                    limit
                ]
            );

        return result.rows.map(
            toStoredOrder
        );
    }
}

async function insertOutboxEvent(
    client: PoolClient,
    order: NewOrder
): Promise<void> {
    await client.query(
        `
            INSERT INTO ${ORDER_OUTBOX_TABLE} (
                event_id,
                event_type,
                routing_key,
                payload,
                occurred_at
            )
            VALUES ($1, $2, $3, $4::jsonb, $5)
        `,
        [
            order.event.eventId,
            order.event.eventType,
            getRoutingKey(
                order.event.eventType
            ),
            JSON.stringify(
                order.event
            ),
            order.event.occurredAt
        ]
    );
}

function toStoredOrder(
    row: OrderRow
): StoredOrder {
    return {
        orderId:
            row.orderId,
        customerId:
            row.customerId,
        items:
            row.items,
        totalAmount:
            Number(
                row.totalAmount
            ),
        correlationId:
            row.correlationId,
        status:
            row.status as OrderStatus,
        createdAt:
            row.createdAt.toISOString(),
        updatedAt:
            row.updatedAt.toISOString()
    };
}

function toTimelineRow(
    row: TimelineDatabaseRow
): TimelineRow {
    return {
        kind:
            row.kind,
        eventId:
            row.eventId,
        eventType:
            row.eventType,
        occurredAt:
            row.occurredAt.toISOString(),
        recordedAt:
            row.recordedAt.toISOString(),
        deliveries:
            row.deliveries,
        details:
            row.details
    };
}

function toPublication(
    row: PublicationRow | undefined
): OrderPublication | null {
    return row
        ? {
            eventId:
                row.eventId,
            createdAt:
                row.createdAt.toISOString(),
            publishedAt:
                row.publishedAt?.toISOString() ??
                null,
            attempts:
                row.attempts,
            lastError:
                row.lastError
        }
        : null;
}
