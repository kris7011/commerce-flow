import {
    applyWorkflowEvent
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
    TimelineRow
} from "./orderStore.js";

interface OrderEntry {
    order: StoredOrder;
    readonly idempotencyKey: string;
    readonly requestFingerprint: string;
    readonly timeline: TimelineRow[];
    publication: OrderPublication;
}

/**
 * In-memory adapter for unit tests. It follows the PostgreSQL adapter's
 * rules; the integration tests prove those rules against PostgreSQL,
 * including concurrency, which this adapter does not model.
 */
export class InMemoryOrderStore
    implements OrderStore {
    private readonly orders =
        new Map<string, OrderEntry>();

    private readonly orderIdByKey =
        new Map<string, string>();

    constructor(
        private readonly now: () => string =
            () => new Date().toISOString()
    ) {
    }

    async submitOrder(
        order: NewOrder
    ): Promise<SubmitOrderOutcome> {
        const existingId =
            this.orderIdByKey.get(
                order.idempotencyKey
            );

        if (existingId !== undefined) {
            const existing =
                this.requireEntry(
                    existingId
                );

            if (existing.requestFingerprint !== order.requestFingerprint) {
                return {
                    kind: "KeyReused"
                };
            }

            const time =
                this.now();

            existing.timeline.push({
                kind: "Replayed",
                eventId: null,
                eventType: null,
                occurredAt: time,
                recordedAt: time,
                deliveries: 1,
                details: null
            });

            return {
                kind: "Replayed",
                order: existing.order
            };
        }

        const time =
            this.now();

        const stored: StoredOrder = {
            orderId:
                order.event.data.orderId,
            customerId:
                order.customerId,
            items:
                order.items,
            totalAmount:
                order.totalAmount,
            correlationId:
                order.correlationId,
            status:
                "Created",
            createdAt:
                time,
            updatedAt:
                time
        };

        this.orders.set(
            stored.orderId,
            {
                order: stored,
                idempotencyKey:
                    order.idempotencyKey,
                requestFingerprint:
                    order.requestFingerprint,
                timeline: [
                    {
                        kind: "Accepted",
                        eventId: null,
                        eventType: null,
                        occurredAt: time,
                        recordedAt: time,
                        deliveries: 1,
                        details: null
                    }
                ],
                publication: {
                    eventId:
                        order.event.eventId,
                    createdAt:
                        time,
                    publishedAt:
                        null,
                    attempts:
                        0,
                    lastError:
                        null
                }
            }
        );

        this.orderIdByKey.set(
            order.idempotencyKey,
            stored.orderId
        );

        return {
            kind: "Created",
            order: stored
        };
    }

    async recordWorkflowEvent(
        event: OrderWorkflowEventRecord
    ): Promise<RecordWorkflowEventOutcome> {
        const entry =
            this.orders.get(
                event.orderId
            );

        if (!entry) {
            return {
                kind: "UnknownOrder"
            };
        }

        const recorded =
            entry.timeline.findIndex(
                row =>
                    row.eventId === event.eventId
            );

        if (recorded >= 0) {
            const row =
                entry.timeline[recorded]!;

            const deliveries =
                row.deliveries + 1;

            entry.timeline[recorded] = {
                ...row,
                deliveries
            };

            return {
                kind: "Duplicate",
                deliveries
            };
        }

        entry.timeline.push({
            kind: "Event",
            eventId:
                event.eventId,
            eventType:
                event.eventType,
            occurredAt:
                event.occurredAt,
            recordedAt:
                this.now(),
            deliveries:
                1,
            details:
                event.data
        });

        entry.order = {
            ...entry.order,
            status:
                applyWorkflowEvent(
                    entry.order.status,
                    event.eventType
                ),
            updatedAt:
                this.now()
        };

        return {
            kind: "Recorded",
            status:
                entry.order.status
        };
    }

    async getOrderDetails(
        orderId: string
    ): Promise<OrderDetails | undefined> {
        const entry =
            this.orders.get(
                orderId
            );

        return entry && {
            order:
                entry.order,
            timeline: [
                ...entry.timeline
            ],
            publication:
                entry.publication
        };
    }

    async listRecentOrders(
        limit: number
    ): Promise<StoredOrder[]> {
        return [...this.orders.values()]
            .map(entry => entry.order)
            .reverse()
            .slice(0, limit);
    }

    /** Test helper: what the outbox relay records after a broker confirm. */
    markPublished(
        orderId: string,
        publishedAt = this.now()
    ): void {
        const entry =
            this.requireEntry(
                orderId
            );

        entry.publication = {
            ...entry.publication,
            publishedAt,
            attempts:
                entry.publication.attempts + 1
        };
    }

    private requireEntry(
        orderId: string
    ): OrderEntry {
        const entry =
            this.orders.get(
                orderId
            );

        if (!entry) {
            throw new Error(
                `Unknown order ${orderId}.`
            );
        }

        return entry;
    }
}
