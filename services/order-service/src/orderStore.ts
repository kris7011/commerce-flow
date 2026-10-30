import type {
    OrderCreatedEvent,
    OrderItem
} from "@commerce-flow/contracts";
import type {
    OrderStatus,
    OrderWorkflowEventType
} from "./orderStatus.js";

/*
 * Persistence port of the Order Service. The PostgreSQL adapter is used
 * in production and integration tests, the in-memory adapter in unit
 * tests; both implement the same idempotency rules.
 */

export interface NewOrder {
    readonly idempotencyKey: string;
    readonly requestFingerprint: string;
    readonly customerId: string;
    readonly items: readonly OrderItem[];
    readonly totalAmount: number;
    readonly correlationId: string;

    /** Written to the outbox in the same transaction as the order. */
    readonly event: OrderCreatedEvent;
}

export interface StoredOrder {
    readonly orderId: string;
    readonly customerId: string;
    readonly items: readonly OrderItem[];
    readonly totalAmount: number;
    readonly correlationId: string;
    readonly status: OrderStatus;
    readonly createdAt: string;
    readonly updatedAt: string;
}

export type SubmitOrderOutcome =
    /** A new order and its OrderCreated outbox event were committed. */
    | {
        readonly kind: "Created";
        readonly order: StoredOrder;
    }
    /** The same key and the same request: the stored order is returned. */
    | {
        readonly kind: "Replayed";
        readonly order: StoredOrder;
    }
    /** The key was already used for a different request. */
    | {
        readonly kind: "KeyReused";
    };

export interface OrderWorkflowEventRecord {
    readonly eventId: string;
    readonly eventType: OrderWorkflowEventType;
    readonly orderId: string;
    readonly correlationId: string;
    readonly occurredAt: string;

    /** The event's `data`, kept for the timeline (no secrets in events). */
    readonly data: unknown;
}

export type RecordWorkflowEventOutcome =
    | {
        readonly kind: "Recorded";
        readonly status: OrderStatus;
    }
    /** Already recorded under this event id; only the delivery count grew. */
    | {
        readonly kind: "Duplicate";
        readonly deliveries: number;
    }
    | {
        readonly kind: "UnknownOrder";
    };

export type TimelineRowKind =
    | "Accepted"
    | "Replayed"
    | "Event";

/** One row of the order's history, as stored. */
export interface TimelineRow {
    readonly kind: TimelineRowKind;
    readonly eventId: string | null;
    readonly eventType: string | null;

    /** When the producer says it happened (events) or when it was recorded. */
    readonly occurredAt: string;

    /** When the Order Service recorded it. */
    readonly recordedAt: string;

    /** How often the broker delivered this event (at-least-once). */
    readonly deliveries: number;
    readonly details: unknown;
}

export interface OrderPublication {
    readonly eventId: string;
    readonly createdAt: string;
    readonly publishedAt: string | null;
    readonly attempts: number;
    readonly lastError: string | null;
}

export interface OrderDetails {
    readonly order: StoredOrder;
    readonly timeline: readonly TimelineRow[];
    readonly publication: OrderPublication | null;
}

export interface OrderStore {
    submitOrder(
        order: NewOrder
    ): Promise<SubmitOrderOutcome>;

    recordWorkflowEvent(
        event: OrderWorkflowEventRecord
    ): Promise<RecordWorkflowEventOutcome>;

    getOrderDetails(
        orderId: string
    ): Promise<OrderDetails | undefined>;

    listRecentOrders(
        limit: number
    ): Promise<StoredOrder[]>;
}
