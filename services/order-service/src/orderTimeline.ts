import {
    getOrderOutcome,
    type OrderOutcome
} from "./orderStatus.js";
import type {
    OrderDetails,
    StoredOrder,
    TimelineRow
} from "./orderStore.js";

/*
 * Turns the stored history of an order into the timeline the API
 * returns and the browser draws. Pure, so it is unit-tested directly.
 *
 * Every entry is something that actually happened and was recorded:
 * the HTTP acceptance, idempotent retries, the broker's confirmation of
 * OrderCreated (from the outbox), and each event consumed from RabbitMQ.
 * Steps performed by simulated services are labelled as simulated.
 *
 * Times: `at` is when the Order Service observed the step, always on the
 * database clock, so offsets between entries are comparable. Events
 * also carry the producer's own `occurredAt`; Payment and Delivery copy
 * it from their source event (to re-emit identical events), so it is not
 * a reliable ordering key. Entries are therefore ordered by workflow
 * stage, which is causal, and retries by when they were received.
 */

export type TimelineEntryKind =
    | "Accepted"
    | "Replayed"
    | "Published"
    | "PublicationPending"
    | "Event";

export interface TimelineEntry {
    readonly kind: TimelineEntryKind;

    /** When the Order Service observed the step (database clock). */
    readonly at: string;
    readonly service: string;
    readonly title: string;
    readonly description: string;
    readonly eventType: string | null;
    readonly eventId: string | null;
    readonly simulated: boolean;

    /** Times the broker delivered the event; above 1 means duplicates. */
    readonly deliveries: number;

    /** The producer's own timestamp, for consumed events. */
    readonly producerTime: string | null;
    readonly outcome: "Success" | "Failure" | "Info" | "Waiting";
}

export interface OrderTimelineView {
    readonly order: StoredOrder;
    readonly outcome: OrderOutcome;
    readonly summary: string;
    readonly entries: readonly TimelineEntry[];
}

const PRODUCERS: Readonly<Record<string, string>> = {
    PaymentAuthorized: "payment-service",
    InventoryReserved: "inventory-service",
    InventoryReservationFailed: "inventory-service",
    DeliveryBooked: "delivery-service"
};

export function buildOrderTimeline(
    details: OrderDetails
): OrderTimelineView {
    const entries: TimelineEntry[] = [];

    for (const row of details.timeline) {
        entries.push(
            toEntry(
                row,
                details.order
            )
        );
    }

    const publication =
        details.publication;

    if (publication?.publishedAt) {
        entries.push({
            kind: "Published",
            at: publication.publishedAt,
            service: "order-service",
            title: "OrderCreated confirmed by RabbitMQ",
            description:
                "The outbox relay published the event and the broker confirmed and " +
                "routed it" +
                (publication.attempts > 1
                    ? ` after ${publication.attempts} attempts.`
                    : "."),
            eventType: "OrderCreated",
            eventId: publication.eventId,
            simulated: false,
            deliveries: 1,
            producerTime: null,
            outcome: "Success"
        });
    } else if (publication) {
        entries.push({
            kind: "PublicationPending",
            at: publication.createdAt,
            service: "order-service",
            title: "OrderCreated waiting in the outbox",
            description:
                publication.lastError
                    ? `Not yet confirmed by the broker (attempt ${publication.attempts}): ` +
                      `${publication.lastError}. The relay keeps retrying; the order is safe.`
                    : "Stored with the order; the outbox relay publishes it shortly.",
            eventType: "OrderCreated",
            eventId: publication.eventId,
            simulated: false,
            deliveries: 1,
            producerTime: null,
            outcome: "Waiting"
        });
    }

    const ordered =
        orderCausally(
            entries
        );

    const outcome =
        getOrderOutcome(
            details.order.status
        );

    return {
        order:
            details.order,
        outcome,
        summary:
            summarize(
                details.order,
                outcome
            ),
        entries:
            ordered
    };
}

function toEntry(
    row: TimelineRow,
    order: StoredOrder
): TimelineEntry {
    switch (row.kind) {
        case "Accepted":
            return {
                kind: "Accepted",
                at: row.occurredAt,
                service: "order-service",
                title: "Order accepted",
                description:
                    `POST /orders answered 201. The order and its OrderCreated event were ` +
                    `stored in one PostgreSQL transaction (total ${formatAmount(order.totalAmount)}).`,
                eventType: null,
                eventId: null,
                simulated: false,
                deliveries: 1,
                producerTime: null,
                outcome: "Success"
            };

        case "Replayed":
            return {
                kind: "Replayed",
                at: row.occurredAt,
                service: "order-service",
                title: "Retry answered from the stored order",
                description:
                    "The same Idempotency-Key and request were sent again. The original order " +
                    "was returned (200); no second order or event was created.",
                eventType: null,
                eventId: null,
                simulated: false,
                deliveries: 1,
                producerTime: null,
                outcome: "Info"
            };

        case "Event":
            return toEventEntry(
                row
            );
    }
}

function toEventEntry(
    row: TimelineRow
): TimelineEntry {
    const data =
        isRecord(row.details)
            ? row.details
            : {};

    const base = {
        kind: "Event" as const,
        at: row.recordedAt,
        service:
            PRODUCERS[row.eventType ?? ""] ?? "unknown",
        eventType: row.eventType,
        eventId: row.eventId,
        deliveries: row.deliveries,
        producerTime: row.occurredAt
    };

    switch (row.eventType) {
        case "PaymentAuthorized":
            return {
                ...base,
                title: "Payment authorized",
                description:
                    `Payment ${text(data.paymentId)} for ${formatAmount(data.amount)}. ` +
                    "Simulated: the Payment Service always authorizes.",
                simulated: true,
                outcome: "Success"
            };

        case "InventoryReserved":
            return {
                ...base,
                title: "Stock reserved",
                description:
                    `Reservation ${text(data.reservationId)} committed in PostgreSQL ` +
                    "together with this event (transactional outbox).",
                simulated: false,
                outcome: "Success"
            };

        case "InventoryReservationFailed":
            return {
                ...base,
                title: "Stock reservation failed",
                description:
                    `${text(data.reason)} The payment is not refunded: there is no ` +
                    "compensating step (no saga) in this project.",
                simulated: false,
                outcome: "Failure"
            };

        case "DeliveryBooked":
            return {
                ...base,
                title: "Delivery booked",
                description:
                    `Carrier ${text(data.carrier)}, estimated ${text(data.estimatedDeliveryDate)}. ` +
                    "Simulated: no carrier is contacted.",
                simulated: true,
                outcome: "Success"
            };

        case null:
        default:
            return {
                ...base,
                title: row.eventType ?? "Event",
                description: "",
                simulated: false,
                outcome: "Info"
            };
    }
}

function summarize(
    order: StoredOrder,
    outcome: OrderOutcome
): string {
    switch (outcome) {
        case "Completed":
            return "Completed: payment authorized (simulated), stock reserved and delivery booked (simulated).";

        case "Failed":
            return "Failed: the stock could not be reserved. The simulated payment is not refunded.";

        case "InProgress":
            return `In progress: ${describeStatus(order.status)}.`;
    }
}

function describeStatus(
    status: StoredOrder["status"]
): string {
    switch (status) {
        case "Created":
            return "waiting for the payment";

        case "PaymentAuthorized":
            return "payment authorized, waiting for the stock reservation";

        case "InventoryReserved":
            return "stock reserved, waiting for the delivery booking";

        case "DeliveryBooked":
        case "InventoryReservationFailed":
            return status;
    }
}

const STAGE_BY_EVENT_TYPE: Readonly<Record<string, number>> = {
    PaymentAuthorized: 2,
    InventoryReserved: 3,
    InventoryReservationFailed: 3,
    DeliveryBooked: 4
};

function stage(
    entry: TimelineEntry
): number {
    switch (entry.kind) {
        case "Accepted":
            return 0;

        case "Published":
        case "PublicationPending":
            return 1;

        case "Event":
            return STAGE_BY_EVENT_TYPE[entry.eventType ?? ""] ?? 5;

        case "Replayed":
            return -1;
    }
}

/**
 * Workflow steps in stage order (each stage can only happen after the
 * previous one), with every retry placed before the first step that
 * was observed after it.
 */
function orderCausally(
    entries: readonly TimelineEntry[]
): TimelineEntry[] {
    const byTime =
        (left: TimelineEntry, right: TimelineEntry): number =>
            Date.parse(left.at) - Date.parse(right.at);

    const steps =
        entries
            .filter(entry => entry.kind !== "Replayed")
            .sort((left, right) =>
                stage(left) - stage(right) ||
                byTime(left, right)
            );

    const retries =
        entries
            .filter(entry => entry.kind === "Replayed")
            .sort(byTime);

    const ordered: TimelineEntry[] = [];

    for (const [index, step] of steps.entries()) {
        // The accepted order always comes first; a retry follows it.
        while (
            index > 0 &&
            retries.length > 0 &&
            Date.parse(retries[0]!.at) < Date.parse(step.at)
        ) {
            ordered.push(retries.shift()!);
        }

        ordered.push(step);
    }

    return [
        ...ordered,
        ...retries
    ];
}

function formatAmount(
    value: unknown
): string {
    return typeof value === "number"
        ? value.toFixed(2)
        : "?";
}

function text(
    value: unknown
): string {
    return typeof value === "string" && value.length > 0
        ? value
        : "?";
}

function isRecord(
    value: unknown
): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
}
