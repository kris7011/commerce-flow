import type {
    EventType
} from "@commerce-flow/contracts";
import type {
    ServiceName
} from "./endpoints.js";

/*
 * Pure functions that turn the messages observed on the exchange into a
 * per-order timeline. Kept free of I/O so they can be unit-tested.
 */

/** The envelope fields every CommerceFlow event carries. */
export interface ObservedEnvelope {
    readonly eventId: string;
    readonly eventType: string;
    readonly correlationId: string;
    readonly orderId: string | undefined;
}

export interface ObservedMessage {
    /** Epoch milliseconds at which the tap received the message. */
    readonly receivedAt: number;
    readonly routingKey: string;
    readonly envelope: ObservedEnvelope;
    readonly body: unknown;
}

export const PRODUCER_BY_EVENT_TYPE:
    Readonly<Record<EventType, ServiceName>> = {
        OrderCreated: "order-service",
        PaymentAuthorized: "payment-service",
        InventoryReserved: "inventory-service",
        InventoryReservationFailed: "inventory-service",
        DeliveryBooked: "delivery-service"
    };

/**
 * Reads the envelope fields without validating the payload: the tap
 * must show every message, including ones a consumer would reject.
 */
export function readEnvelope(
    body: unknown
): ObservedEnvelope | undefined {
    if (!isRecord(body)) {
        return undefined;
    }

    const {
        eventId,
        eventType,
        correlationId,
        data
    } = body;

    if (
        typeof eventId !== "string" ||
        typeof eventType !== "string"
    ) {
        return undefined;
    }

    const orderId =
        isRecord(data) &&
        typeof data.orderId === "string"
            ? data.orderId
            : undefined;

    return {
        eventId,
        eventType,
        correlationId:
            typeof correlationId === "string"
                ? correlationId
                : "",
        orderId
    };
}

export type OrderOutcome =
    | "Delivered"
    | "ReservationFailed"
    | "Pending";

export interface TimelineEntry {
    /** Milliseconds since the order was submitted. */
    readonly offsetInMs: number;
    readonly producer: string;
    readonly eventType: string;
    readonly routingKey: string;
    readonly eventId: string;

    /** How often a message with this event id was observed. */
    readonly copies: number;
}

export interface OrderTimeline {
    readonly orderId: string;
    readonly entries: readonly TimelineEntry[];
    readonly outcome: OrderOutcome;
}

export function buildOrderTimeline(
    orderId: string,
    messages: readonly ObservedMessage[],
    submittedAt: number
): OrderTimeline {
    const byEventId =
        new Map<string, {
            first: ObservedMessage;
            copies: number;
        }>();

    for (const message of messages) {
        if (message.envelope.orderId !== orderId) {
            continue;
        }

        const existing =
            byEventId.get(
                message.envelope.eventId
            );

        if (existing) {
            existing.copies += 1;
            continue;
        }

        byEventId.set(
            message.envelope.eventId,
            {
                first: message,
                copies: 1
            }
        );
    }

    const entries =
        [...byEventId.values()]
            .sort(
                (left, right) =>
                    left.first.receivedAt -
                    right.first.receivedAt
            )
            .map(({ first, copies }) => ({
                offsetInMs:
                    Math.max(
                        0,
                        first.receivedAt -
                        submittedAt
                    ),
                producer:
                    producerOf(
                        first.envelope.eventType
                    ),
                eventType:
                    first.envelope.eventType,
                routingKey:
                    first.routingKey,
                eventId:
                    first.envelope.eventId,
                copies
            }));

    return {
        orderId,
        entries,
        outcome:
            determineOutcome(entries)
    };
}

export function determineOutcome(
    entries: readonly Pick<TimelineEntry, "eventType">[]
): OrderOutcome {
    if (
        entries.some(
            entry =>
                entry.eventType ===
                "DeliveryBooked"
        )
    ) {
        return "Delivered";
    }

    if (
        entries.some(
            entry =>
                entry.eventType ===
                "InventoryReservationFailed"
        )
    ) {
        return "ReservationFailed";
    }

    return "Pending";
}

export function formatTimeline(
    timeline: OrderTimeline
): string[] {
    if (timeline.entries.length === 0) {
        return [
            "    (no events observed)"
        ];
    }

    return timeline.entries.map(entry => {
        const copies =
            entry.copies > 1
                ? `  (${entry.copies} copies of the same event id)`
                : "";

        return (
            `    +${String(entry.offsetInMs).padStart(5)} ms  ` +
            `${entry.producer.padEnd(21)}` +
            `${entry.eventType.padEnd(28)}` +
            `${entry.routingKey}${copies}`
        );
    });
}

function producerOf(
    eventType: string
): string {
    return Object.hasOwn(
        PRODUCER_BY_EVENT_TYPE,
        eventType
    )
        ? PRODUCER_BY_EVENT_TYPE[
            eventType as EventType
        ]
        : "unknown";
}

function isRecord(
    value: unknown
): value is Record<string, unknown> {
    return (
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
    );
}
