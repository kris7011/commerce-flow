import assert from "node:assert/strict";
import test from "node:test";
import {
    buildOrderTimeline,
    determineOutcome,
    formatTimeline,
    readEnvelope,
    type ObservedMessage
} from "../src/timeline.js";

function observed(
    eventType: string,
    eventId: string,
    receivedAt: number,
    orderId = "order-1",
    routingKey = eventType.toLowerCase()
): ObservedMessage {
    return {
        receivedAt,
        routingKey,
        envelope: {
            eventId,
            eventType,
            correlationId: "demo-1",
            orderId
        },
        body: {}
    };
}

test(
    "reads the envelope of an event without validating its payload",
    () => {
        assert.deepEqual(
            readEnvelope({
                eventId: "event-1",
                eventType: "OrderCreated",
                correlationId: "demo-1",
                data: {
                    orderId: "order-1",
                    unexpected: true
                }
            }),
            {
                eventId: "event-1",
                eventType: "OrderCreated",
                correlationId: "demo-1",
                orderId: "order-1"
            }
        );

        assert.equal(
            readEnvelope({
                eventId: "event-1",
                eventType: "OrderCreated"
            })?.orderId,
            undefined
        );

        for (const invalid of [null, "text", [], { eventType: "OrderCreated" }]) {
            assert.equal(readEnvelope(invalid), undefined);
        }
    }
);

test(
    "orders the events of one order by arrival and names their producers",
    () => {
        const timeline =
            buildOrderTimeline(
                "order-1",
                [
                    observed("PaymentAuthorized", "e2", 1_030),
                    observed("OrderCreated", "e1", 1_010),
                    observed("OrderCreated", "other", 1_020, "order-2"),
                    observed("InventoryReserved", "e3", 1_050),
                    observed("DeliveryBooked", "e4", 1_070)
                ],
                1_000
            );

        assert.equal(timeline.outcome, "Delivered");

        assert.deepEqual(
            timeline.entries.map(entry => [
                entry.offsetInMs,
                entry.producer,
                entry.eventType
            ]),
            [
                [10, "order-service", "OrderCreated"],
                [30, "payment-service", "PaymentAuthorized"],
                [50, "inventory-service", "InventoryReserved"],
                [70, "delivery-service", "DeliveryBooked"]
            ]
        );
    }
);

test(
    "collapses redeliveries of the same event id and counts them",
    () => {
        const timeline =
            buildOrderTimeline(
                "order-1",
                [
                    observed("PaymentAuthorized", "e2", 30),
                    observed("PaymentAuthorized", "e2", 90),
                    observed("PaymentAuthorized", "e2-new-id", 95)
                ],
                0
            );

        assert.deepEqual(
            timeline.entries.map(entry => [entry.eventId, entry.copies]),
            [
                ["e2", 2],
                ["e2-new-id", 1]
            ]
        );

        assert.match(
            formatTimeline(timeline)[0] ?? "",
            /2 copies of the same event id/
        );
    }
);

test(
    "derives the outcome from the terminal event",
    () => {
        assert.equal(
            determineOutcome([
                { eventType: "OrderCreated" },
                { eventType: "InventoryReservationFailed" }
            ]),
            "ReservationFailed"
        );

        assert.equal(
            determineOutcome([
                { eventType: "OrderCreated" },
                { eventType: "PaymentAuthorized" }
            ]),
            "Pending"
        );
    }
);

test(
    "marks unknown event types and never reports negative offsets",
    () => {
        const timeline =
            buildOrderTimeline(
                "order-1",
                [
                    observed("SomethingElse", "x", 5)
                ],
                10
            );

        assert.equal(timeline.entries[0]?.producer, "unknown");
        assert.equal(timeline.entries[0]?.offsetInMs, 0);
        assert.equal(timeline.outcome, "Pending");

        assert.deepEqual(
            formatTimeline(
                buildOrderTimeline("missing", [], 0)
            ),
            ["    (no events observed)"]
        );
    }
);
