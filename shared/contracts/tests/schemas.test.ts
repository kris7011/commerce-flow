import assert from "node:assert/strict";
import test from "node:test";
import {
    COMMERCE_TOPOLOGY,
    ROUTING_KEYS,
    getRoutingKey,
    parseDeliveryBookedEvent,
    parseInventoryReservationFailedEvent,
    parseInventoryReservedEvent,
    parseOrderCreatedEvent,
    parsePaymentAuthorizedEvent,
    type PaymentAuthorizedEvent
} from "../src/index.js";

function createPaymentAuthorizedEvent():
    PaymentAuthorizedEvent {
    return {
        eventId:
            "2f1c4f8e-8d7b-4a52-9d0e-2d6b0f0e1a11",
        eventType:
            "PaymentAuthorized",
        occurredAt:
            "2026-10-15T12:00:00.000Z",
        correlationId:
            "correlation-001",
        data: {
            orderId:
                "order-001",
            paymentId:
                "payment-001",
            amount:
                9998,
            items: [
                {
                    productId:
                        "washing-machine-01",
                    quantity:
                        2,
                    unitPrice:
                        4999
                }
            ]
        }
    };
}

test(
    "accepts a valid event and ignores unknown fields",
    () => {
        const event =
            createPaymentAuthorizedEvent();

        const parsed =
            parsePaymentAuthorizedEvent({
                ...event,
                producerVersion:
                    "future-field",
                data: {
                    ...event.data,
                    paymentProvider:
                        "future-field"
                }
            });

        assert.deepEqual(
            parsed,
            event
        );
    }
);

test(
    "rejects an event with the wrong event type",
    () => {
        assert.throws(
            () =>
                parsePaymentAuthorizedEvent({
                    ...createPaymentAuthorizedEvent(),
                    eventType:
                        "OrderCreated"
                })
        );
    }
);

test(
    "rejects fractional or non-positive quantities",
    () => {
        for (
            const quantity of [
                0,
                -1,
                1.5
            ]
        ) {
            const event =
                createPaymentAuthorizedEvent();

            assert.throws(
                () =>
                    parsePaymentAuthorizedEvent({
                        ...event,
                        data: {
                            ...event.data,
                            items: [
                                {
                                    productId:
                                        "washing-machine-01",
                                    quantity,
                                    unitPrice:
                                        1
                                }
                            ]
                        }
                    }),
                `quantity ${quantity} must be rejected`
            );
        }
    }
);

test(
    "rejects events without an event id or with an invalid timestamp",
    () => {
        assert.throws(
            () =>
                parsePaymentAuthorizedEvent({
                    ...createPaymentAuthorizedEvent(),
                    eventId:
                        ""
                })
        );

        assert.throws(
            () =>
                parsePaymentAuthorizedEvent({
                    ...createPaymentAuthorizedEvent(),
                    occurredAt:
                        "yesterday"
                })
        );
    }
);

test(
    "parses every event type in the workflow",
    () => {
        const envelope = {
            eventId:
                "event-001",
            occurredAt:
                "2026-10-15T12:00:00.000Z",
            correlationId:
                "correlation-001"
        };

        const items = [
            {
                productId:
                    "dryer-01",
                quantity:
                    1,
                unitPrice:
                    2999
            }
        ];

        assert.equal(
            parseOrderCreatedEvent({
                ...envelope,
                eventType:
                    "OrderCreated",
                data: {
                    orderId:
                        "order-001",
                    customerId:
                        "customer-001",
                    items,
                    totalAmount:
                        2999
                }
            }).eventType,
            "OrderCreated"
        );

        assert.equal(
            parseInventoryReservedEvent({
                ...envelope,
                eventType:
                    "InventoryReserved",
                data: {
                    orderId:
                        "order-001",
                    reservationId:
                        "reservation-001",
                    items
                }
            }).eventType,
            "InventoryReserved"
        );

        assert.equal(
            parseInventoryReservationFailedEvent({
                ...envelope,
                eventType:
                    "InventoryReservationFailed",
                data: {
                    orderId:
                        "order-001",
                    reason:
                        "Out of stock",
                    unavailableItems: [
                        {
                            productId:
                                "dryer-01",
                            requestedQuantity:
                                4,
                            availableQuantity:
                                3
                        }
                    ]
                }
            }).eventType,
            "InventoryReservationFailed"
        );

        assert.equal(
            parseDeliveryBookedEvent({
                ...envelope,
                eventType:
                    "DeliveryBooked",
                data: {
                    orderId:
                        "order-001",
                    deliveryId:
                        "delivery-001",
                    carrier:
                        "DefaultCarrier",
                    estimatedDeliveryDate:
                        "2026-10-18"
                }
            }).eventType,
            "DeliveryBooked"
        );
    }
);

test(
    "maps every event type to its routing key",
    () => {
        assert.equal(
            getRoutingKey(
                "InventoryReserved"
            ),
            "inventory.reserved"
        );

        assert.equal(
            new Set(
                Object.values(
                    ROUTING_KEYS
                )
            ).size,
            Object.keys(
                ROUTING_KEYS
            ).length,
            "Routing keys must be unique."
        );
    }
);

test(
    "binds every routing key to at least one queue",
    () => {
        const boundRoutingKeys =
            new Set(
                COMMERCE_TOPOLOGY.flatMap(
                    queue =>
                        queue.routingKeys
                )
            );

        for (const routingKey of Object.values(ROUTING_KEYS)) {
            assert.ok(
                boundRoutingKeys.has(
                    routingKey
                ),
                `No queue is bound to '${routingKey}'; ` +
                `events with this routing key would be unroutable.`
            );
        }

        assert.equal(
            new Set(
                COMMERCE_TOPOLOGY.map(
                    queue =>
                        queue.queueName
                )
            ).size,
            COMMERCE_TOPOLOGY.length,
            "Queue names must be unique."
        );
    }
);
