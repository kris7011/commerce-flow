import assert from "node:assert/strict";
import test from "node:test";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import {
    parseIdempotencyKey
} from "../src/idempotencyKey.js";
import {
    InMemoryOrderStore
} from "../src/inMemoryOrderStore.js";
import {
    fingerprintOrderRequest,
    OrderService
} from "../src/orderService.js";
import {
    applyWorkflowEvent,
    getOrderOutcome,
    ORDER_WORKFLOW_EVENT_TYPES,
    type OrderStatus
} from "../src/orderStatus.js";
import type {
    OrderDetails
} from "../src/orderStore.js";
import {
    buildOrderTimeline
} from "../src/orderTimeline.js";
import {
    createOrderWorkflowEventHandler,
    parseOrderWorkflowEvent
} from "../src/orderWorkflowEventHandler.js";

const ORDER_ID =
    "6f1c2a52-6a4b-4f7e-9a59-3c2b9f0d1a01";

const request = {
    customerId:
        "customer-001",
    items: [
        {
            productId: "dryer-01",
            quantity: 1,
            unitPrice: 2999
        },
        {
            productId: "dishwasher-01",
            quantity: 2,
            unitPrice: 3999
        }
    ]
};

// ------------------------------------------------------------ status rules

test(
    "the status follows the workflow forward",
    () => {
        let status: OrderStatus = "Created";

        for (const eventType of ["PaymentAuthorized", "InventoryReserved", "DeliveryBooked"] as const) {
            status = applyWorkflowEvent(status, eventType);
            assert.equal(status, eventType);
        }

        assert.equal(getOrderOutcome(status), "Completed");
    }
);

test(
    "late or repeated events never move the status backwards",
    () => {
        assert.equal(applyWorkflowEvent("DeliveryBooked", "PaymentAuthorized"), "DeliveryBooked");
        assert.equal(applyWorkflowEvent("InventoryReserved", "InventoryReserved"), "InventoryReserved");
        // An out-of-order delivery arriving first still counts.
        assert.equal(applyWorkflowEvent("Created", "DeliveryBooked"), "DeliveryBooked");
    }
);

test(
    "a failed reservation is final and the first inventory decision wins",
    () => {
        assert.equal(applyWorkflowEvent("PaymentAuthorized", "InventoryReservationFailed"), "InventoryReservationFailed");

        for (const eventType of ORDER_WORKFLOW_EVENT_TYPES) {
            assert.equal(
                applyWorkflowEvent("InventoryReservationFailed", eventType),
                "InventoryReservationFailed"
            );
        }

        assert.equal(applyWorkflowEvent("InventoryReserved", "InventoryReservationFailed"), "InventoryReserved");
        assert.equal(getOrderOutcome("InventoryReservationFailed"), "Failed");
        assert.equal(getOrderOutcome("PaymentAuthorized"), "InProgress");
    }
);

// ---------------------------------------------------- key and fingerprint

test(
    "the fingerprint identifies the request content",
    () => {
        const fingerprint =
            fingerprintOrderRequest(request);

        assert.match(fingerprint, /^[0-9a-f]{64}$/);

        // Unknown fields and key order do not matter...
        assert.equal(
            fingerprintOrderRequest({
                items: request.items.map(item => ({
                    unitPrice: item.unitPrice,
                    quantity: item.quantity,
                    productId: item.productId,
                    note: "ignored"
                })),
                customerId: request.customerId
            } as typeof request),
            fingerprint
        );

        // ...but every value and the item order do.
        assert.notEqual(fingerprintOrderRequest({ ...request, customerId: "customer-002" }), fingerprint);
        assert.notEqual(fingerprintOrderRequest({ ...request, items: [...request.items].reverse() }), fingerprint);
        assert.notEqual(
            fingerprintOrderRequest({
                ...request,
                items: [{ ...request.items[0]!, quantity: 2 }, request.items[1]!]
            }),
            fingerprint
        );
    }
);

test(
    "createOrder returns the request fingerprint with the event",
    () => {
        const result =
            new OrderService().createOrder(request);

        assert.equal(result.requestFingerprint, fingerprintOrderRequest(request));
        assert.equal(result.event.data.totalAmount, 10997);
    }
);

test(
    "Idempotency-Key values are validated",
    () => {
        assert.deepEqual(parseIdempotencyKey("8e03978e-40d5-43e8-bc93-6894a57f9324"), {
            success: true,
            key: "8e03978e-40d5-43e8-bc93-6894a57f9324"
        });
        assert.equal(parseIdempotencyKey("k".repeat(255)).success, true);

        for (const invalid of [undefined, "", "k".repeat(256), "has space", "tab\tkey", "nøgle"]) {
            assert.equal(parseIdempotencyKey(invalid).success, false, String(invalid));
        }
    }
);

// ---------------------------------------------------------- event handler

test(
    "parses the four workflow events and rejects anything else",
    () => {
        const payment = {
            eventId: "event-1",
            eventType: "PaymentAuthorized",
            occurredAt: "2026-08-02T12:00:01.000Z",
            correlationId: "c-1",
            data: {
                orderId: ORDER_ID,
                paymentId: "payment-1",
                amount: 2999,
                items: [request.items[0]]
            }
        };

        assert.equal(parseOrderWorkflowEvent(payment).eventType, "PaymentAuthorized");
        assert.throws(() => parseOrderWorkflowEvent({ ...payment, eventType: "OrderCreated" }));
        assert.throws(() => parseOrderWorkflowEvent({ ...payment, data: {} }));
        assert.throws(() => parseOrderWorkflowEvent("not an event"));
    }
);

class RecordingLogger
    implements AppLogger {
    readonly entries: {
        level: string;
        message: string;
        context?: LogContext;
    }[] = [];

    info(message: string, context?: LogContext): void {
        this.entries.push({ level: "info", message, context });
    }

    warn(message: string, context?: LogContext): void {
        this.entries.push({ level: "warn", message, context });
    }

    error(message: string): void {
        this.entries.push({ level: "error", message });
    }

    child(): AppLogger {
        return this;
    }
}

async function storeWithOrder(): Promise<InMemoryOrderStore> {
    const store =
        new InMemoryOrderStore(
            () => "2026-08-02T12:00:00.000Z"
        );

    const created =
        new OrderService({
            generateId: (() => {
                const ids = [ORDER_ID, "0a8b6c1d-2e3f-4a5b-8c7d-9e0f1a2b3c4d"];
                return () => ids.shift() ?? "spare";
            })()
        }).createOrder(request, "c-1");

    await store.submitOrder({
        idempotencyKey: "key-1",
        requestFingerprint: created.requestFingerprint,
        customerId: request.customerId,
        items: request.items,
        totalAmount: created.response.totalAmount,
        correlationId: created.response.correlationId,
        event: created.event
    });

    return store;
}

test(
    "records each event once, counts redeliveries and ignores unknown orders",
    async () => {
        const store =
            await storeWithOrder();

        const logger =
            new RecordingLogger();

        const handle =
            createOrderWorkflowEventHandler({
                store,
                logger
            });

        const reserved =
            parseOrderWorkflowEvent({
                eventId: "event-2",
                eventType: "InventoryReserved",
                occurredAt: "2026-08-02T12:00:02.000Z",
                correlationId: "c-1",
                data: {
                    orderId: ORDER_ID,
                    reservationId: "reservation-1",
                    items: request.items
                }
            });

        await handle(reserved);
        await handle(reserved);
        await handle({
            ...reserved,
            eventId: "event-3",
            data: {
                ...reserved.data,
                orderId: "8d1b2c3e-0000-4000-8000-000000000000"
            }
        } as typeof reserved);

        assert.deepEqual(
            logger.entries.map(entry => entry.message),
            [
                "Recorded order workflow event",
                "Ignored duplicate order workflow event",
                "Ignored workflow event for an unknown order"
            ]
        );
        assert.equal(logger.entries[0]?.context?.orderStatus, "InventoryReserved");
        assert.equal(logger.entries[1]?.context?.deliveries, 2);

        const details =
            await store.getOrderDetails(ORDER_ID);

        const events =
            details?.timeline.filter(row => row.kind === "Event") ?? [];

        assert.equal(events.length, 1);
        assert.equal(events[0]?.deliveries, 2);
    }
);

// --------------------------------------------------------------- timeline

function details(
    status: OrderStatus,
    rows: OrderDetails["timeline"],
    publication: OrderDetails["publication"]
): OrderDetails {
    return {
        order: {
            orderId: ORDER_ID,
            customerId: "customer-001",
            items: request.items,
            totalAmount: 10997,
            correlationId: "c-1",
            status,
            createdAt: "2026-08-02T12:00:00.000Z",
            updatedAt: "2026-08-02T12:00:03.000Z"
        },
        timeline: rows,
        publication
    };
}

function eventRow(
    eventType: string,
    occurredAt: string,
    data: Record<string, unknown>,
    deliveries = 1
): OrderDetails["timeline"][number] {
    return {
        kind: "Event",
        eventId: `${eventType}-id`,
        eventType,
        occurredAt,
        recordedAt: occurredAt,
        deliveries,
        details: data
    };
}

const accepted: OrderDetails["timeline"][number] = {
    kind: "Accepted",
    eventId: null,
    eventType: null,
    occurredAt: "2026-08-02T12:00:00.000Z",
    recordedAt: "2026-08-02T12:00:00.000Z",
    deliveries: 1,
    details: null
};

test(
    "a completed order shows every step in time order with simulated steps labelled",
    () => {
        const view =
            buildOrderTimeline(
                details(
                    "DeliveryBooked",
                    [
                        accepted,
                        // Recorded out of order: delivery before inventory.
                        eventRow("DeliveryBooked", "2026-08-02T12:00:03.000Z", {
                            carrier: "SimulatedCarrier",
                            estimatedDeliveryDate: "2026-08-05"
                        }),
                        eventRow("PaymentAuthorized", "2026-08-02T12:00:01.000Z", {
                            paymentId: "payment-1",
                            amount: 10997
                        }),
                        eventRow("InventoryReserved", "2026-08-02T12:00:02.000Z", {
                            reservationId: "reservation-1"
                        }, 3)
                    ],
                    {
                        eventId: "event-0",
                        createdAt: "2026-08-02T12:00:00.000Z",
                        publishedAt: "2026-08-02T12:00:00.500Z",
                        attempts: 1,
                        lastError: null
                    }
                )
            );

        assert.equal(view.outcome, "Completed");
        assert.deepEqual(
            view.entries.map(entry => entry.title),
            [
                "Order accepted",
                "OrderCreated confirmed by RabbitMQ",
                "Payment authorized",
                "Stock reserved",
                "Delivery booked"
            ]
        );
        assert.deepEqual(
            view.entries.map(entry => entry.simulated),
            [false, false, true, false, true]
        );
        assert.deepEqual(
            view.entries.map(entry => entry.service),
            ["order-service", "order-service", "payment-service", "inventory-service", "delivery-service"]
        );
        assert.equal(view.entries[3]?.deliveries, 3);
        assert.match(view.entries[4]?.description ?? "", /SimulatedCarrier, estimated 2026-08-05/);
    }
);

test(
    "a failed reservation says that the simulated payment is not refunded",
    () => {
        const view =
            buildOrderTimeline(
                details(
                    "InventoryReservationFailed",
                    [
                        accepted,
                        eventRow("InventoryReservationFailed", "2026-08-02T12:00:02.000Z", {
                            reason: "Not enough stock for dryer-01."
                        })
                    ],
                    null
                )
            );

        assert.equal(view.outcome, "Failed");
        assert.match(view.summary, /not refunded/);
        assert.equal(view.entries[1]?.outcome, "Failure");
        assert.match(view.entries[1]?.description ?? "", /Not enough stock for dryer-01\. The payment is not refunded/);
    }
);

test(
    "an unpublished event and a replayed retry are visible",
    () => {
        const view =
            buildOrderTimeline(
                details(
                    "Created",
                    [
                        accepted,
                        {
                            ...accepted,
                            kind: "Replayed",
                            occurredAt: "2026-08-02T12:00:04.000Z",
                            recordedAt: "2026-08-02T12:00:04.000Z"
                        }
                    ],
                    {
                        eventId: "event-0",
                        createdAt: "2026-08-02T12:00:00.000Z",
                        publishedAt: null,
                        attempts: 2,
                        lastError: "RabbitMQ connection is unavailable"
                    }
                )
            );

        assert.equal(view.outcome, "InProgress");
        assert.match(view.summary, /waiting for the payment/);
        assert.deepEqual(
            view.entries.map(entry => [entry.kind, entry.outcome]),
            [
                ["Accepted", "Success"],
                ["PublicationPending", "Waiting"],
                ["Replayed", "Info"]
            ]
        );
        assert.match(view.entries[1]?.description ?? "", /attempt 2.*RabbitMQ connection is unavailable/);
    }
);

test(
    "the timeline is causal even when producer clocks and the publication mark disagree",
    () => {
        // Seen in the container demo: Payment copies its timestamp from
        // OrderCreated (before the database commit), and the relay marks
        // the event as published after Payment's event was recorded.
        const paymentRow = {
            ...eventRow("PaymentAuthorized", "2026-08-02T11:59:59.990Z", {
                paymentId: "payment-1",
                amount: 10997
            }),
            recordedAt: "2026-08-02T12:00:00.020Z"
        };

        const view =
            buildOrderTimeline(
                details(
                    "PaymentAuthorized",
                    [
                        accepted,
                        paymentRow,
                        {
                            ...accepted,
                            kind: "Replayed",
                            occurredAt: "2026-08-02T12:00:00.010Z",
                            recordedAt: "2026-08-02T12:00:00.010Z"
                        }
                    ],
                    {
                        eventId: "event-0",
                        createdAt: "2026-08-02T12:00:00.000Z",
                        publishedAt: "2026-08-02T12:00:00.030Z",
                        attempts: 1,
                        lastError: null
                    }
                )
            );

        assert.deepEqual(
            view.entries.map(entry => entry.kind),
            ["Accepted", "Replayed", "Published", "Event"]
        );

        const payment =
            view.entries[3]!;

        // Shown at the observation time, with the producer's time kept.
        assert.equal(payment.at, "2026-08-02T12:00:00.020Z");
        assert.equal(payment.producerTime, "2026-08-02T11:59:59.990Z");
    }
);
