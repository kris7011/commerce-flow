import assert from "node:assert/strict";
import test from "node:test";
import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import { PaymentService } from "../src/paymentService.js";

// PaymentAuthorized is recorded as of the order it authorizes.
const orderCreatedAt = "2026-07-31T08:59:00.000Z";

test(
    "creates a payment authorized event from an order created event",
    () => {
        const paymentService = createPaymentService([
            "payment-event-001",
            "payment-001"
        ]);

        const sourceEvent = createOrderCreatedEvent();

        const result =
            paymentService.authorizePayment(sourceEvent);

        assert.deepEqual(result, {
            eventId: "payment-event-001",
            eventType: "PaymentAuthorized",
            occurredAt: orderCreatedAt,
            correlationId: "correlation-001",
            data: {
                orderId: "order-001",
                paymentId: "payment-001",
                amount: 8498.95,
                items: sourceEvent.data.items
            }
        });
    }
);

test(
    "preserves the correlation id and order data",
    () => {
        const paymentService = createPaymentService([
            "payment-event-001",
            "payment-001"
        ]);

        const sourceEvent = createOrderCreatedEvent({
            correlationId: "workflow-correlation-999",
            orderId: "order-999",
            totalAmount: 0
        });

        const result =
            paymentService.authorizePayment(sourceEvent);

        assert.equal(
            result.correlationId,
            "workflow-correlation-999"
        );

        assert.equal(
            result.data.orderId,
            "order-999"
        );

        assert.equal(
            result.data.amount,
            0
        );

        assert.deepEqual(
            result.data.items,
            sourceEvent.data.items
        );
    }
);

test(
    "does not modify the source order event",
    () => {
        const paymentService = createPaymentService([
            "payment-event-001",
            "payment-001"
        ]);

        const sourceEvent = createOrderCreatedEvent();

        const sourceEventBeforeAuthorization =
            structuredClone(sourceEvent);

        paymentService.authorizePayment(sourceEvent);

        assert.deepEqual(
            sourceEvent,
            sourceEventBeforeAuthorization
        );
    }
);

test(
    "re-emits an identical event when the same order event is redelivered",
    () => {
        const paymentService = new PaymentService();

        const first = paymentService.authorizePayment(
            createOrderCreatedEvent()
        );

        const redelivered = paymentService.authorizePayment(
            createOrderCreatedEvent()
        );

        assert.deepEqual(redelivered, first);

        assert.match(
            first.eventId,
            /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
        );

        assert.notEqual(first.eventId, first.data.paymentId);
    }
);

test(
    "re-emits an identical event when the redelivery happens on a later date",
    t => {
        t.mock.timers.enable({
            apis: ["Date"],
            now: new Date("2026-07-31T23:59:30.000Z")
        });

        const paymentService = new PaymentService();

        const first = paymentService.authorizePayment(
            createOrderCreatedEvent()
        );

        // The broker redelivers the message two days later.
        t.mock.timers.tick(2 * 24 * 60 * 60 * 1000);

        const redelivered = paymentService.authorizePayment(
            createOrderCreatedEvent()
        );

        assert.deepEqual(redelivered, first);
        assert.equal(first.occurredAt, orderCreatedAt);
    }
);

test(
    "derives different ids for different orders",
    () => {
        const paymentService = new PaymentService();

        const first = paymentService.authorizePayment(
            createOrderCreatedEvent()
        );

        const second = paymentService.authorizePayment({
            ...createOrderCreatedEvent({
                orderId: "order-002"
            }),
            eventId: "order-event-002"
        });

        assert.notEqual(first.eventId, second.eventId);
        assert.notEqual(first.data.paymentId, second.data.paymentId);
    }
);

function createPaymentService(
    generatedIds: string[]
): PaymentService {
    let currentIndex = 0;

    return new PaymentService({
        generateId: () => {
            const generatedId =
                generatedIds[currentIndex];

            if (!generatedId) {
                throw new Error(
                    "The test did not provide enough generated IDs."
                );
            }

            currentIndex += 1;

            return generatedId;
        }
    });
}

interface OrderCreatedEventOverrides {
    correlationId?: string;
    orderId?: string;
    totalAmount?: number;
}

function createOrderCreatedEvent(
    overrides: OrderCreatedEventOverrides = {}
): OrderCreatedEvent {
    return {
        eventId: "order-event-001",
        eventType: "OrderCreated",
        occurredAt: orderCreatedAt,
        correlationId:
            overrides.correlationId ??
            "correlation-001",
        data: {
            orderId:
                overrides.orderId ??
                "order-001",
            customerId: "customer-001",
            items: [
                {
                    productId: "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                },
                {
                    productId: "dishwasher-01",
                    quantity: 1,
                    unitPrice: 3499.95
                }
            ],
            totalAmount:
                overrides.totalAmount ??
                8498.95
        }
    };
}
