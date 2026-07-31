import assert from "node:assert/strict";
import test from "node:test";
import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import { PaymentService } from "../src/paymentService.js";

const fixedTime = "2026-07-31T09:00:00.000Z";

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
            occurredAt: fixedTime,
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
        },
        getCurrentTime: () => fixedTime
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
        occurredAt: "2026-07-31T08:59:00.000Z",
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
