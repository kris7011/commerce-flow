import assert from "node:assert/strict";
import test from "node:test";
import type {
    OrderCreatedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    createOrderCreatedHandler,
    type PaymentAuthorizedPublisher,
    type PaymentEventLogger
} from "../src/orderCreatedHandler.js";
import {
    PaymentService
} from "../src/paymentService.js";

const fixedTime =
    "2026-08-02T13:00:00.000Z";

test(
    "authorizes payment and publishes PaymentAuthorized",
    async () => {
        const publisher =
            new RecordingPaymentAuthorizedPublisher();

        const logger =
            new RecordingPaymentLogger();

        const handler =
            createOrderCreatedHandler({
                paymentService:
                    createPaymentService([
                        "payment-event-001",
                        "payment-001"
                    ]),
                paymentAuthorizedPublisher:
                    publisher,
                logger
            });

        const sourceEvent =
            createOrderCreatedEvent();

        await handler(sourceEvent);

        assert.equal(
            publisher.events.length,
            1
        );

        assert.deepEqual(
            publisher.events[0],
            {
                eventId:
                    "payment-event-001",
                eventType:
                    "PaymentAuthorized",
                occurredAt:
                    fixedTime,
                correlationId:
                    "correlation-001",
                data: {
                    orderId:
                        "order-001",
                    paymentId:
                        "payment-001",
                    amount:
                        8498.95,
                    items:
                        sourceEvent.data.items
                }
            }
        );

        assert.deepEqual(
            logger.messages,
            [
                "[payment-service] " +
                "Received OrderCreated " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'",

                "[payment-service] " +
                "Authorized payment " +
                "for order 'order-001'"
            ]
        );
    }
);

test(
    "propagates publishing failures without logging successful authorization",
    async () => {
        const publisher =
            new FailingPaymentAuthorizedPublisher(
                new Error(
                    "RabbitMQ publish failed"
                )
            );

        const logger =
            new RecordingPaymentLogger();

        const handler =
            createOrderCreatedHandler({
                paymentService:
                    createPaymentService([
                        "payment-event-001",
                        "payment-001"
                    ]),
                paymentAuthorizedPublisher:
                    publisher,
                logger
            });

        await assert.rejects(
            async () => {
                await handler(
                    createOrderCreatedEvent()
                );
            },
            {
                message:
                    "RabbitMQ publish failed"
            }
        );

        assert.deepEqual(
            logger.messages,
            [
                "[payment-service] " +
                "Received OrderCreated " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'"
            ]
        );
    }
);

class RecordingPaymentAuthorizedPublisher
    implements PaymentAuthorizedPublisher {
    readonly events:
        PaymentAuthorizedEvent[] = [];

    async publishPaymentAuthorized(
        event: PaymentAuthorizedEvent
    ): Promise<void> {
        this.events.push(event);
    }
}

class FailingPaymentAuthorizedPublisher
    implements PaymentAuthorizedPublisher {
    constructor(
        private readonly error:
            Error
    ) {
    }

    async publishPaymentAuthorized(
        _event: PaymentAuthorizedEvent
    ): Promise<void> {
        throw this.error;
    }
}

class RecordingPaymentLogger
    implements PaymentEventLogger {
    readonly messages:
        string[] = [];

    log(message: string): void {
        this.messages.push(message);
    }
}

function createPaymentService(
    generatedIds: string[]
): PaymentService {
    let currentIndex = 0;

    return new PaymentService({
        generateId: () => {
            const generatedId =
                generatedIds[
                currentIndex
                ];

            if (!generatedId) {
                throw new Error(
                    "The test did not provide " +
                    "enough generated IDs."
                );
            }

            currentIndex += 1;

            return generatedId;
        },
        getCurrentTime: () =>
            fixedTime
    });
}

function createOrderCreatedEvent():
    OrderCreatedEvent {
    return {
        eventId:
            "order-event-001",
        eventType:
            "OrderCreated",
        occurredAt:
            "2026-08-02T12:59:00.000Z",
        correlationId:
            "correlation-001",
        data: {
            orderId:
                "order-001",
            customerId:
                "customer-001",
            items: [
                {
                    productId:
                        "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                },
                {
                    productId:
                        "dishwasher-01",
                    quantity: 1,
                    unitPrice: 3499.95
                }
            ],
            totalAmount:
                8498.95
        }
    };
}