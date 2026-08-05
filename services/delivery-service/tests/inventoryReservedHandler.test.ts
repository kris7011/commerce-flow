import assert from "node:assert/strict";
import test from "node:test";
import type {
    DeliveryBookedEvent,
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import {
    DeliveryService
} from "../src/deliveryService.js";
import {
    createInventoryReservedHandler,
    type DeliveryBookedPublisher,
    type DeliveryEventLogger
} from "../src/inventoryReservedHandler.js";

const fixedDate =
    new Date(
        "2026-08-05T13:00:00.000Z"
    );

test(
    "books delivery and publishes DeliveryBooked",
    async () => {
        const publisher =
            new RecordingDeliveryBookedPublisher();

        const logger =
            new RecordingDeliveryLogger();

        const handler =
            createInventoryReservedHandler({
                deliveryService:
                    createDeliveryService([
                        "delivery-event-001",
                        "delivery-001"
                    ]),
                deliveryBookedPublisher:
                    publisher,
                logger
            });

        const sourceEvent =
            createInventoryReservedEvent();

        await handler(sourceEvent);

        assert.equal(
            publisher.events.length,
            1
        );

        assert.deepEqual(
            publisher.events[0],
            {
                eventId:
                    "delivery-event-001",
                eventType:
                    "DeliveryBooked",
                occurredAt:
                    "2026-08-05T13:00:00.000Z",
                correlationId:
                    "correlation-001",
                data: {
                    orderId:
                        "order-001",
                    deliveryId:
                        "delivery-001",
                    carrier:
                        "DefaultCarrier",
                    estimatedDeliveryDate:
                        "2026-08-08"
                }
            }
        );

        assert.deepEqual(
            logger.messages,
            [
                "[delivery-service] " +
                "Received InventoryReserved " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'",

                "[delivery-service] " +
                "Booked delivery " +
                "for order 'order-001'"
            ]
        );
    }
);

test(
    "propagates publishing failures without logging a completed booking",
    async () => {
        const publisher =
            new FailingDeliveryBookedPublisher(
                new Error(
                    "RabbitMQ publish failed"
                )
            );

        const logger =
            new RecordingDeliveryLogger();

        const handler =
            createInventoryReservedHandler({
                deliveryService:
                    createDeliveryService([
                        "delivery-event-001",
                        "delivery-001"
                    ]),
                deliveryBookedPublisher:
                    publisher,
                logger
            });

        await assert.rejects(
            async () => {
                await handler(
                    createInventoryReservedEvent()
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
                "[delivery-service] " +
                "Received InventoryReserved " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'"
            ]
        );
    }
);

class RecordingDeliveryBookedPublisher
    implements DeliveryBookedPublisher {
    readonly events:
        DeliveryBookedEvent[] = [];

    async publishDeliveryBooked(
        event: DeliveryBookedEvent
    ): Promise<void> {
        this.events.push(event);
    }
}

class FailingDeliveryBookedPublisher
    implements DeliveryBookedPublisher {
    constructor(
        private readonly error:
            Error
    ) {
    }

    async publishDeliveryBooked(
        _event: DeliveryBookedEvent
    ): Promise<void> {
        throw this.error;
    }
}

class RecordingDeliveryLogger
    implements DeliveryEventLogger {
    readonly messages:
        string[] = [];

    log(message: string): void {
        this.messages.push(message);
    }
}

function createDeliveryService(
    generatedIds: string[]
): DeliveryService {
    let currentIndex = 0;

    return new DeliveryService({
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
        getCurrentDate: () =>
            new Date(
                fixedDate.getTime()
            )
    });
}

function createInventoryReservedEvent():
    InventoryReservedEvent {
    return {
        eventId:
            "inventory-event-001",
        eventType:
            "InventoryReserved",
        occurredAt:
            "2026-08-05T12:59:00.000Z",
        correlationId:
            "correlation-001",
        data: {
            orderId:
                "order-001",
            reservationId:
                "reservation-001",
            items: [
                {
                    productId:
                        "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                }
            ]
        }
    };
}