import assert from "node:assert/strict";
import test from "node:test";
import type {
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    InMemoryInventoryRepository
} from "../src/inMemoryInventoryRepository.js";
import {
    InventoryService,
    type InventoryResultEvent
} from "../src/inventoryService.js";
import {
    createPaymentAuthorizedHandler,
    type InventoryEventLogger,
    type InventoryResultPublisher
} from "../src/paymentAuthorizedHandler.js";

const fixedTime =
    "2026-08-05T12:00:00.000Z";

test(
    "reserves stock and publishes InventoryReserved",
    async () => {
        const repository =
            createRepository();

        const publisher =
            new RecordingInventoryResultPublisher();

        const logger =
            new RecordingInventoryLogger();

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService:
                    createInventoryService(
                        repository,
                        [
                            "inventory-event-001",
                            "reservation-001"
                        ]
                    ),
                inventoryResultPublisher:
                    publisher,
                logger
            });

        const sourceEvent =
            createPaymentAuthorizedEvent([
                {
                    productId:
                        "washing-machine-01",
                    quantity: 2,
                    unitPrice: 4999
                }
            ]);

        await handler(sourceEvent);

        assert.equal(
            publisher.events.length,
            1
        );

        assert.deepEqual(
            publisher.events[0],
            {
                eventId:
                    "inventory-event-001",
                eventType:
                    "InventoryReserved",
                occurredAt:
                    fixedTime,
                correlationId:
                    "correlation-001",
                data: {
                    orderId:
                        "order-001",
                    reservationId:
                        "reservation-001",
                    items:
                        sourceEvent.data.items
                }
            }
        );

        assert.deepEqual(
            repository.getAllStock(),
            {
                "washing-machine-01": 8,
                "dishwasher-01": 5,
                "dryer-01": 3
            }
        );

        assert.deepEqual(
            logger.messages,
            [
                "[inventory-service] " +
                "Received PaymentAuthorized " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'",

                "[inventory-service] " +
                "Reserved inventory " +
                "for order 'order-001'"
            ]
        );
    }
);

test(
    "publishes InventoryReservationFailed without changing stock",
    async () => {
        const repository =
            createRepository();

        const publisher =
            new RecordingInventoryResultPublisher();

        const logger =
            new RecordingInventoryLogger();

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService:
                    createInventoryService(
                        repository,
                        [
                            "inventory-event-001"
                        ]
                    ),
                inventoryResultPublisher:
                    publisher,
                logger
            });

        await handler(
            createPaymentAuthorizedEvent([
                {
                    productId:
                        "dryer-01",
                    quantity: 4,
                    unitPrice: 2999
                }
            ])
        );

        assert.equal(
            publisher.events.length,
            1
        );

        assert.deepEqual(
            publisher.events[0],
            {
                eventId:
                    "inventory-event-001",
                eventType:
                    "InventoryReservationFailed",
                occurredAt:
                    fixedTime,
                correlationId:
                    "correlation-001",
                data: {
                    orderId:
                        "order-001",
                    reason:
                        "One or more products " +
                        "are not available in " +
                        "the requested quantity.",
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
            }
        );

        assert.deepEqual(
            repository.getAllStock(),
            {
                "washing-machine-01": 10,
                "dishwasher-01": 5,
                "dryer-01": 3
            }
        );

        assert.deepEqual(
            logger.messages,
            [
                "[inventory-service] " +
                "Received PaymentAuthorized " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'",

                "[inventory-service] " +
                "Inventory reservation failed " +
                "for order 'order-001'"
            ]
        );
    }
);

test(
    "propagates publishing failures without logging a completed result",
    async () => {
        const repository =
            createRepository();

        const publisher =
            new FailingInventoryResultPublisher(
                new Error(
                    "RabbitMQ publish failed"
                )
            );

        const logger =
            new RecordingInventoryLogger();

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService:
                    createInventoryService(
                        repository,
                        [
                            "inventory-event-001"
                        ]
                    ),
                inventoryResultPublisher:
                    publisher,
                logger
            });

        await assert.rejects(
            async () => {
                await handler(
                    createPaymentAuthorizedEvent([
                        {
                            productId:
                                "dryer-01",
                            quantity: 4,
                            unitPrice: 2999
                        }
                    ])
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
                "[inventory-service] " +
                "Received PaymentAuthorized " +
                "for order 'order-001' " +
                "with correlationId " +
                "'correlation-001'"
            ]
        );

        assert.deepEqual(
            repository.getAllStock(),
            {
                "washing-machine-01": 10,
                "dishwasher-01": 5,
                "dryer-01": 3
            }
        );
    }
);

class RecordingInventoryResultPublisher
    implements InventoryResultPublisher {
    readonly events:
        InventoryResultEvent[] = [];

    async publishInventoryResult(
        event: InventoryResultEvent
    ): Promise<void> {
        this.events.push(event);
    }
}

class FailingInventoryResultPublisher
    implements InventoryResultPublisher {
    constructor(
        private readonly error:
            Error
    ) {
    }

    async publishInventoryResult(
        _event: InventoryResultEvent
    ): Promise<void> {
        throw this.error;
    }
}

class RecordingInventoryLogger
    implements InventoryEventLogger {
    readonly messages:
        string[] = [];

    log(message: string): void {
        this.messages.push(message);
    }
}

function createRepository():
    InMemoryInventoryRepository {
    return new InMemoryInventoryRepository({
        "washing-machine-01": 10,
        "dishwasher-01": 5,
        "dryer-01": 3
    });
}

function createInventoryService(
    repository:
        InMemoryInventoryRepository,
    generatedIds: string[]
): InventoryService {
    let currentIndex = 0;

    return new InventoryService(
        repository,
        {
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
        }
    );
}

function createPaymentAuthorizedEvent(
    items: OrderItem[]
): PaymentAuthorizedEvent {
    const amount =
        items.reduce(
            (
                total,
                item
            ) => {
                return total +
                    item.quantity *
                    item.unitPrice;
            },
            0
        );

    return {
        eventId:
            "payment-event-001",
        eventType:
            "PaymentAuthorized",
        occurredAt:
            "2026-08-05T11:59:00.000Z",
        correlationId:
            "correlation-001",
        data: {
            orderId:
                "order-001",
            paymentId:
                "payment-001",
            amount,
            items
        }
    };
}