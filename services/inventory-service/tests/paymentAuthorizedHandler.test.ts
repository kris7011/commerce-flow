import assert from "node:assert/strict";
import test from "node:test";
import type {
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import {
    InMemoryInventoryStore
} from "../src/inMemoryInventoryStore.js";
import {
    InventoryService
} from "../src/inventoryService.js";
import {
    createPaymentAuthorizedHandler
} from "../src/paymentAuthorizedHandler.js";

const fixedTime =
    "2026-08-05T12:00:00.000Z";

test(
    "reserves stock, enqueues InventoryReserved and wakes the outbox relay",
    async () => {
        const store =
            createStore();

        const logger =
            new RecordingInventoryLogger();

        let notifications =
            0;

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService:
                    createInventoryService(
                        store,
                        [
                            "inventory-event-001",
                            "reservation-001"
                        ]
                    ),
                logger,
                onEventEnqueued:
                    () => {
                        notifications +=
                            1;
                    }
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

        assert.deepEqual(
            store.getOutbox(),
            [
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
            ]
        );

        assert.equal(
            notifications,
            1
        );

        assert.deepEqual(
            await store.getAllStock(),
            {
                "washing-machine-01": 8,
                "dishwasher-01": 5,
                "dryer-01": 3
            }
        );

        assert.deepEqual(
            logger.infoLogs.map(
                entry =>
                    entry.message
            ),
            [
                "Received PaymentAuthorized",
                "Reserved inventory"
            ]
        );

        assert.deepEqual(
            logger.infoLogs[1]?.context,
            {
                eventId:
                    "inventory-event-001",
                orderId:
                    "order-001",
                reservationId:
                    "reservation-001",
                correlationId:
                    "correlation-001",
                itemCount:
                    1
            }
        );
    }
);

test(
    "enqueues InventoryReservationFailed without changing stock",
    async () => {
        const store =
            createStore();

        const logger =
            new RecordingInventoryLogger();

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService:
                    createInventoryService(
                        store,
                        [
                            "inventory-event-001"
                        ]
                    ),
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
            store.getOutbox()[0]?.eventType,
            "InventoryReservationFailed"
        );

        assert.deepEqual(
            await store.getAllStock(),
            {
                "washing-machine-01": 10,
                "dishwasher-01": 5,
                "dryer-01": 3
            }
        );

        assert.deepEqual(
            logger.warningLogs.map(
                entry =>
                    entry.message
            ),
            [
                "Inventory reservation failed"
            ]
        );
    }
);

test(
    "acknowledges a duplicate delivery without enqueueing another event",
    async () => {
        const store =
            createStore();

        const logger =
            new RecordingInventoryLogger();

        let notifications =
            0;

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService:
                    createInventoryService(
                        store,
                        [
                            "inventory-event-001",
                            "reservation-001"
                        ]
                    ),
                logger,
                onEventEnqueued:
                    () => {
                        notifications +=
                            1;
                    }
            });

        const sourceEvent =
            createPaymentAuthorizedEvent([
                {
                    productId:
                        "dryer-01",
                    quantity: 1,
                    unitPrice: 2999
                }
            ]);

        await handler(sourceEvent);

        await handler({
            ...sourceEvent,
            eventId:
                "payment-event-002"
        });

        assert.equal(
            store.getOutbox().length,
            1
        );

        assert.equal(
            notifications,
            1
        );

        assert.deepEqual(
            logger.warningLogs.at(-1),
            {
                message:
                    "Ignored duplicate PaymentAuthorized for an already processed order",
                context: {
                    eventId:
                        "payment-event-002",
                    orderId:
                        "order-001",
                    correlationId:
                        "correlation-001",
                    originalSourceEventId:
                        "payment-event-001",
                    originalStatus:
                        "Reserved",
                    originalResultEventId:
                        "inventory-event-001"
                }
            }
        );
    }
);

test(
    "propagates persistence failures so the message is retried",
    async () => {
        const logger =
            new RecordingInventoryLogger();

        let notifications =
            0;

        const handler =
            createPaymentAuthorizedHandler({
                inventoryService: {
                    reserveForPayment:
                        async () => {
                            throw new Error(
                                "PostgreSQL unavailable"
                            );
                        }
                },
                logger,
                onEventEnqueued:
                    () => {
                        notifications +=
                            1;
                    }
            });

        await assert.rejects(
            handler(
                createPaymentAuthorizedEvent([
                    {
                        productId:
                            "dryer-01",
                        quantity: 1,
                        unitPrice: 2999
                    }
                ])
            ),
            {
                message:
                    "PostgreSQL unavailable"
            }
        );

        assert.equal(
            notifications,
            0
        );

        assert.deepEqual(
            logger.infoLogs.map(
                entry =>
                    entry.message
            ),
            [
                "Received PaymentAuthorized"
            ]
        );
    }
);

class RecordingInventoryLogger
    implements AppLogger {
    readonly infoLogs: {
        message: string;
        context?: LogContext;
    }[] = [];

    readonly warningLogs: {
        message: string;
        context?: LogContext;
    }[] = [];

    readonly errorLogs: {
        message: string;
        error?: unknown;
        context?: LogContext;
    }[] = [];

    info(
        message: string,
        context?: LogContext
    ): void {
        this.infoLogs.push({
            message,
            context
        });
    }

    warn(
        message: string,
        context?: LogContext
    ): void {
        this.warningLogs.push({
            message,
            context
        });
    }

    error(
        message: string,
        error?: unknown,
        context?: LogContext
    ): void {
        this.errorLogs.push({
            message,
            error,
            context
        });
    }

    child(
        _context: LogContext
    ): AppLogger {
        return this;
    }
}

function createStore():
    InMemoryInventoryStore {
    return new InMemoryInventoryStore({
        "washing-machine-01": 10,
        "dishwasher-01": 5,
        "dryer-01": 3
    });
}

function createInventoryService(
    store:
        InMemoryInventoryStore,
    generatedIds: string[]
): InventoryService {
    let currentIndex = 0;

    return new InventoryService(
        store,
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
