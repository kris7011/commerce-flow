import assert from "node:assert/strict";
import test from "node:test";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import { InMemoryInventoryStore } from "../src/inMemoryInventoryStore.js";
import type {
    InventoryTransaction,
    InventoryUnitOfWork
} from "../src/inventoryStore.js";
import {
    InventoryService,
    type InventoryResultEvent,
    type ReservationOutcome
} from "../src/inventoryService.js";

const fixedTime = "2026-07-31T08:00:00.000Z";

test(
    "reserves all requested items and enqueues InventoryReserved atomically",
    async () => {
        const store = new InMemoryInventoryStore({
            "washing-machine-01": 10,
            "dishwasher-01": 5
        });

        const service = createInventoryService(store);

        const sourceEvent = createPaymentAuthorizedEvent([
            {
                productId: "washing-machine-01",
                quantity: 2,
                unitPrice: 4999
            },
            {
                productId: "dishwasher-01",
                quantity: 1,
                unitPrice: 3499
            }
        ]);

        const outcome = await service.reserveForPayment(sourceEvent);
        const result = getProcessedEvent(outcome);

        assertInventoryReserved(result);

        assert.equal(result.eventId, "generated-id-1");
        assert.equal(result.data.reservationId, "generated-id-2");
        assert.equal(result.occurredAt, fixedTime);
        assert.equal(result.correlationId, sourceEvent.correlationId);
        assert.equal(result.data.orderId, sourceEvent.data.orderId);
        assert.deepEqual(result.data.items, sourceEvent.data.items);

        assert.deepEqual(await store.getAllStock(), {
            "washing-machine-01": 8,
            "dishwasher-01": 4
        });

        assert.deepEqual(store.getOutbox(), [result]);

        assert.deepEqual(store.getReservations(), [
            {
                orderId: "order-001",
                status: "Reserved",
                reservationId: "generated-id-2",
                sourceEventId: "payment-event-001",
                resultEventId: "generated-id-1",
                items: sourceEvent.data.items
            }
        ]);
    }
);

test(
    "records InventoryReservationFailed without reserving any stock",
    async () => {
        const store = new InMemoryInventoryStore({
            "washing-machine-01": 10,
            "dryer-01": 3
        });

        const service = createInventoryService(store);

        const outcome = await service.reserveForPayment(
            createPaymentAuthorizedEvent([
                {
                    productId: "washing-machine-01",
                    quantity: 2,
                    unitPrice: 4999
                },
                {
                    productId: "dryer-01",
                    quantity: 4,
                    unitPrice: 2999
                }
            ])
        );

        const result = getProcessedEvent(outcome);

        assertInventoryReservationFailed(result);

        assert.equal(
            result.data.reason,
            "One or more products are not available in the requested quantity."
        );

        assert.deepEqual(result.data.unavailableItems, [
            {
                productId: "dryer-01",
                requestedQuantity: 4,
                availableQuantity: 3
            }
        ]);

        assert.deepEqual(await store.getAllStock(), {
            "washing-machine-01": 10,
            "dryer-01": 3
        });

        assert.deepEqual(store.getOutbox(), [result]);
        assert.equal(store.getReservations()[0]?.status, "Rejected");
        assert.equal(store.getReservations()[0]?.reservationId, null);
    }
);

test(
    "combines duplicate order lines before checking availability",
    async () => {
        const store = new InMemoryInventoryStore({
            "washing-machine-01": 10
        });

        const outcome = await createInventoryService(store)
            .reserveForPayment(
                createPaymentAuthorizedEvent([
                    {
                        productId: "washing-machine-01",
                        quantity: 6,
                        unitPrice: 4999
                    },
                    {
                        productId: "washing-machine-01",
                        quantity: 6,
                        unitPrice: 4999
                    }
                ])
            );

        const result = getProcessedEvent(outcome);

        assertInventoryReservationFailed(result);

        assert.deepEqual(result.data.unavailableItems, [
            {
                productId: "washing-machine-01",
                requestedQuantity: 12,
                availableQuantity: 10
            }
        ]);
    }
);

test(
    "treats an unknown product as having zero available stock",
    async () => {
        const store = new InMemoryInventoryStore({
            "washing-machine-01": 10
        });

        const outcome = await createInventoryService(store)
            .reserveForPayment(
                createPaymentAuthorizedEvent([
                    {
                        productId: "unknown-product",
                        quantity: 1,
                        unitPrice: 100
                    }
                ])
            );

        const result = getProcessedEvent(outcome);

        assertInventoryReservationFailed(result);

        assert.deepEqual(result.data.unavailableItems, [
            {
                productId: "unknown-product",
                requestedQuantity: 1,
                availableQuantity: 0
            }
        ]);
    }
);

test(
    "ignores a redelivered PaymentAuthorized event for the same order",
    async () => {
        const store = new InMemoryInventoryStore({
            "dryer-01": 3
        });

        const service = createInventoryService(store);

        const sourceEvent = createPaymentAuthorizedEvent([
            {
                productId: "dryer-01",
                quantity: 1,
                unitPrice: 2999
            }
        ]);

        const first = await service.reserveForPayment(sourceEvent);
        const second = await service.reserveForPayment(sourceEvent);

        assert.equal(first.status, "Processed");
        assert.equal(second.status, "Duplicate");

        assert.deepEqual(await store.getAllStock(), {
            "dryer-01": 2
        });

        assert.equal(store.getOutbox().length, 1);
    }
);

test(
    "treats a different event for an already processed order as a duplicate",
    async () => {
        const store = new InMemoryInventoryStore({
            "dryer-01": 3
        });

        const service = createInventoryService(store);

        const items = [
            {
                productId: "dryer-01",
                quantity: 1,
                unitPrice: 2999
            }
        ];

        await service.reserveForPayment(
            createPaymentAuthorizedEvent(items)
        );

        const duplicate = await service.reserveForPayment({
            ...createPaymentAuthorizedEvent(items),
            eventId: "payment-event-retry-with-new-id"
        });

        assert.equal(duplicate.status, "Duplicate");

        if (duplicate.status === "Duplicate") {
            assert.equal(
                duplicate.existing.sourceEventId,
                "payment-event-001"
            );
        }

        assert.deepEqual(await store.getAllStock(), {
            "dryer-01": 2
        });
    }
);

test(
    "processes concurrent duplicates of the same order only once",
    async () => {
        const store = new InMemoryInventoryStore({
            "dryer-01": 3
        });

        const service = createInventoryService(store);

        const sourceEvent = createPaymentAuthorizedEvent([
            {
                productId: "dryer-01",
                quantity: 1,
                unitPrice: 2999
            }
        ]);

        const outcomes = await Promise.all(
            Array.from(
                { length: 5 },
                () => service.reserveForPayment(sourceEvent)
            )
        );

        assert.equal(
            outcomes.filter(outcome => outcome.status === "Processed").length,
            1
        );

        assert.deepEqual(await store.getAllStock(), {
            "dryer-01": 2
        });
    }
);

test(
    "leaves no partial changes when the outbox write fails",
    async () => {
        const store = new InMemoryInventoryStore({
            "dryer-01": 3
        });

        const failingUnitOfWork: InventoryUnitOfWork = {
            inTransaction: work =>
                store.inTransaction(transaction =>
                    work(
                        withFailingOutbox(transaction)
                    )
                )
        };

        const service = new InventoryService(failingUnitOfWork);

        await assert.rejects(
            service.reserveForPayment(
                createPaymentAuthorizedEvent([
                    {
                        productId: "dryer-01",
                        quantity: 2,
                        unitPrice: 2999
                    }
                ])
            ),
            /outbox unavailable/
        );

        assert.deepEqual(await store.getAllStock(), {
            "dryer-01": 3
        });

        assert.deepEqual(store.getReservations(), []);
        assert.deepEqual(store.getOutbox(), []);
    }
);

function withFailingOutbox(
    transaction: InventoryTransaction
): InventoryTransaction {
    return {
        lockOrder: orderId => transaction.lockOrder(orderId),
        lockStock: productIds => transaction.lockStock(productIds),
        decrementStock: quantities => transaction.decrementStock(quantities),
        recordReservation: record => transaction.recordReservation(record),
        enqueueEvent: async () => {
            throw new Error("outbox unavailable");
        }
    };
}

function getProcessedEvent(
    outcome: ReservationOutcome
): InventoryResultEvent {
    assert.equal(outcome.status, "Processed");

    if (outcome.status !== "Processed") {
        throw new Error("Expected a processed outcome.");
    }

    return outcome.event;
}

function assertInventoryReserved(
    event: InventoryResultEvent
): asserts event is InventoryReservedEvent {
    assert.equal(
        event.eventType,
        "InventoryReserved",
        "Expected an InventoryReserved event."
    );
}

function assertInventoryReservationFailed(
    event: InventoryResultEvent
): asserts event is InventoryReservationFailedEvent {
    assert.equal(
        event.eventType,
        "InventoryReservationFailed",
        "Expected an InventoryReservationFailed event."
    );
}

function createInventoryService(
    store: InMemoryInventoryStore
): InventoryService {
    let generatedIdNumber = 0;

    return new InventoryService(store, {
        generateId: () => {
            generatedIdNumber += 1;

            return `generated-id-${generatedIdNumber}`;
        },
        getCurrentTime: () => fixedTime
    });
}

function createPaymentAuthorizedEvent(
    items: OrderItem[]
): PaymentAuthorizedEvent {
    const amount = items.reduce((total, item) => {
        return total + item.quantity * item.unitPrice;
    }, 0);

    return {
        eventId: "payment-event-001",
        eventType: "PaymentAuthorized",
        occurredAt: "2026-07-31T07:59:00.000Z",
        correlationId: "correlation-001",
        data: {
            orderId: "order-001",
            paymentId: "payment-001",
            amount,
            items
        }
    };
}
