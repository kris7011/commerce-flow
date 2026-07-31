import assert from "node:assert/strict";
import test from "node:test";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import { InMemoryInventoryRepository } from "../src/inMemoryInventoryRepository.js";
import {
    InventoryService,
    type InventoryResultEvent
} from "../src/inventoryService.js";

const fixedTime = "2026-07-31T08:00:00.000Z";

test(
    "reserves all requested items when sufficient stock is available",
    () => {
        const repository = new InMemoryInventoryRepository({
            "washing-machine-01": 10,
            "dishwasher-01": 5
        });

        const service = createInventoryService(repository);

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

        const result =
            service.processPaymentAuthorized(sourceEvent);

        assertInventoryReserved(result);

        assert.equal(result.eventId, "generated-id-1");
        assert.equal(
            result.data.reservationId,
            "generated-id-2"
        );
        assert.equal(result.occurredAt, fixedTime);
        assert.equal(
            result.correlationId,
            sourceEvent.correlationId
        );
        assert.equal(
            result.data.orderId,
            sourceEvent.data.orderId
        );
        assert.deepEqual(
            result.data.items,
            sourceEvent.data.items
        );

        assert.deepEqual(repository.getAllStock(), {
            "washing-machine-01": 8,
            "dishwasher-01": 4
        });
    }
);

test(
    "does not reserve any stock when one product is unavailable",
    () => {
        const repository = new InMemoryInventoryRepository({
            "washing-machine-01": 10,
            "dryer-01": 3
        });

        const service = createInventoryService(repository);

        const sourceEvent = createPaymentAuthorizedEvent([
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
        ]);

        const result =
            service.processPaymentAuthorized(sourceEvent);

        assertInventoryReservationFailed(result);

        assert.equal(result.eventId, "generated-id-1");
        assert.equal(result.occurredAt, fixedTime);
        assert.equal(
            result.correlationId,
            sourceEvent.correlationId
        );
        assert.equal(
            result.data.orderId,
            sourceEvent.data.orderId
        );

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

        assert.deepEqual(repository.getAllStock(), {
            "washing-machine-01": 10,
            "dryer-01": 3
        });
    }
);

test(
    "combines duplicate order lines before checking availability",
    () => {
        const repository = new InMemoryInventoryRepository({
            "washing-machine-01": 10
        });

        const service = createInventoryService(repository);

        const sourceEvent = createPaymentAuthorizedEvent([
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
        ]);

        const result =
            service.processPaymentAuthorized(sourceEvent);

        assertInventoryReservationFailed(result);

        assert.deepEqual(result.data.unavailableItems, [
            {
                productId: "washing-machine-01",
                requestedQuantity: 12,
                availableQuantity: 10
            }
        ]);

        assert.deepEqual(repository.getAllStock(), {
            "washing-machine-01": 10
        });
    }
);

test(
    "treats an unknown product as having zero available stock",
    () => {
        const repository = new InMemoryInventoryRepository({
            "washing-machine-01": 10
        });

        const service = createInventoryService(repository);

        const sourceEvent = createPaymentAuthorizedEvent([
            {
                productId: "unknown-product",
                quantity: 1,
                unitPrice: 100
            }
        ]);

        const result =
            service.processPaymentAuthorized(sourceEvent);

        assertInventoryReservationFailed(result);

        assert.deepEqual(result.data.unavailableItems, [
            {
                productId: "unknown-product",
                requestedQuantity: 1,
                availableQuantity: 0
            }
        ]);

        assert.deepEqual(repository.getAllStock(), {
            "washing-machine-01": 10
        });
    }
);

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
    repository: InMemoryInventoryRepository
): InventoryService {
    let generatedIdNumber = 0;

    return new InventoryService(repository, {
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