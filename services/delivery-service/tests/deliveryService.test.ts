import assert from "node:assert/strict";
import test from "node:test";
import type {
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import { DeliveryService } from "../src/deliveryService.js";

const fixedDate =
    new Date("2026-07-31T09:30:00.000Z");

test(
    "creates a delivery booked event from an inventory reserved event",
    () => {
        const deliveryService =
            createDeliveryService([
                "delivery-event-001",
                "delivery-001"
            ]);

        const sourceEvent =
            createInventoryReservedEvent();

        const result =
            deliveryService.bookDelivery(sourceEvent);

        assert.deepEqual(result, {
            eventId: "delivery-event-001",
            eventType: "DeliveryBooked",
            occurredAt: "2026-07-31T09:30:00.000Z",
            correlationId: "correlation-001",
            data: {
                orderId: "order-001",
                deliveryId: "delivery-001",
                carrier: "DefaultCarrier",
                estimatedDeliveryDate: "2026-08-03"
            }
        });
    }
);

test(
    "preserves the workflow correlation id and order id",
    () => {
        const deliveryService =
            createDeliveryService([
                "delivery-event-001",
                "delivery-001"
            ]);

        const sourceEvent =
            createInventoryReservedEvent({
                correlationId:
                    "workflow-correlation-999",
                orderId: "order-999"
            });

        const result =
            deliveryService.bookDelivery(sourceEvent);

        assert.equal(
            result.correlationId,
            "workflow-correlation-999"
        );

        assert.equal(
            result.data.orderId,
            "order-999"
        );
    }
);

test(
    "calculates the estimated delivery date across a year boundary",
    () => {
        const deliveryService =
            new DeliveryService({
                generateId:
                    createIdGenerator([
                        "delivery-event-001",
                        "delivery-001"
                    ]),
                getCurrentDate: () =>
                    new Date(
                        "2026-12-30T23:30:00.000Z"
                    )
            });

        const result =
            deliveryService.bookDelivery(
                createInventoryReservedEvent()
            );

        assert.equal(
            result.occurredAt,
            "2026-12-30T23:30:00.000Z"
        );

        assert.equal(
            result.data.estimatedDeliveryDate,
            "2027-01-02"
        );
    }
);

test(
    "does not modify the source inventory event",
    () => {
        const deliveryService =
            createDeliveryService([
                "delivery-event-001",
                "delivery-001"
            ]);

        const sourceEvent =
            createInventoryReservedEvent();

        const sourceEventBeforeBooking =
            structuredClone(sourceEvent);

        deliveryService.bookDelivery(sourceEvent);

        assert.deepEqual(
            sourceEvent,
            sourceEventBeforeBooking
        );
    }
);

function createDeliveryService(
    generatedIds: string[]
): DeliveryService {
    return new DeliveryService({
        generateId:
            createIdGenerator(generatedIds),
        getCurrentDate: () =>
            new Date(fixedDate.getTime())
    });
}

function createIdGenerator(
    generatedIds: string[]
): () => string {
    let currentIndex = 0;

    return () => {
        const generatedId =
            generatedIds[currentIndex];

        if (!generatedId) {
            throw new Error(
                "The test did not provide enough generated IDs."
            );
        }

        currentIndex += 1;

        return generatedId;
    };
}

interface InventoryReservedEventOverrides {
    correlationId?: string;
    orderId?: string;
}

function createInventoryReservedEvent(
    overrides: InventoryReservedEventOverrides = {}
): InventoryReservedEvent {
    return {
        eventId: "inventory-event-001",
        eventType: "InventoryReserved",
        occurredAt: "2026-07-31T09:29:00.000Z",
        correlationId:
            overrides.correlationId ??
            "correlation-001",
        data: {
            orderId:
                overrides.orderId ??
                "order-001",
            reservationId: "reservation-001",
            items: [
                {
                    productId: "washing-machine-01",
                    quantity: 1,
                    unitPrice: 4999
                }
            ]
        }
    };
}
