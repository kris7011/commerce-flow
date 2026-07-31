import assert from "node:assert/strict";
import test from "node:test";
import type {
    DeliveryBookedEvent,
    InventoryReservationFailedEvent
} from "@commerce-flow/contracts";
import {
    NotificationService,
    type CustomerNotification
} from "../src/notificationService.js";

const fixedTime =
    "2026-07-31T10:00:00.000Z";

test(
    "creates and stores a delivery notification",
    () => {
        const service =
            createNotificationService([
                "notification-001"
            ]);

        const sourceEvent =
            createDeliveryBookedEvent();

        const result =
            service.createNotification(sourceEvent);

        assert.deepEqual(result, {
            notificationId: "notification-001",
            orderId: "order-001",
            type: "DeliveryBooked",
            message:
                "Delivery has been booked with " +
                "DefaultCarrier. " +
                "Estimated delivery date: " +
                "2026-08-03.",
            correlationId: "correlation-001",
            createdAt: fixedTime
        });

        assert.deepEqual(
            service.getNotifications(),
            [result]
        );
    }
);

test(
    "creates a notification for a failed inventory reservation",
    () => {
        const service =
            createNotificationService([
                "notification-001"
            ]);

        const sourceEvent =
            createInventoryReservationFailedEvent();

        const result =
            service.createNotification(sourceEvent);

        assert.deepEqual(result, {
            notificationId: "notification-001",
            orderId: "order-002",
            type: "InventoryReservationFailed",
            message:
                "Inventory reservation failed. " +
                "Reason: Insufficient stock. " +
                "Unavailable items: " +
                "dryer-01 requested=4, available=3; " +
                "dishwasher-01 requested=2, " +
                "available=0.",
            correlationId: "correlation-002",
            createdAt: fixedTime
        });

        assert.deepEqual(
            service.getNotifications(),
            [result]
        );
    }
);

test(
    "stores notifications in creation order",
    () => {
        const service =
            createNotificationService([
                "notification-001",
                "notification-002"
            ]);

        service.createNotification(
            createDeliveryBookedEvent()
        );

        service.createNotification(
            createInventoryReservationFailedEvent()
        );

        const notifications =
            service.getNotifications();

        assert.equal(notifications.length, 2);

        assert.deepEqual(
            notifications.map(notification => {
                return notification.notificationId;
            }),
            [
                "notification-001",
                "notification-002"
            ]
        );

        assert.deepEqual(
            notifications.map(notification => {
                return notification.type;
            }),
            [
                "DeliveryBooked",
                "InventoryReservationFailed"
            ]
        );
    }
);

test(
    "does not modify either source event",
    () => {
        const service =
            createNotificationService([
                "notification-001",
                "notification-002"
            ]);

        const deliveryEvent =
            createDeliveryBookedEvent();

        const failureEvent =
            createInventoryReservationFailedEvent();

        const deliveryEventBefore =
            structuredClone(deliveryEvent);

        const failureEventBefore =
            structuredClone(failureEvent);

        service.createNotification(deliveryEvent);
        service.createNotification(failureEvent);

        assert.deepEqual(
            deliveryEvent,
            deliveryEventBefore
        );

        assert.deepEqual(
            failureEvent,
            failureEventBefore
        );
    }
);

test(
    "returns defensive copies of stored notifications",
    () => {
        const service =
            createNotificationService([
                "notification-001"
            ]);

        const created =
            service.createNotification(
                createDeliveryBookedEvent()
            );

        const firstSnapshot =
            service.getNotifications();

        const secondSnapshot =
            service.getNotifications();

        assert.notStrictEqual(
            firstSnapshot,
            secondSnapshot
        );

        assert.notStrictEqual(
            firstSnapshot[0],
            secondSnapshot[0]
        );

        (
            created as {
                message: string;
            }
        ).message = "Externally changed";

        (
            firstSnapshot[0] as {
                message: string;
            }
        ).message = "Snapshot changed";

        const storedNotification =
            service.getNotifications()[0];

        assert.ok(storedNotification);

        assert.equal(
            storedNotification.message,
            "Delivery has been booked with " +
            "DefaultCarrier. " +
            "Estimated delivery date: " +
            "2026-08-03."
        );
    }
);

function createNotificationService(
    generatedIds: string[]
): NotificationService {
    let currentIndex = 0;

    return new NotificationService({
        generateId: () => {
            const generatedId =
                generatedIds[currentIndex];

            if (!generatedId) {
                throw new Error(
                    "The test did not provide " +
                    "enough generated IDs."
                );
            }

            currentIndex += 1;

            return generatedId;
        },
        getCurrentTime: () => fixedTime
    });
}

function createDeliveryBookedEvent():
    DeliveryBookedEvent {
    return {
        eventId: "delivery-event-001",
        eventType: "DeliveryBooked",
        occurredAt:
            "2026-07-31T09:59:00.000Z",
        correlationId: "correlation-001",
        data: {
            orderId: "order-001",
            deliveryId: "delivery-001",
            carrier: "DefaultCarrier",
            estimatedDeliveryDate:
                "2026-08-03"
        }
    };
}

function createInventoryReservationFailedEvent():
    InventoryReservationFailedEvent {
    return {
        eventId: "inventory-event-002",
        eventType:
            "InventoryReservationFailed",
        occurredAt:
            "2026-07-31T09:59:30.000Z",
        correlationId: "correlation-002",
        data: {
            orderId: "order-002",
            reason: "Insufficient stock",
            unavailableItems: [
                {
                    productId: "dryer-01",
                    requestedQuantity: 4,
                    availableQuantity: 3
                },
                {
                    productId: "dishwasher-01",
                    requestedQuantity: 2,
                    availableQuantity: 0
                }
            ]
        }
    };
}
