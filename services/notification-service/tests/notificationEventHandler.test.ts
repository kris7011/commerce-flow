import assert from "node:assert/strict";
import test from "node:test";
import type {
    DeliveryBookedEvent,
    InventoryReservationFailedEvent
} from "@commerce-flow/contracts";
import {
    createNotificationEventHandler,
    type NotificationEventLogger
} from "../src/notificationEventHandler.js";
import {
    NotificationService
} from "../src/notificationService.js";

const fixedTime =
    "2026-08-06T09:00:00.000Z";

test(
    "stores and logs a delivery notification",
    async () => {
        const notificationService =
            createNotificationService([
                "notification-001"
            ]);

        const logger =
            new RecordingNotificationLogger();

        const handler =
            createNotificationEventHandler({
                notificationService,
                logger
            });

        await handler(
            createDeliveryBookedEvent()
        );

        assert.deepEqual(
            notificationService
                .getNotifications(),
            [
                {
                    notificationId:
                        "notification-001",
                    orderId:
                        "order-001",
                    type:
                        "DeliveryBooked",
                    message:
                        "Delivery has been booked " +
                        "with DefaultCarrier. " +
                        "Estimated delivery date: " +
                        "2026-08-09.",
                    correlationId:
                        "correlation-001",
                    createdAt:
                        fixedTime
                }
            ]
        );

        assert.deepEqual(
            logger.messages,
            [
                "[notification-service] " +
                "Customer notification created " +
                "for booked delivery on order " +
                "'order-001' " +
                "with correlationId " +
                "'correlation-001'"
            ]
        );
    }
);

test(
    "stores and logs an inventory failure notification",
    async () => {
        const notificationService =
            createNotificationService([
                "notification-002"
            ]);

        const logger =
            new RecordingNotificationLogger();

        const handler =
            createNotificationEventHandler({
                notificationService,
                logger
            });

        await handler(
            createInventoryFailureEvent()
        );

        assert.deepEqual(
            notificationService
                .getNotifications(),
            [
                {
                    notificationId:
                        "notification-002",
                    orderId:
                        "order-002",
                    type:
                        "InventoryReservationFailed",
                    message:
                        "Inventory reservation " +
                        "failed. Reason: " +
                        "Insufficient stock. " +
                        "Unavailable items: " +
                        "dryer-01 requested=4, " +
                        "available=3.",
                    correlationId:
                        "correlation-002",
                    createdAt:
                        fixedTime
                }
            ]
        );

        assert.deepEqual(
            logger.messages,
            [
                "[notification-service] " +
                "Customer notification created " +
                "for failed inventory " +
                "reservation on order " +
                "'order-002' " +
                "with correlationId " +
                "'correlation-002'"
            ]
        );
    }
);

class RecordingNotificationLogger
    implements NotificationEventLogger {
    readonly messages:
        string[] = [];

    log(message: string): void {
        this.messages.push(message);
    }
}

function createNotificationService(
    generatedIds: string[]
): NotificationService {
    let currentIndex = 0;

    return new NotificationService({
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

function createDeliveryBookedEvent():
    DeliveryBookedEvent {
    return {
        eventId:
            "delivery-event-001",
        eventType:
            "DeliveryBooked",
        occurredAt:
            "2026-08-06T08:59:00.000Z",
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
                "2026-08-09"
        }
    };
}

function createInventoryFailureEvent():
    InventoryReservationFailedEvent {
    return {
        eventId:
            "inventory-event-002",
        eventType:
            "InventoryReservationFailed",
        occurredAt:
            "2026-08-06T08:59:30.000Z",
        correlationId:
            "correlation-002",
        data: {
            orderId:
                "order-002",
            reason:
                "Insufficient stock",
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
    };
}