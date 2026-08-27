import assert from "node:assert/strict";
import test from "node:test";
import type {
    DeliveryBookedEvent,
    InventoryReservationFailedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import {
    createNotificationEventHandler
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
            logger.infoLogs,
            [
                {
                    message:
                        "Created customer notification",
                    context: {
                        eventId:
                            "delivery-event-001",
                        notificationId:
                            "notification-001",
                        notificationType:
                            "DeliveryBooked",
                        orderId:
                            "order-001",
                        correlationId:
                            "correlation-001",
                        deliveryId:
                            "delivery-001",
                        carrier:
                            "DefaultCarrier",
                        estimatedDeliveryDate:
                            "2026-08-09"
                    }
                }
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
            logger.infoLogs,
            [
                {
                    message:
                        "Created customer notification",
                    context: {
                        eventId:
                            "inventory-event-002",
                        notificationId:
                            "notification-002",
                        notificationType:
                            "InventoryReservationFailed",
                        orderId:
                            "order-002",
                        correlationId:
                            "correlation-002",
                        reason:
                            "Insufficient stock",
                        unavailableItemCount:
                            1
                    }
                }
            ]
        );
    }
);

class RecordingNotificationLogger
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