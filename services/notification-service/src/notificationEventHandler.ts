import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    NotificationService,
    type CustomerNotification,
    type NotificationEvent
} from "./notificationService.js";

export interface NotificationEventHandlerDependencies {
    readonly notificationService:
    NotificationService;

    readonly logger:
    AppLogger;
}

export type NotificationEventHandler = (
    event: NotificationEvent
) => Promise<void>;

export function createNotificationEventHandler(
    dependencies:
        NotificationEventHandlerDependencies
): NotificationEventHandler {
    const {
        notificationService,
        logger
    } = dependencies;

    return async (
        event: NotificationEvent
    ): Promise<void> => {
        const notification =
            notificationService
                .createNotification(event);

        logNotificationCreated(
            event,
            notification,
            logger
        );
    };
}

function logNotificationCreated(
    event: NotificationEvent,
    notification:
        CustomerNotification,
    logger: AppLogger
): void {
    switch (event.eventType) {
        case "DeliveryBooked":
            logger.info(
                "Created customer notification",
                {
                    eventId:
                        event.eventId,
                    notificationId:
                        notification
                            .notificationId,
                    notificationType:
                        notification.type,
                    orderId:
                        notification.orderId,
                    correlationId:
                        notification
                            .correlationId,
                    deliveryId:
                        event.data.deliveryId,
                    carrier:
                        event.data.carrier,
                    estimatedDeliveryDate:
                        event.data
                            .estimatedDeliveryDate
                }
            );
            return;

        case "InventoryReservationFailed":
            logger.info(
                "Created customer notification",
                {
                    eventId:
                        event.eventId,
                    notificationId:
                        notification
                            .notificationId,
                    notificationType:
                        notification.type,
                    orderId:
                        notification.orderId,
                    correlationId:
                        notification
                            .correlationId,
                    reason:
                        event.data.reason,
                    unavailableItemCount:
                        event.data
                            .unavailableItems
                            .length
                }
            );
            return;

        default:
            assertNever(event);
    }
}

function assertNever(
    value: never
): never {
    throw new Error(
        `Unsupported notification event: ` +
        `${JSON.stringify(value)}`
    );
}