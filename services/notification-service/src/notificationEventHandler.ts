import {
    NotificationService,
    type CustomerNotification,
    type NotificationEvent
} from "./notificationService.js";

export interface NotificationEventLogger {
    log(message: string): void;
}

export interface NotificationEventHandlerDependencies {
    readonly notificationService:
    NotificationService;

    readonly logger?:
    NotificationEventLogger;
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
        logger = console
    } = dependencies;

    return async (
        event: NotificationEvent
    ): Promise<void> => {
        const notification =
            notificationService
                .createNotification(event);

        logNotificationCreated(
            notification,
            logger
        );
    };
}

function logNotificationCreated(
    notification:
        CustomerNotification,
    logger:
        NotificationEventLogger
): void {
    switch (notification.type) {
        case "DeliveryBooked":
            logger.log(
                `[notification-service] ` +
                `Customer notification created ` +
                `for booked delivery on order ` +
                `'${notification.orderId}' ` +
                `with correlationId ` +
                `'${notification.correlationId}'`
            );
            return;

        case "InventoryReservationFailed":
            logger.log(
                `[notification-service] ` +
                `Customer notification created ` +
                `for failed inventory reservation ` +
                `on order ` +
                `'${notification.orderId}' ` +
                `with correlationId ` +
                `'${notification.correlationId}'`
            );
            return;

        default:
            assertNever(
                notification.type
            );
    }
}

function assertNever(
    value: never
): never {
    throw new Error(
        `Unsupported notification type: ` +
        `${value}`
    );
}