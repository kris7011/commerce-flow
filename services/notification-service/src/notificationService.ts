import { randomUUID } from "node:crypto";
import type {
    DeliveryBookedEvent,
    InventoryReservationFailedEvent
} from "@commerce-flow/contracts";

export type NotificationEvent =
    | DeliveryBookedEvent
    | InventoryReservationFailedEvent;

export interface CustomerNotification {
    readonly notificationId: string;
    readonly orderId: string;
    readonly type:
    | "DeliveryBooked"
    | "InventoryReservationFailed";
    readonly message: string;
    readonly correlationId: string;
    readonly createdAt: string;
}

export interface NotificationServiceDependencies {
    generateId?: () => string;
    getCurrentTime?: () => string;
}

export class NotificationService {
    private readonly notifications:
        CustomerNotification[] = [];

    private readonly generateId: () => string;
    private readonly getCurrentTime: () => string;

    constructor(
        dependencies:
            NotificationServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId ??
            (() => randomUUID());

        this.getCurrentTime =
            dependencies.getCurrentTime ??
            (() => new Date().toISOString());
    }

    createNotification(
        event: NotificationEvent
    ): CustomerNotification {
        const notification =
            this.buildNotification(event);

        this.notifications.push(notification);

        return {
            ...notification
        };
    }

    getNotifications():
        readonly CustomerNotification[] {
        return this.notifications.map(notification => {
            return {
                ...notification
            };
        });
    }

    private buildNotification(
        event: NotificationEvent
    ): CustomerNotification {
        switch (event.eventType) {
            case "DeliveryBooked":
                return this.createDeliveryNotification(
                    event
                );

            case "InventoryReservationFailed":
                return this.createInventoryFailureNotification(
                    event
                );

            default:
                return assertNever(event);
        }
    }

    private createDeliveryNotification(
        event: DeliveryBookedEvent
    ): CustomerNotification {
        return {
            notificationId: this.generateId(),
            orderId: event.data.orderId,
            type: "DeliveryBooked",
            message:
                `Delivery has been booked with ` +
                `${event.data.carrier}. ` +
                `Estimated delivery date: ` +
                `${event.data.estimatedDeliveryDate}.`,
            correlationId: event.correlationId,
            createdAt: this.getCurrentTime()
        };
    }

    private createInventoryFailureNotification(
        event: InventoryReservationFailedEvent
    ): CustomerNotification {
        const unavailableProducts =
            event.data.unavailableItems
                .map(item => {
                    return (
                        `${item.productId} ` +
                        `requested=${item.requestedQuantity}, ` +
                        `available=${item.availableQuantity}`
                    );
                })
                .join("; ");

        return {
            notificationId: this.generateId(),
            orderId: event.data.orderId,
            type: "InventoryReservationFailed",
            message:
                `Inventory reservation failed. ` +
                `Reason: ${event.data.reason}. ` +
                `Unavailable items: ` +
                `${unavailableProducts}.`,
            correlationId: event.correlationId,
            createdAt: this.getCurrentTime()
        };
    }
}

function assertNever(value: never): never {
    throw new Error(
        `Unsupported notification event: ` +
        `${JSON.stringify(value)}`
    );
}
