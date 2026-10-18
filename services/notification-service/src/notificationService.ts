import { randomUUID } from "node:crypto";
import {
    parseDeliveryBookedEvent,
    parseInventoryReservationFailedEvent,
    type DeliveryBookedEvent,
    type InventoryReservationFailedEvent
} from "@commerce-flow/contracts";

export type NotificationEvent =
    | DeliveryBookedEvent
    | InventoryReservationFailedEvent;

export function parseNotificationEvent(
    value: unknown
): NotificationEvent {
    const eventType =
        typeof value === "object" &&
        value !== null &&
        "eventType" in value
            ? value.eventType
            : undefined;

    switch (eventType) {
        case "DeliveryBooked":
            return parseDeliveryBookedEvent(
                value
            );

        case "InventoryReservationFailed":
            return parseInventoryReservationFailedEvent(
                value
            );

        default:
            throw new Error(
                `Unsupported notification event type: ` +
                `${String(eventType)}`
            );
    }
}

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

    findNotification(
        orderId: string,
        type: CustomerNotification["type"]
    ): CustomerNotification | undefined {
        const existing =
            this.notifications.find(notification => {
                return (
                    notification.orderId === orderId &&
                    notification.type === type
                );
            });

        return existing
            ? {
                ...existing
            }
            : undefined;
    }

    /**
     * Idempotent per order and notification type: a customer is told
     * about each outcome once, even if the event is delivered again
     * (including re-emissions with a different event id).
     */
    createNotification(
        event: NotificationEvent
    ): CustomerNotification {
        const existing =
            this.findNotification(
                event.data.orderId,
                event.eventType
            );

        if (existing) {
            return existing;
        }

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
                // The reason is free text and may end with a period.
                `Reason: ${event.data.reason.replace(/[.\s]+$/, "")}. ` +
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
