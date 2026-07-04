import express from "express";
import type { Request, Response } from "express";
import { randomUUID } from "node:crypto";
import type {
    DeliveryBookedEvent,
    InventoryReservationFailedEvent
} from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";

type NotificationEvent =
    | DeliveryBookedEvent
    | InventoryReservationFailedEvent;

interface CustomerNotification {
    notificationId: string;
    orderId: string;
    type: "DeliveryBooked" | "InventoryReservationFailed";
    message: string;
    correlationId: string;
    createdAt: string;
}

const port = Number(process.env.NOTIFICATION_SERVICE_PORT ?? 3005);
const rabbitMqUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

const notifications: CustomerNotification[] = [];

app.get("/health", (_request: Request, response: Response) => {
    response.json({
        status: "Healthy",
        service: "notification-service"
    });
});

app.get("/notifications", (_request: Request, response: Response) => {
    response.json({
        notifications
    });
});

async function handleNotificationEvent(event: NotificationEvent): Promise<void> {
    switch (event.eventType) {
        case "DeliveryBooked":
            handleDeliveryBooked(event);
            return;

        case "InventoryReservationFailed":
            handleInventoryReservationFailed(event);
            return;

        default:
            assertNever(event);
    }
}

function handleDeliveryBooked(event: DeliveryBookedEvent): void {
    const notification: CustomerNotification = {
        notificationId: randomUUID(),
        orderId: event.data.orderId,
        type: "DeliveryBooked",
        message: `Delivery has been booked with ${event.data.carrier}. Estimated delivery date: ${event.data.estimatedDeliveryDate}.`,
        correlationId: event.correlationId,
        createdAt: new Date().toISOString()
    };

    notifications.push(notification);

    console.log(
        `[notification-service] Customer notification created for booked delivery on order '${event.data.orderId}' with correlationId '${event.correlationId}'`
    );
}

function handleInventoryReservationFailed(
    event: InventoryReservationFailedEvent
): void {
    const unavailableProducts = event.data.unavailableItems
        .map(item => {
            return `${item.productId} requested=${item.requestedQuantity}, available=${item.availableQuantity}`;
        })
        .join("; ");

    const notification: CustomerNotification = {
        notificationId: randomUUID(),
        orderId: event.data.orderId,
        type: "InventoryReservationFailed",
        message: `Inventory reservation failed. Reason: ${event.data.reason}. Unavailable items: ${unavailableProducts}.`,
        correlationId: event.correlationId,
        createdAt: new Date().toISOString()
    };

    notifications.push(notification);

    console.log(
        `[notification-service] Customer notification created for failed inventory reservation on order '${event.data.orderId}' with correlationId '${event.correlationId}'`
    );
}

function assertNever(value: never): never {
    throw new Error(`Unsupported event type: ${JSON.stringify(value)}`);
}

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq.subscribe<NotificationEvent>(
        "notification-service.customer-events",
        ["delivery.booked", "inventory.reservation.failed"],
        async event => {
            await handleNotificationEvent(event);
        }
    );

    app.listen(port, () => {
        console.log(`[notification-service] Listening on port ${port}`);
    });
}

start().catch(error => {
    console.error("[notification-service] Failed to start service", error);
    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});