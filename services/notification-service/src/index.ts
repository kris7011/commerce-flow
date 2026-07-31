import express from "express";
import type {
    Request,
    Response
} from "express";
import { RabbitMqClient } from "@commerce-flow/messaging";
import {
    NotificationService,
    type CustomerNotification,
    type NotificationEvent
} from "./notificationService.js";

const port =
    Number(
        process.env.NOTIFICATION_SERVICE_PORT ??
        3005
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

const notificationService =
    new NotificationService();

app.get(
    "/health",
    (_request: Request, response: Response) => {
        response.json({
            status: "Healthy",
            service: "notification-service"
        });
    }
);

app.get(
    "/notifications",
    (_request: Request, response: Response) => {
        response.json({
            notifications:
                notificationService
                    .getNotifications()
        });
    }
);

async function handleNotificationEvent(
    event: NotificationEvent
): Promise<void> {
    const notification =
        notificationService
            .createNotification(event);

    logNotificationCreated(notification);
}

function logNotificationCreated(
    notification: CustomerNotification
): void {
    switch (notification.type) {
        case "DeliveryBooked":
            console.log(
                `[notification-service] ` +
                `Customer notification created ` +
                `for booked delivery on order ` +
                `'${notification.orderId}' ` +
                `with correlationId ` +
                `'${notification.correlationId}'`
            );
            return;

        case "InventoryReservationFailed":
            console.log(
                `[notification-service] ` +
                `Customer notification created ` +
                `for failed inventory reservation ` +
                `on order '${notification.orderId}' ` +
                `with correlationId ` +
                `'${notification.correlationId}'`
            );
            return;

        default:
            assertNever(notification.type);
    }
}

function assertNever(value: never): never {
    throw new Error(
        `Unsupported notification type: ${value}`
    );
}

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq.subscribe<NotificationEvent>(
        "notification-service.customer-events",
        [
            "delivery.booked",
            "inventory.reservation.failed"
        ],
        async event => {
            await handleNotificationEvent(event);
        }
    );

    app.listen(port, () => {
        console.log(
            `[notification-service] ` +
            `Listening on port ${port}`
        );
    });
}

start().catch(error => {
    console.error(
        "[notification-service] " +
        "Failed to start service",
        error
    );

    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});
