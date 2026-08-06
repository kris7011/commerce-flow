import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createNotificationApp
} from "./app.js";
import {
    createNotificationEventHandler
} from "./notificationEventHandler.js";
import {
    NotificationService,
    type NotificationEvent
} from "./notificationService.js";

const port =
    Number(
        process.env
            .NOTIFICATION_SERVICE_PORT ??
        3005
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const notificationService =
    new NotificationService();

const handleNotificationEvent =
    createNotificationEventHandler({
        notificationService
    });

const app =
    createNotificationApp({
        notificationReader:
            notificationService
    });

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq
        .subscribe<NotificationEvent>(
            "notification-service." +
            "customer-events",
            [
                "delivery.booked",
                "inventory.reservation.failed"
            ],
            handleNotificationEvent
        );

    app.listen(
        port,
        () => {
            console.log(
                `[notification-service] ` +
                `Listening on port ${port}`
            );
        }
    );
}

start().catch(error => {
    console.error(
        "[notification-service] " +
        "Failed to start service",
        error
    );

    process.exit(1);
});

process.on(
    "SIGINT",
    async () => {
        await rabbitMq.close();

        process.exit(0);
    }
);