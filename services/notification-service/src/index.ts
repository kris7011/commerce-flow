import {
    createStructuredLogger
} from "@commerce-flow/logging";
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

const logger =
    createStructuredLogger(
        "notification-service"
    );

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const notificationService =
    new NotificationService();

const handleNotificationEvent =
    createNotificationEventHandler({
        notificationService,
        logger
    });

const app =
    createNotificationApp({
        notificationReader:
            notificationService,
        readinessProbe:
            rabbitMq
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
            logger.info(
                "Service listening",
                {
                    port
                }
            );
        }
    );
}

start().catch(error => {
    logger.error(
        "Failed to start service",
        error,
        {
            port
        }
    );

    process.exit(1);
});

process.on(
    "SIGINT",
    async () => {
        logger.info(
            "Service shutting down"
        );

        await rabbitMq.close();

        process.exit(0);
    }
);