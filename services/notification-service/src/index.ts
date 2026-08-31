import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    RabbitMqSupervisor
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

const supervisorController =
    new AbortController();

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async () => {
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
        },
        {},
        {
            logger
        }
    );

const app =
    createNotificationApp({
        notificationReader:
            notificationService,
        readinessProbe:
            rabbitMqSupervisor
    });

function start(): void {
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

    void rabbitMqSupervisor
        .run(
            supervisorController.signal
        )
        .catch(error => {
            logger.error(
                "RabbitMQ supervisor stopped unexpectedly",
                error,
                {
                    port
                }
            );
        });
}

start();

process.on(
    "SIGINT",
    async () => {
        logger.info(
            "Service shutting down"
        );

        supervisorController.abort();

        await rabbitMq.close();

        process.exit(0);
    }
);