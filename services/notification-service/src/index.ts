import {
    ROUTING_KEYS
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    DEVELOPMENT_RABBITMQ_URL,
    RabbitMqClient,
    RabbitMqSupervisor
} from "@commerce-flow/messaging";
import {
    readPort,
    readUrl,
    runService
} from "@commerce-flow/runtime";
import {
    createNotificationApp
} from "./app.js";
import {
    createNotificationEventHandler
} from "./notificationEventHandler.js";
import {
    NotificationService,
    parseNotificationEvent
} from "./notificationService.js";

const logger =
    createStructuredLogger(
        "notification-service"
    );

const port =
    readPort(
        process.env,
        "NOTIFICATION_SERVICE_PORT",
        3005
    );

const rabbitMqUrl =
    readUrl(
        process.env,
        "RABBITMQ_URL",
        DEVELOPMENT_RABBITMQ_URL,
        [
            "amqp:",
            "amqps:"
        ]
    );

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl,
        "commerce.events",
        {},
        {
            logger:
                logger.child({
                    component:
                        "messaging"
                })
        }
    );

const notificationService =
    new NotificationService();

const handleNotificationEvent =
    createNotificationEventHandler({
        notificationService,
        logger
    });

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async signal => {
            await rabbitMq.subscribe({
                queueName:
                    "notification-service.customer-events",
                routingKeys: [
                    ROUTING_KEYS.DeliveryBooked,
                    ROUTING_KEYS.InventoryReservationFailed
                ],
                parseEvent:
                    parseNotificationEvent,
                handler:
                    handleNotificationEvent,
                signal
            });
        },
        {},
        {
            logger
        }
    );

await runService({
    port,
    requestListener:
        createNotificationApp({
            notificationReader:
                notificationService,
            readinessProbe:
                rabbitMqSupervisor
        }),
    logger,
    backgroundTasks: [
        {
            name:
                "rabbitmq-supervisor",
            run: signal =>
                rabbitMqSupervisor.run(
                    signal
                )
        }
    ],
    async stop() {
        await rabbitMq.close();
    }
});
