import {
    ROUTING_KEYS,
    parseInventoryReservedEvent
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
    createDeliveryApp
} from "./app.js";
import {
    DeliveryService
} from "./deliveryService.js";
import {
    createInventoryReservedHandler,
    type DeliveryBookedPublisher
} from "./inventoryReservedHandler.js";

const logger =
    createStructuredLogger(
        "delivery-service"
    );

const port =
    readPort(
        process.env,
        "DELIVERY_SERVICE_PORT",
        3004
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

// Publish-then-ack with deterministic ids, see payment-service.
const deliveryBookedPublisher:
    DeliveryBookedPublisher = {
    async publishDeliveryBooked(
        event
    ): Promise<void> {
        await rabbitMq.publish(
            ROUTING_KEYS.DeliveryBooked,
            event
        );
    }
};

const handleInventoryReserved =
    createInventoryReservedHandler({
        deliveryService:
            new DeliveryService(),
        deliveryBookedPublisher,
        logger
    });

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async signal => {
            await rabbitMq.subscribe({
                queueName:
                    "delivery-service.inventory-reserved",
                routingKeys: [
                    ROUTING_KEYS.InventoryReserved
                ],
                parseEvent:
                    parseInventoryReservedEvent,
                handler:
                    handleInventoryReserved,
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
        createDeliveryApp({
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
