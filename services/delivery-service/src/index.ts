import type {
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
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

const port =
    Number(
        process.env.DELIVERY_SERVICE_PORT ??
        3004
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const logger =
    createStructuredLogger(
        "delivery-service"
    );

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const deliveryService =
    new DeliveryService();

const deliveryBookedPublisher:
    DeliveryBookedPublisher = {
    async publishDeliveryBooked(
        event
    ): Promise<void> {
        await rabbitMq.publish(
            "delivery.booked",
            event
        );
    }
};

const handleInventoryReserved =
    createInventoryReservedHandler({
        deliveryService,
        deliveryBookedPublisher,
        logger
    });

const app =
    createDeliveryApp();

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq
        .subscribe<InventoryReservedEvent>(
            "delivery-service." +
            "inventory-reserved",
            [
                "inventory.reserved"
            ],
            handleInventoryReserved
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