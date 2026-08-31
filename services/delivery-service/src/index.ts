import type {
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    RabbitMqSupervisor
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

const supervisorController =
    new AbortController();

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async () => {
            await rabbitMq
                .subscribe<InventoryReservedEvent>(
                    "delivery-service." +
                    "inventory-reserved",
                    [
                        "inventory.reserved"
                    ],
                    handleInventoryReserved
                );
        },
        {},
        {
            logger
        }
    );

const app =
    createDeliveryApp({
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