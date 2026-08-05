import type {
    InventoryReservedEvent
} from "@commerce-flow/contracts";
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
        deliveryBookedPublisher
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
            console.log(
                `[delivery-service] ` +
                `Listening on port ${port}`
            );
        }
    );
}

start().catch(error => {
    console.error(
        "[delivery-service] " +
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