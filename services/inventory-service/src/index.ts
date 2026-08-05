import type {
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createInventoryApp
} from "./app.js";
import {
    InMemoryInventoryRepository
} from "./inMemoryInventoryRepository.js";
import {
    InventoryService
} from "./inventoryService.js";
import {
    createPaymentAuthorizedHandler,
    type InventoryResultPublisher
} from "./paymentAuthorizedHandler.js";

const port =
    Number(
        process.env
            .INVENTORY_SERVICE_PORT ??
        3003
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const initialStock:
    Readonly<Record<string, number>> = {
    "washing-machine-01": 10,
    "dishwasher-01": 5,
    "dryer-01": 3
};

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const inventoryRepository =
    new InMemoryInventoryRepository(
        initialStock
    );

const inventoryService =
    new InventoryService(
        inventoryRepository
    );

const inventoryResultPublisher:
    InventoryResultPublisher = {
    async publishInventoryResult(
        event
    ): Promise<void> {
        switch (event.eventType) {
            case "InventoryReserved":
                await rabbitMq.publish(
                    "inventory.reserved",
                    event
                );
                return;

            case "InventoryReservationFailed":
                await rabbitMq.publish(
                    "inventory.reservation.failed",
                    event
                );
                return;

            default:
                assertNever(event);
        }
    }
};

const handlePaymentAuthorized =
    createPaymentAuthorizedHandler({
        inventoryService,
        inventoryResultPublisher
    });

const app =
    createInventoryApp({
        stockReader:
            inventoryRepository
    });

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq
        .subscribe<PaymentAuthorizedEvent>(
            "inventory-service." +
            "payment-authorized",
            [
                "payment.authorized"
            ],
            handlePaymentAuthorized
        );

    app.listen(
        port,
        () => {
            console.log(
                `[inventory-service] ` +
                `Listening on port ${port}`
            );
        }
    );
}

start().catch(error => {
    console.error(
        "[inventory-service] " +
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

function assertNever(
    value: never
): never {
    throw new Error(
        `Unsupported inventory result event: ` +
        `${JSON.stringify(value)}`
    );
}