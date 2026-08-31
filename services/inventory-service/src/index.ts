import type {
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    RabbitMqSupervisor
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

const logger =
    createStructuredLogger(
        "inventory-service"
    );

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
        inventoryResultPublisher,
        logger
    });

const supervisorController =
    new AbortController();

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async () => {
            await rabbitMq
                .subscribe<PaymentAuthorizedEvent>(
                    "inventory-service." +
                    "payment-authorized",
                    [
                        "payment.authorized"
                    ],
                    handlePaymentAuthorized
                );
        },
        {},
        {
            logger
        }
    );

const app =
    createInventoryApp({
        stockReader:
            inventoryRepository,
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

function assertNever(
    value: never
): never {
    throw new Error(
        `Unsupported inventory result event: ` +
        `${JSON.stringify(value)}`
    );
}