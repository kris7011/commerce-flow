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
    createInventoryApp,
    type ReadinessProbe
} from "./app.js";
import {
    createInventoryDatabasePool,
    initializeInventoryDatabase,
    isInventoryDatabaseReady
} from "./database.js";
import {
    InventoryService
} from "./inventoryService.js";
import {
    createPaymentAuthorizedHandler,
    type InventoryResultPublisher
} from "./paymentAuthorizedHandler.js";
import {
    PostgresInventoryRepository
} from "./postgresInventoryRepository.js";

const port =
    Number(
        process.env
            .INVENTORY_SERVICE_PORT ??
        3003
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const logger =
    createStructuredLogger(
        "inventory-service"
    );

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const databasePool =
    createInventoryDatabasePool();

const inventoryRepository =
    new PostgresInventoryRepository(
        databasePool
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
        async signal => {
            await rabbitMq
                .subscribe<PaymentAuthorizedEvent>(
                    "inventory-service." +
                    "payment-authorized",
                    [
                        "payment.authorized"
                    ],
                    handlePaymentAuthorized,
                    signal
                );
        },
        {},
        {
            logger
        }
    );

const databaseReadinessProbe:
    ReadinessProbe = {
    async isReady(): Promise<boolean> {
        return isInventoryDatabaseReady(
            databasePool
        );
    }
};

const app =
    createInventoryApp({
        stockReader:
            inventoryRepository,
        rabbitMqReadinessProbe:
            rabbitMqSupervisor,
        databaseReadinessProbe
    });

async function start():
    Promise<void> {
    await initializeInventoryDatabase(
        databasePool
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

void start()
    .catch(
        async error => {
            logger.error(
                "Service failed to start",
                error,
                {
                    port
                }
            );

            supervisorController.abort();

            await Promise.allSettled([
                rabbitMq.close(),
                databasePool.end()
            ]);

            process.exit(1);
        }
    );

let shuttingDown =
    false;

async function shutdown(
    signal: string
): Promise<void> {
    if (shuttingDown) {
        return;
    }

    shuttingDown =
        true;

    logger.info(
        "Service shutting down",
        {
            signal
        }
    );

    supervisorController.abort();

    await Promise.allSettled([
        rabbitMq.close(),
        databasePool.end()
    ]);

    process.exit(0);
}

process.once(
    "SIGINT",
    () => {
        void shutdown(
            "SIGINT"
        );
    }
);

process.once(
    "SIGTERM",
    () => {
        void shutdown(
            "SIGTERM"
        );
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
