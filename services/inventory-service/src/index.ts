import {
    COMMERCE_TOPOLOGY,
    QUEUES,
    parsePaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    RabbitMqSupervisor
} from "@commerce-flow/messaging";
import {
    runService
} from "@commerce-flow/runtime";
import {
    createInventoryApp
} from "./app.js";
import {
    readInventoryConfig
} from "./config.js";
import {
    createInventoryDatabasePool,
    initializeInventoryDatabase,
    isInventoryDatabaseReady
} from "./database.js";
import {
    InventoryService
} from "./inventoryService.js";
import {
    OutboxRelay
} from "./outbox/outboxRelay.js";
import {
    PostgresOutboxStore
} from "./outbox/postgresOutboxStore.js";
import {
    RabbitMqOutboxPublisher
} from "./outbox/rabbitMqOutboxPublisher.js";
import {
    createPaymentAuthorizedHandler
} from "./paymentAuthorizedHandler.js";
import {
    PostgresInventoryStore
} from "./postgresInventoryStore.js";

/*
 * Composition root: the only place that knows about concrete adapters.
 */

const logger =
    createStructuredLogger(
        "inventory-service"
    );

const config =
    readInventoryConfig(
        process.env
    );

const databasePool =
    createInventoryDatabasePool({
        connectionString:
            config.databaseUrl,
        logger
    });

const inventoryStore =
    new PostgresInventoryStore(
        databasePool
    );

const inventoryService =
    new InventoryService(
        inventoryStore
    );

const rabbitMq =
    new RabbitMqClient(
        config.rabbitMqUrl,
        "commerce.events",
        {
            publishTimeoutInMs:
                config.publishTimeoutInMs,
            topology:
                COMMERCE_TOPOLOGY
        },
        {
            logger:
                logger.child({
                    component:
                        "messaging"
                })
        }
    );

const outboxStore =
    new PostgresOutboxStore(
        databasePool
    );

const outboxRelay =
    new OutboxRelay(
        {
            store:
                outboxStore,
            publisher:
                new RabbitMqOutboxPublisher(
                    rabbitMq
                ),
            logger:
                logger.child({
                    component:
                        "outbox-relay"
                })
        },
        config.outbox
    );

const handlePaymentAuthorized =
    createPaymentAuthorizedHandler({
        inventoryService,
        logger,
        onEventEnqueued:
            () => {
                outboxRelay.notify();
            }
    });

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async signal => {
            await rabbitMq.subscribe({
                ...QUEUES.inventoryPaymentAuthorized,
                parseEvent:
                    parsePaymentAuthorizedEvent,
                handler:
                    handlePaymentAuthorized,
                signal
            });
        },
        {},
        {
            logger
        }
    );

const app =
    createInventoryApp({
        stockReader:
            inventoryStore,
        rabbitMqReadinessProbe:
            rabbitMqSupervisor,
        databaseReadinessProbe: {
            isReady: () =>
                isInventoryDatabaseReady(
                    databasePool
                )
        },
        outboxDiagnosticsReader: {
            async getOutboxDiagnostics() {
                return {
                    ...(await outboxStore.getStatistics()),
                    relay:
                        outboxRelay.getStatus()
                };
            }
        },
        logger
    });

await runService({
    port:
        config.port,
    requestListener:
        app,
    logger,

    async start() {
        const migration =
            await initializeInventoryDatabase(
                databasePool
            );

        logger.info(
            "Inventory database ready",
            {
                schemaVersion:
                    migration.currentVersion,
                appliedMigrations:
                    migration.appliedVersions
            }
        );
    },

    backgroundTasks: [
        {
            name:
                "rabbitmq-supervisor",
            run: signal =>
                rabbitMqSupervisor.run(
                    signal
                )
        },
        {
            name:
                "outbox-relay",
            run: signal =>
                outboxRelay.run(
                    signal
                )
        }
    ],

    // Background tasks have stopped at this point: drain in-flight
    // consumers first (they still use the database), then close the
    // pool.
    async stop() {
        await rabbitMq.close();
        await databasePool.end();
    }
});
