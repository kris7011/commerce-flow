import {
    fileURLToPath
} from "node:url";
import {
    COMMERCE_TOPOLOGY,
    QUEUES
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    RabbitMqSupervisor
} from "@commerce-flow/messaging";
import {
    OutboxRelay,
    PostgresOutboxStore,
    RabbitMqOutboxPublisher
} from "@commerce-flow/persistence";
import {
    runService
} from "@commerce-flow/runtime";
import {
    createOrderApp
} from "./app.js";
import {
    readOrderConfig
} from "./config.js";
import {
    createOrderDatabasePool,
    isOrderDatabaseReady,
    ORDER_OUTBOX_TABLE
} from "./database.js";
import {
    migrateOrderDatabase
} from "./migrations.js";
import {
    OrderService
} from "./orderService.js";
import {
    createOrderWorkflowEventHandler,
    parseOrderWorkflowEvent
} from "./orderWorkflowEventHandler.js";
import {
    PostgresOrderStore
} from "./postgresOrderStore.js";

/*
 * Composition root: the only place that knows about concrete adapters.
 */

const logger =
    createStructuredLogger(
        "order-service"
    );

const config =
    readOrderConfig(
        process.env
    );

const databasePool =
    createOrderDatabasePool({
        connectionString:
            config.databaseUrl,
        logger
    });

const orderStore =
    new PostgresOrderStore(
        databasePool
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
        databasePool,
        ORDER_OUTBOX_TABLE
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

const handleWorkflowEvent =
    createOrderWorkflowEventHandler({
        store:
            orderStore,
        logger
    });

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async signal => {
            await rabbitMq.subscribe({
                ...QUEUES.orderWorkflowEvents,
                parseEvent:
                    parseOrderWorkflowEvent,
                handler:
                    handleWorkflowEvent,
                signal
            });
        },
        {},
        {
            logger
        }
    );

const app =
    createOrderApp({
        orderService:
            new OrderService(),
        orderStore,
        logger,
        rabbitMqReadinessProbe:
            rabbitMqSupervisor,
        databaseReadinessProbe: {
            isReady: () =>
                isOrderDatabaseReady(
                    databasePool
                )
        },
        onOrderAccepted:
            () => {
                outboxRelay.notify();
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
        // ../public works from src/ (tsx) and dist/ (compiled) alike.
        publicDirectory:
            fileURLToPath(
                new URL(
                    "../public/",
                    import.meta.url
                )
            )
    });

await runService({
    port:
        config.port,
    requestListener:
        app,
    logger,

    async start() {
        const migration =
            await migrateOrderDatabase(
                databasePool
            );

        logger.info(
            "Order database ready",
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
