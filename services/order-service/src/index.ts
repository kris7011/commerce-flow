import {
    COMMERCE_TOPOLOGY,
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
    createOrderApp,
    type OrderCreatedPublisher
} from "./app.js";
import {
    OrderService
} from "./orderService.js";

const logger =
    createStructuredLogger(
        "order-service"
    );

const port =
    readPort(
        process.env,
        "ORDER_SERVICE_PORT",
        3001
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
        {
            // Bounds how long an HTTP request waits for the broker
            // before answering 503.
            publishTimeoutInMs:
                5_000,
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

const orderCreatedPublisher:
    OrderCreatedPublisher = {
    async publishOrderCreated(
        event
    ): Promise<void> {
        await rabbitMq.publish(
            ROUTING_KEYS.OrderCreated,
            event
        );
    }
};

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async signal => {
            await rabbitMq.connect(
                signal
            );
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
        orderCreatedPublisher,
        logger,
        readinessProbe:
            rabbitMqSupervisor
    });

await runService({
    port,
    requestListener:
        app,
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
