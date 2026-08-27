import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createOrderApp,
    type OrderCreatedPublisher
} from "./app.js";
import {
    OrderService
} from "./orderService.js";

const port =
    Number(
        process.env.ORDER_SERVICE_PORT ??
        3001
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const logger =
    createStructuredLogger(
        "order-service"
    );

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const orderService =
    new OrderService();

const orderCreatedPublisher:
    OrderCreatedPublisher = {
    async publishOrderCreated(
        event
    ): Promise<void> {
        await rabbitMq.publish(
            "order.created",
            event
        );
    }
};

const app =
    createOrderApp({
        orderService,
        orderCreatedPublisher,
        logger
    });

async function start(): Promise<void> {
    await rabbitMq.connect();

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