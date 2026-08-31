import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient,
    RabbitMqSupervisor
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

const supervisorController =
    new AbortController();

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async () => {
            await rabbitMq.connect();
        },
        {},
        {
            logger
        }
    );

const app =
    createOrderApp({
        orderService,
        orderCreatedPublisher,
        logger,
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