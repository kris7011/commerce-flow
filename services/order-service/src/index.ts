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
        orderCreatedPublisher
    });

async function start(): Promise<void> {
    await rabbitMq.connect();

    app.listen(
        port,
        () => {
            console.log(
                `[order-service] ` +
                `Listening on port ${port}`
            );
        }
    );
}

start().catch(error => {
    console.error(
        "[order-service] " +
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