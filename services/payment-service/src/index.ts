import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createPaymentApp
} from "./app.js";
import {
    createOrderCreatedHandler,
    type PaymentAuthorizedPublisher
} from "./orderCreatedHandler.js";
import {
    PaymentService
} from "./paymentService.js";

const port =
    Number(
        process.env.PAYMENT_SERVICE_PORT ??
        3002
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const rabbitMq =
    new RabbitMqClient(
        rabbitMqUrl
    );

const paymentService =
    new PaymentService();

const paymentAuthorizedPublisher:
    PaymentAuthorizedPublisher = {
    async publishPaymentAuthorized(
        event
    ): Promise<void> {
        await rabbitMq.publish(
            "payment.authorized",
            event
        );
    }
};

const handleOrderCreated =
    createOrderCreatedHandler({
        paymentService,
        paymentAuthorizedPublisher
    });

const app =
    createPaymentApp();

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq.subscribe<OrderCreatedEvent>(
        "payment-service.order-created",
        [
            "order.created"
        ],
        handleOrderCreated
    );

    app.listen(
        port,
        () => {
            console.log(
                `[payment-service] ` +
                `Listening on port ${port}`
            );
        }
    );
}

start().catch(error => {
    console.error(
        "[payment-service] " +
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