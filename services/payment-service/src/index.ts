import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import {
    createStructuredLogger
} from "@commerce-flow/logging";
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

const logger =
    createStructuredLogger(
        "payment-service"
    );

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
        paymentAuthorizedPublisher,
        logger
    });

const app =
    createPaymentApp({
        readinessProbe:
            rabbitMq
    });

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