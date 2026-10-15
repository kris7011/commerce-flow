import {
    ROUTING_KEYS,
    parseOrderCreatedEvent
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
    createPaymentApp
} from "./app.js";
import {
    createOrderCreatedHandler,
    type PaymentAuthorizedPublisher
} from "./orderCreatedHandler.js";
import {
    PaymentService
} from "./paymentService.js";

const logger =
    createStructuredLogger(
        "payment-service"
    );

const port =
    readPort(
        process.env,
        "PAYMENT_SERVICE_PORT",
        3002
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
        {},
        {
            logger:
                logger.child({
                    component:
                        "messaging"
                })
        }
    );

// Publish-then-ack: if the process dies before the acknowledgement,
// the OrderCreated message is redelivered and re-emits the identical
// (deterministically identified) PaymentAuthorized event.
const paymentAuthorizedPublisher:
    PaymentAuthorizedPublisher = {
    async publishPaymentAuthorized(
        event
    ): Promise<void> {
        await rabbitMq.publish(
            ROUTING_KEYS.PaymentAuthorized,
            event
        );
    }
};

const handleOrderCreated =
    createOrderCreatedHandler({
        paymentService:
            new PaymentService(),
        paymentAuthorizedPublisher,
        logger
    });

const rabbitMqSupervisor =
    new RabbitMqSupervisor(
        rabbitMq,
        async signal => {
            await rabbitMq.subscribe({
                queueName:
                    "payment-service.order-created",
                routingKeys: [
                    ROUTING_KEYS.OrderCreated
                ],
                parseEvent:
                    parseOrderCreatedEvent,
                handler:
                    handleOrderCreated,
                signal
            });
        },
        {},
        {
            logger
        }
    );

await runService({
    port,
    requestListener:
        createPaymentApp({
            readinessProbe:
                rabbitMqSupervisor
        }),
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
