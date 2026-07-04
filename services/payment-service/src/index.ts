import express, { Request, Response } from "express";
import { randomUUID } from "node:crypto";
import {
    OrderCreatedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";

const port = Number(process.env.PAYMENT_SERVICE_PORT ?? 3002);
const rabbitMqUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

app.get("/health", (_request: Request, response: Response) => {
    response.json({
        status: "Healthy",
        service: "payment-service"
    });
});

async function handleOrderCreated(event: OrderCreatedEvent): Promise<void> {
    console.log(
        `[payment-service] Received OrderCreated for order '${event.data.orderId}' with correlationId '${event.correlationId}'`
    );

    const paymentAuthorizedEvent: PaymentAuthorizedEvent = {
        eventId: randomUUID(),
        eventType: "PaymentAuthorized",
        occurredAt: new Date().toISOString(),
        correlationId: event.correlationId,
        data: {
            orderId: event.data.orderId,
            paymentId: randomUUID(),
            amount: event.data.totalAmount
        }
    };

    await rabbitMq.publish("payment.authorized", paymentAuthorizedEvent);

    console.log(
        `[payment-service] Authorized payment for order '${event.data.orderId}'`
    );
}

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq.subscribe<OrderCreatedEvent>(
        "payment-service.order-created",
        ["order.created"],
        async event => {
            await handleOrderCreated(event);
        }
    );

    app.listen(port, () => {
        console.log(`[payment-service] Listening on port ${port}`);
    });
}

start().catch(error => {
    console.error("[payment-service] Failed to start service", error);
    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});