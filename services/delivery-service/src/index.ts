import express from "express";
import type { Request, Response } from "express";
import { randomUUID } from "node:crypto";
import type {
    DeliveryBookedEvent,
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";

const port = Number(process.env.DELIVERY_SERVICE_PORT ?? 3004);
const rabbitMqUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

app.get("/health", (_request: Request, response: Response) => {
    response.json({
        status: "Healthy",
        service: "delivery-service"
    });
});

async function handleInventoryReserved(
    event: InventoryReservedEvent
): Promise<void> {
    console.log(
        `[delivery-service] Received InventoryReserved for order '${event.data.orderId}' with correlationId '${event.correlationId}'`
    );

    const deliveryBookedEvent: DeliveryBookedEvent = {
        eventId: randomUUID(),
        eventType: "DeliveryBooked",
        occurredAt: new Date().toISOString(),
        correlationId: event.correlationId,
        data: {
            orderId: event.data.orderId,
            deliveryId: randomUUID(),
            carrier: selectCarrier(),
            estimatedDeliveryDate: calculateEstimatedDeliveryDate()
        }
    };

    await rabbitMq.publish("delivery.booked", deliveryBookedEvent);

    console.log(
        `[delivery-service] Booked delivery for order '${event.data.orderId}'`
    );
}

function selectCarrier(): string {
    return "DefaultCarrier";
}

function calculateEstimatedDeliveryDate(): string {
    const estimatedDate = new Date();
    estimatedDate.setDate(estimatedDate.getDate() + 3);

    return estimatedDate.toISOString().slice(0, 10);
}

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq.subscribe<InventoryReservedEvent>(
        "delivery-service.inventory-reserved",
        ["inventory.reserved"],
        async event => {
            await handleInventoryReserved(event);
        }
    );

    app.listen(port, () => {
        console.log(`[delivery-service] Listening on port ${port}`);
    });
}

start().catch(error => {
    console.error("[delivery-service] Failed to start service", error);
    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});