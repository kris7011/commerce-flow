import express from "express";
import type {
    Request,
    Response
} from "express";
import type {
    DeliveryBookedEvent,
    InventoryReservedEvent
} from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";
import { DeliveryService } from "./deliveryService.js";

const port =
    Number(process.env.DELIVERY_SERVICE_PORT ?? 3004);

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);
const deliveryService = new DeliveryService();

app.get(
    "/health",
    (_request: Request, response: Response) => {
        response.json({
            status: "Healthy",
            service: "delivery-service"
        });
    }
);

async function handleInventoryReserved(
    event: InventoryReservedEvent
): Promise<void> {
    console.log(
        `[delivery-service] Received InventoryReserved ` +
        `for order '${event.data.orderId}' ` +
        `with correlationId '${event.correlationId}'`
    );

    const deliveryBookedEvent =
        deliveryService.bookDelivery(event);

    await publishDeliveryBooked(
        deliveryBookedEvent
    );

    console.log(
        `[delivery-service] Booked delivery ` +
        `for order '${event.data.orderId}'`
    );
}

async function publishDeliveryBooked(
    event: DeliveryBookedEvent
): Promise<void> {
    await rabbitMq.publish(
        "delivery.booked",
        event
    );
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
        console.log(
            `[delivery-service] Listening on port ${port}`
        );
    });
}

start().catch(error => {
    console.error(
        "[delivery-service] Failed to start service",
        error
    );

    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});
