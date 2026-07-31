import express from "express";
import type { Request, Response } from "express";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";
import { InMemoryInventoryRepository } from "./inMemoryInventoryRepository.js";
import {
    InventoryService,
    type InventoryResultEvent
} from "./inventoryService.js";

const port = Number(process.env.INVENTORY_SERVICE_PORT ?? 3003);

const rabbitMqUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const initialStock: Readonly<Record<string, number>> = {
    "washing-machine-01": 10,
    "dishwasher-01": 5,
    "dryer-01": 3
};

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

const inventoryRepository =
    new InMemoryInventoryRepository(initialStock);

const inventoryService =
    new InventoryService(inventoryRepository);

app.get("/health", (_request: Request, response: Response) => {
    response.json({
        status: "Healthy",
        service: "inventory-service"
    });
});

app.get("/stock", (_request: Request, response: Response) => {
    response.json({
        stock: inventoryRepository.getAllStock()
    });
});

async function handlePaymentAuthorized(
    event: PaymentAuthorizedEvent
): Promise<void> {
    console.log(
        `[inventory-service] Received PaymentAuthorized ` +
        `for order '${event.data.orderId}' ` +
        `with correlationId '${event.correlationId}'`
    );

    const resultEvent =
        inventoryService.processPaymentAuthorized(event);

    await publishInventoryResult(resultEvent);

    logInventoryResult(resultEvent);
}

async function publishInventoryResult(
    event: InventoryResultEvent
): Promise<void> {
    switch (event.eventType) {
        case "InventoryReserved":
            await rabbitMq.publish(
                "inventory.reserved",
                event
            );
            return;

        case "InventoryReservationFailed":
            await rabbitMq.publish(
                "inventory.reservation.failed",
                event
            );
            return;

        default:
            assertNever(event);
    }
}

function logInventoryResult(event: InventoryResultEvent): void {
    switch (event.eventType) {
        case "InventoryReserved":
            logInventoryReserved(event);
            return;

        case "InventoryReservationFailed":
            logInventoryReservationFailed(event);
            return;

        default:
            assertNever(event);
    }
}

function logInventoryReserved(
    event: InventoryReservedEvent
): void {
    console.log(
        `[inventory-service] Reserved inventory ` +
        `for order '${event.data.orderId}'`
    );
}

function logInventoryReservationFailed(
    event: InventoryReservationFailedEvent
): void {
    console.log(
        `[inventory-service] Inventory reservation failed ` +
        `for order '${event.data.orderId}'`
    );
}

function assertNever(value: never): never {
    throw new Error(
        `Unsupported inventory result event: ${JSON.stringify(value)}`
    );
}

async function start(): Promise<void> {
    await rabbitMq.connect();

    await rabbitMq.subscribe<PaymentAuthorizedEvent>(
        "inventory-service.payment-authorized",
        ["payment.authorized"],
        async event => {
            await handlePaymentAuthorized(event);
        }
    );

    app.listen(port, () => {
        console.log(
            `[inventory-service] Listening on port ${port}`
        );
    });
}

start().catch(error => {
    console.error(
        "[inventory-service] Failed to start service",
        error
    );

    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});