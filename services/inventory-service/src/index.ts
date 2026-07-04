import express from "express";
import type { Request, Response } from "express";
import { randomUUID } from "node:crypto";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";

const port = Number(process.env.INVENTORY_SERVICE_PORT ?? 3003);
const rabbitMqUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

const stockByProductId = new Map<string, number>([
    ["washing-machine-01", 10],
    ["dishwasher-01", 5],
    ["dryer-01", 3]
]);

app.get("/health", (_request: Request, response: Response) => {
    response.json({
        status: "Healthy",
        service: "inventory-service"
    });
});

app.get("/stock", (_request: Request, response: Response) => {
    response.json({
        stock: Object.fromEntries(stockByProductId)
    });
});

async function handlePaymentAuthorized(
    event: PaymentAuthorizedEvent
): Promise<void> {
    console.log(
        `[inventory-service] Received PaymentAuthorized for order '${event.data.orderId}' with correlationId '${event.correlationId}'`
    );

    const unavailableItems = findUnavailableItems(event.data.items);

    if (unavailableItems.length > 0) {
        const failedEvent: InventoryReservationFailedEvent = {
            eventId: randomUUID(),
            eventType: "InventoryReservationFailed",
            occurredAt: new Date().toISOString(),
            correlationId: event.correlationId,
            data: {
                orderId: event.data.orderId,
                reason: "One or more products are not available in the requested quantity.",
                unavailableItems
            }
        };

        await rabbitMq.publish("inventory.reservation.failed", failedEvent);

        console.log(
            `[inventory-service] Inventory reservation failed for order '${event.data.orderId}'`
        );

        return;
    }

    reserveStock(event.data.items);

    const inventoryReservedEvent: InventoryReservedEvent = {
        eventId: randomUUID(),
        eventType: "InventoryReserved",
        occurredAt: new Date().toISOString(),
        correlationId: event.correlationId,
        data: {
            orderId: event.data.orderId,
            reservationId: randomUUID(),
            items: event.data.items
        }
    };

    await rabbitMq.publish("inventory.reserved", inventoryReservedEvent);

    console.log(
        `[inventory-service] Reserved inventory for order '${event.data.orderId}'`
    );
}

function findUnavailableItems(items: OrderItem[]): {
    productId: string;
    requestedQuantity: number;
    availableQuantity: number;
}[] {
    return items
        .map(item => {
            const availableQuantity = stockByProductId.get(item.productId) ?? 0;

            return {
                productId: item.productId,
                requestedQuantity: item.quantity,
                availableQuantity
            };
        })
        .filter(item => item.availableQuantity < item.requestedQuantity);
}

function reserveStock(items: OrderItem[]): void {
    for (const item of items) {
        const currentQuantity = stockByProductId.get(item.productId) ?? 0;
        stockByProductId.set(item.productId, currentQuantity - item.quantity);
    }
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
        console.log(`[inventory-service] Listening on port ${port}`);
    });
}

start().catch(error => {
    console.error("[inventory-service] Failed to start service", error);
    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});