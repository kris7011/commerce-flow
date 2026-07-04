import express, { Request, Response } from "express";
import { randomUUID } from "node:crypto";
import { OrderCreatedEvent, OrderItem } from "@commerce-flow/contracts";
import { RabbitMqClient } from "@commerce-flow/messaging";

interface CreateOrderRequest {
    customerId: string;
    items: OrderItem[];
}

const port = Number(process.env.ORDER_SERVICE_PORT ?? 3001);
const rabbitMqUrl =
    process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);

app.use(express.json());

app.get("/health", (_request: Request, response: Response) => {
    response.json({
        status: "Healthy",
        service: "order-service"
    });
});

app.post("/orders", async (request: Request, response: Response) => {
    if (!isCreateOrderRequest(request.body)) {
        return response.status(400).json({
            error:
                "Invalid order request. Expected customerId and at least one order item with productId, quantity and unitPrice."
        });
    }

    const correlationId =
        request.header("x-correlation-id") ?? randomUUID();

    const orderId = randomUUID();
    const totalAmount = calculateTotalAmount(request.body.items);

    const event: OrderCreatedEvent = {
        eventId: randomUUID(),
        eventType: "OrderCreated",
        occurredAt: new Date().toISOString(),
        correlationId,
        data: {
            orderId,
            customerId: request.body.customerId,
            items: request.body.items,
            totalAmount
        }
    };

    await rabbitMq.publish("order.created", event);

    console.log(
        `[order-service] Created order '${orderId}' with correlationId '${correlationId}'`
    );

    return response.status(201).json({
        orderId,
        status: "Created",
        totalAmount,
        correlationId
    });
});

app.listen(port, async () => {
    await rabbitMq.connect();

    console.log(`[order-service] Listening on port ${port}`);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});

function calculateTotalAmount(items: OrderItem[]): number {
    const total = items.reduce((sum, item) => {
        return sum + item.quantity * item.unitPrice;
    }, 0);

    return Number(total.toFixed(2));
}

function isCreateOrderRequest(input: unknown): input is CreateOrderRequest {
    const body = input as Partial<CreateOrderRequest>;

    return (
        typeof body.customerId === "string" &&
        Array.isArray(body.items) &&
        body.items.length > 0 &&
        body.items.every(item => {
            return (
                typeof item.productId === "string" &&
                typeof item.quantity === "number" &&
                item.quantity > 0 &&
                typeof item.unitPrice === "number" &&
                item.unitPrice >= 0
            );
        })
    );
}