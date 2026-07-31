import express from "express";
import type {
    Request,
    Response
} from "express";
import { RabbitMqClient } from "@commerce-flow/messaging";
import { isCreateOrderRequest } from "./orderRequestValidator.js";
import { OrderService } from "./orderService.js";

const port =
    Number(process.env.ORDER_SERVICE_PORT ?? 3001);

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const app = express();
const rabbitMq = new RabbitMqClient(rabbitMqUrl);
const orderService = new OrderService();

app.use(express.json());

app.get(
    "/health",
    (_request: Request, response: Response) => {
        response.json({
            status: "Healthy",
            service: "order-service"
        });
    }
);

app.post(
    "/orders",
    async (request: Request, response: Response) => {
        if (!isCreateOrderRequest(request.body)) {
            return response.status(400).json({
                error:
                    "Invalid order request. Expected customerId and at least one order item with productId, quantity and unitPrice."
            });
        }

        const result = orderService.createOrder(
            request.body,
            request.header("x-correlation-id")
        );

        await rabbitMq.publish(
            "order.created",
            result.event
        );

        console.log(
            `[order-service] Created order ` +
            `'${result.response.orderId}' ` +
            `with correlationId ` +
            `'${result.response.correlationId}'`
        );

        return response
            .status(201)
            .json(result.response);
    }
);

async function start(): Promise<void> {
    await rabbitMq.connect();

    app.listen(port, () => {
        console.log(
            `[order-service] Listening on port ${port}`
        );
    });
}

start().catch(error => {
    console.error(
        "[order-service] Failed to start service",
        error
    );

    process.exit(1);
});

process.on("SIGINT", async () => {
    await rabbitMq.close();
    process.exit(0);
});
