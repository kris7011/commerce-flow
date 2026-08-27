import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";
import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import {
    parseCreateOrderRequest
} from "./orderRequestValidator.js";
import {
    OrderService
} from "./orderService.js";

export interface OrderCreatedPublisher {
    publishOrderCreated(
        event: OrderCreatedEvent
    ): Promise<void>;
}

export interface OrderAppLogger {
    log(message: string): void;
}

export interface OrderAppDependencies {
    readonly orderService:
    OrderService;

    readonly orderCreatedPublisher:
    OrderCreatedPublisher;

    readonly logger?:
    OrderAppLogger;
}

export function createOrderApp(
    dependencies:
        OrderAppDependencies
): Express {
    const {
        orderService,
        orderCreatedPublisher,
        logger = console
    } = dependencies;

    const app = express();

    app.use(express.json());

    app.get(
        "/health",
        (
            _request: Request,
            response: Response
        ) => {
            response.json({
                status: "Healthy",
                service: "order-service"
            });
        }
    );

    app.post(
        "/orders",
        async (
            request: Request,
            response: Response
        ) => {
            const validationResult =
                parseCreateOrderRequest(
                    request.body
                );

            if (!validationResult.success) {
                return response
                    .status(400)
                    .json({
                        error:
                            "Invalid order request. " +
                            "Expected customerId and " +
                            "at least one order item " +
                            "with productId, quantity " +
                            "and unitPrice."
                    });
            }

            const result =
                orderService.createOrder(
                    validationResult.data,
                    request.header(
                        "x-correlation-id"
                    )
                );

            await orderCreatedPublisher
                .publishOrderCreated(
                    result.event
                );

            logger.log(
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

    return app;
}