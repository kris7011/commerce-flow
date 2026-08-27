import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";
import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
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

export interface ReadinessProbe {
    isReady(): boolean;
}

export interface OrderAppDependencies {
    readonly orderService:
    OrderService;

    readonly orderCreatedPublisher:
    OrderCreatedPublisher;

    readonly logger:
    AppLogger;

    readonly readinessProbe:
    ReadinessProbe;
}

export function createOrderApp(
    dependencies:
        OrderAppDependencies
): Express {
    const {
        orderService,
        orderCreatedPublisher,
        logger,
        readinessProbe
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

    app.get(
        "/ready",
        (
            _request: Request,
            response: Response
        ) => {
            const rabbitMqReady =
                readinessProbe.isReady();

            const status =
                rabbitMqReady
                    ? "Ready"
                    : "NotReady";

            return response
                .status(
                    rabbitMqReady
                        ? 200
                        : 503
                )
                .json({
                    status,
                    service:
                        "order-service",
                    dependencies: {
                        rabbitMq:
                            status
                    }
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
                logger.warn(
                    "Rejected invalid order request",
                    {
                        validationIssues:
                            validationResult
                                .error
                                .issues
                                .map(issue => ({
                                    path:
                                        issue.path
                                            .join("."),
                                    code:
                                        issue.code,
                                    message:
                                        issue.message
                                }))
                    }
                );

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

            logger.info(
                "Created order",
                {
                    orderId:
                        result.response.orderId,
                    eventId:
                        result.event.eventId,
                    customerId:
                        validationResult
                            .data.customerId,
                    correlationId:
                        result.response
                            .correlationId,
                    totalAmount:
                        result.response
                            .totalAmount,
                    itemCount:
                        validationResult
                            .data.items.length
                }
            );

            return response
                .status(201)
                .json(result.response);
        }
    );

    return app;
}