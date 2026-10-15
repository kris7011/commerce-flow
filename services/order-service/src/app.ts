import express from "express";
import type {
    Express,
    NextFunction,
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

    // Do not advertise the framework version to clients.
    app.disable(
        "x-powered-by"
    );

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

            try {
                // Resolves only after the broker confirms the event.
                await orderCreatedPublisher
                    .publishOrderCreated(
                        result.event
                    );
            } catch (error) {
                // Nothing has been persisted, so the client can safely
                // retry. (If the broker received the event but the
                // confirm was lost, a retry creates a second order;
                // see the README's known limitations.)
                logger.error(
                    "Could not publish OrderCreated; order not accepted",
                    error,
                    {
                        orderId:
                            result.response.orderId,
                        eventId:
                            result.event.eventId,
                        correlationId:
                            result.response
                                .correlationId
                    }
                );

                return response
                    .status(503)
                    .set(
                        "Retry-After",
                        "5"
                    )
                    .json({
                        error:
                            "The order could not be accepted " +
                            "because the message broker is " +
                            "unavailable. Please retry later.",
                        correlationId:
                            result.response
                                .correlationId
                    });
            }

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
                .set(
                    "x-correlation-id",
                    result.response.correlationId
                )
                .json(result.response);
        }
    );

    app.use(
        (
            error: unknown,
            request: Request,
            response: Response,
            // Express identifies error middleware by its arity.
            _next: NextFunction
        ) => {
            if (isMalformedJsonError(error)) {
                return response
                    .status(400)
                    .json({
                        error:
                            "The request body is not valid JSON."
                    });
            }

            logger.error(
                "Request failed",
                error,
                {
                    method:
                        request.method,
                    path:
                        request.path
                }
            );

            return response
                .status(500)
                .json({
                    error:
                        "Internal server error."
                });
        }
    );

    return app;
}
function isMalformedJsonError(
    error: unknown
): boolean {
    return (
        typeof error === "object" &&
        error !== null &&
        Reflect.get(
            error,
            "type"
        ) === "entity.parse.failed"
    );
}
