import express from "express";
import type {
    Express,
    NextFunction,
    Request,
    Response
} from "express";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    IDEMPOTENCY_KEY_HEADER,
    parseIdempotencyKey
} from "./idempotencyKey.js";
import {
    parseCreateOrderRequest
} from "./orderRequestValidator.js";
import {
    OrderService,
    type CreatedOrderResponse
} from "./orderService.js";
import type {
    OrderStore,
    StoredOrder,
    SubmitOrderOutcome
} from "./orderStore.js";
import {
    buildOrderTimeline
} from "./orderTimeline.js";

export interface ReadinessProbe {
    isReady():
        boolean |
        Promise<boolean>;
}

export interface OutboxDiagnosticsReader {
    getOutboxDiagnostics():
        Promise<Readonly<Record<string, unknown>>>;
}

export interface OrderAppDependencies {
    readonly orderService:
    OrderService;

    readonly orderStore:
    OrderStore;

    readonly logger:
    AppLogger;

    readonly rabbitMqReadinessProbe:
    ReadinessProbe;

    readonly databaseReadinessProbe:
    ReadinessProbe;

    /** Called after a new order was committed, to wake the outbox relay. */
    readonly onOrderAccepted?:
    () => void;

    readonly outboxDiagnosticsReader?:
    OutboxDiagnosticsReader;

    /** Directory of the order timeline page; omitted in API-only tests. */
    readonly publicDirectory?:
    string;
}

const DEFAULT_LIST_LIMIT =
    20;

const MAX_LIST_LIMIT =
    100;

// The timeline page loads only its own script and stylesheet.
const CONTENT_SECURITY_POLICY =
    "default-src 'none'; script-src 'self'; style-src 'self'; " +
    "connect-src 'self'; img-src 'self' data:; base-uri 'none'; " +
    "form-action 'none'; frame-ancestors 'none'";

export function createOrderApp(
    dependencies:
        OrderAppDependencies
): Express {
    const {
        orderService,
        orderStore,
        logger,
        rabbitMqReadinessProbe,
        databaseReadinessProbe,
        onOrderAccepted,
        outboxDiagnosticsReader,
        publicDirectory
    } = dependencies;

    const app = express();

    // Do not advertise the framework version to clients.
    app.disable(
        "x-powered-by"
    );

    app.use(
        (
            _request: Request,
            response: Response,
            next: NextFunction
        ) => {
            response.set({
                "Content-Security-Policy":
                    CONTENT_SECURITY_POLICY,
                "X-Content-Type-Options":
                    "nosniff",
                "Referrer-Policy":
                    "no-referrer"
            });

            next();
        }
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
        async (
            _request: Request,
            response: Response
        ) => {
            const [
                rabbitMqReady,
                databaseReady
            ] =
                await Promise.all([
                    getProbeReadiness(
                        rabbitMqReadinessProbe
                    ),
                    getProbeReadiness(
                        databaseReadinessProbe
                    )
                ]);

            const ready =
                rabbitMqReady &&
                databaseReady;

            return response
                .status(
                    ready
                        ? 200
                        : 503
                )
                .json({
                    status:
                        toReadinessStatus(
                            ready
                        ),
                    service:
                        "order-service",
                    dependencies: {
                        rabbitMq:
                            toReadinessStatus(
                                rabbitMqReady
                            ),
                        database:
                            toReadinessStatus(
                                databaseReady
                            )
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
            const idempotencyKey =
                parseIdempotencyKey(
                    request.header(
                        IDEMPOTENCY_KEY_HEADER
                    )
                );

            if (!idempotencyKey.success) {
                return response
                    .status(400)
                    .json({
                        error:
                            idempotencyKey.error
                    });
            }

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

            let outcome:
                SubmitOrderOutcome;

            try {
                outcome =
                    await orderStore.submitOrder({
                        idempotencyKey:
                            idempotencyKey.key,
                        requestFingerprint:
                            result.requestFingerprint,
                        customerId:
                            validationResult.data.customerId,
                        items:
                            validationResult.data.items,
                        totalAmount:
                            result.response.totalAmount,
                        correlationId:
                            result.response.correlationId,
                        event:
                            result.event
                    });
            } catch (error) {
                // Either nothing was committed, or the commit succeeded
                // and only its acknowledgement was lost. A retry with the
                // same key is correct in both cases.
                logger.error(
                    "Could not store the order",
                    error,
                    {
                        correlationId:
                            result.response.correlationId
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
                            "The order could not be stored right now. Retry " +
                            "with the same Idempotency-Key; it will not be " +
                            "created twice.",
                        correlationId:
                            result.response.correlationId
                    });
            }

            switch (outcome.kind) {
                case "Created":
                    onOrderAccepted?.();

                    logger.info(
                        "Created order",
                        {
                            orderId:
                                outcome.order.orderId,
                            eventId:
                                result.event.eventId,
                            customerId:
                                outcome.order.customerId,
                            correlationId:
                                outcome.order.correlationId,
                            totalAmount:
                                outcome.order.totalAmount,
                            itemCount:
                                outcome.order.items.length
                        }
                    );

                    return response
                        .status(201)
                        .set(
                            "x-correlation-id",
                            outcome.order.correlationId
                        )
                        .json(
                            toCreatedResponse(
                                outcome.order
                            )
                        );

                case "Replayed":
                    logger.info(
                        "Replayed order for a repeated Idempotency-Key",
                        {
                            orderId:
                                outcome.order.orderId,
                            correlationId:
                                outcome.order.correlationId
                        }
                    );

                    return response
                        .status(200)
                        .set({
                            "x-correlation-id":
                                outcome.order.correlationId,
                            "Idempotent-Replayed":
                                "true"
                        })
                        .json(
                            toCreatedResponse(
                                outcome.order
                            )
                        );

                case "KeyReused":
                    logger.warn(
                        "Rejected an Idempotency-Key reused for a different order",
                        {
                            correlationId:
                                result.response.correlationId
                        }
                    );

                    return response
                        .status(422)
                        .json({
                            error:
                                "This Idempotency-Key was already used for a " +
                                "different order. Use a new key for a new order."
                        });
            }
        }
    );

    app.get(
        "/orders",
        async (
            request: Request,
            response: Response,
            next: NextFunction
        ) => {
            const limit =
                parseLimit(
                    request.query.limit
                );

            if (limit === undefined) {
                return response
                    .status(400)
                    .json({
                        error:
                            `limit must be an integer from 1 to ${MAX_LIST_LIMIT}.`
                    });
            }

            try {
                const orders =
                    await orderStore.listRecentOrders(
                        limit
                    );

                return response.json({
                    orders
                });
            } catch (error) {
                next(error);
            }
        }
    );

    app.get(
        "/orders/:orderId",
        async (
            request: Request<{
                orderId: string;
            }>,
            response: Response,
            next: NextFunction
        ) => {
            try {
                const details =
                    await orderStore.getOrderDetails(
                        request.params.orderId
                    );

                if (!details) {
                    return response
                        .status(404)
                        .json({
                            error:
                                "Order not found."
                        });
                }

                return response.json({
                    ...buildOrderTimeline(
                        details
                    ),
                    publication:
                        details.publication
                });
            } catch (error) {
                next(error);
            }
        }
    );

    if (outboxDiagnosticsReader) {
        // Operational view of the outbox backlog. Contains counts and
        // timestamps only, never event payloads.
        app.get(
            "/diagnostics/outbox",
            async (
                _request: Request,
                response: Response,
                next: NextFunction
            ) => {
                try {
                    response.json(
                        await outboxDiagnosticsReader
                            .getOutboxDiagnostics()
                    );
                } catch (error) {
                    next(error);
                }
            }
        );
    }

    if (publicDirectory) {
        app.use(
            express.static(
                publicDirectory
            )
        );
    }

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

            // Details stay in the server logs; clients get a stable,
            // non-sensitive body.
            return response
                .status(503)
                .json({
                    error:
                        "The order data is temporarily unavailable."
                });
        }
    );

    return app;
}

function toCreatedResponse(
    order: StoredOrder
): CreatedOrderResponse {
    // The creation result, identical for the first answer and every
    // replay; the current workflow status is at GET /orders/{id}.
    return {
        orderId:
            order.orderId,
        status:
            "Created",
        totalAmount:
            order.totalAmount,
        correlationId:
            order.correlationId
    };
}

function parseLimit(
    value: unknown
): number | undefined {
    if (value === undefined) {
        return DEFAULT_LIST_LIMIT;
    }

    if (
        typeof value !== "string" ||
        !/^\d{1,3}$/.test(value)
    ) {
        return undefined;
    }

    const limit =
        Number(value);

    return limit >= 1 && limit <= MAX_LIST_LIMIT
        ? limit
        : undefined;
}

async function getProbeReadiness(
    probe: ReadinessProbe
): Promise<boolean> {
    try {
        return await probe.isReady();
    } catch {
        return false;
    }
}

function toReadinessStatus(
    ready: boolean
): "Ready" | "NotReady" {
    return ready
        ? "Ready"
        : "NotReady";
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
