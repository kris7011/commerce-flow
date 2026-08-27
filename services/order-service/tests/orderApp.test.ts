import assert from "node:assert/strict";
import type {
    Server
} from "node:http";
import type {
    AddressInfo
} from "node:net";
import {
    once
} from "node:events";
import test from "node:test";
import type {
    OrderCreatedEvent
} from "@commerce-flow/contracts";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import {
    createOrderApp,
    type OrderCreatedPublisher,
    type ReadinessProbe
} from "../src/app.js";
import {
    OrderService
} from "../src/orderService.js";

const fixedTime =
    "2026-08-02T12:00:00.000Z";

const readyProbe:
    ReadinessProbe = {
    isReady(): boolean {
        return true;
    }
};

const notReadyProbe:
    ReadinessProbe = {
    isReady(): boolean {
        return false;
    }
};

const silentLogger:
    AppLogger = {
    info(): void {
        // Logging is intentionally disabled
        // during these HTTP tests.
    },

    warn(): void {
        // Logging is intentionally disabled
        // during these HTTP tests.
    },

    error(): void {
        // Logging is intentionally disabled
        // during these HTTP tests.
    },

    child(): AppLogger {
        return silentLogger;
    }
};

test(
    "returns the Order Service health response",
    async () => {
        const publisher =
            new RecordingOrderCreatedPublisher();

        const app =
            createOrderApp({
                orderService:
                    createOrderService([]),
                orderCreatedPublisher:
                    publisher,
                logger:
                    silentLogger,
                readinessProbe:
                    readyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/health`
                    );

                assert.equal(
                    response.status,
                    200
                );

                const body =
                    await response.json() as {
                        status: string;
                        service: string;
                    };

                assert.deepEqual(
                    body,
                    {
                        status:
                            "Healthy",
                        service:
                            "order-service"
                    }
                );

                assert.equal(
                    publisher.events.length,
                    0
                );
            }
        );
    }
);

test(
    "returns ready when RabbitMQ is available",
    async () => {
        const publisher =
            new RecordingOrderCreatedPublisher();

        const app =
            createOrderApp({
                orderService:
                    createOrderService([]),
                orderCreatedPublisher:
                    publisher,
                logger:
                    silentLogger,
                readinessProbe:
                    readyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/ready`
                    );

                assert.equal(
                    response.status,
                    200
                );

                const body =
                    await response.json();

                assert.deepEqual(
                    body,
                    {
                        status:
                            "Ready",
                        service:
                            "order-service",
                        dependencies: {
                            rabbitMq:
                                "Ready"
                        }
                    }
                );

                assert.equal(
                    publisher.events.length,
                    0
                );
            }
        );
    }
);

test(
    "returns not ready when RabbitMQ is unavailable",
    async () => {
        const publisher =
            new RecordingOrderCreatedPublisher();

        const app =
            createOrderApp({
                orderService:
                    createOrderService([]),
                orderCreatedPublisher:
                    publisher,
                logger:
                    silentLogger,
                readinessProbe:
                    notReadyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/ready`
                    );

                assert.equal(
                    response.status,
                    503
                );

                const body =
                    await response.json();

                assert.deepEqual(
                    body,
                    {
                        status:
                            "NotReady",
                        service:
                            "order-service",
                        dependencies: {
                            rabbitMq:
                                "NotReady"
                        }
                    }
                );

                assert.equal(
                    publisher.events.length,
                    0
                );
            }
        );
    }
);

test(
    "rejects an invalid order request without publishing an event",
    async () => {
        const publisher =
            new RecordingOrderCreatedPublisher();

        const app =
            createOrderApp({
                orderService:
                    createOrderService([]),
                orderCreatedPublisher:
                    publisher,
                logger:
                    silentLogger,
                readinessProbe:
                    readyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/orders`,
                        {
                            method: "POST",
                            headers: {
                                "content-type":
                                    "application/json"
                            },
                            body:
                                JSON.stringify({
                                    customerId:
                                        "customer-001",
                                    items: []
                                })
                        }
                    );

                assert.equal(
                    response.status,
                    400
                );

                const body =
                    await response.json() as {
                        error: string;
                    };

                assert.match(
                    body.error,
                    /Invalid order request/
                );

                assert.equal(
                    publisher.events.length,
                    0
                );
            }
        );
    }
);

test(
    "creates an order and publishes OrderCreated through the injected publisher",
    async () => {
        const publisher =
            new RecordingOrderCreatedPublisher();

        const app =
            createOrderApp({
                orderService:
                    createOrderService([
                        "order-001",
                        "event-001"
                    ]),
                orderCreatedPublisher:
                    publisher,
                logger:
                    silentLogger,
                readinessProbe:
                    readyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/orders`,
                        {
                            method: "POST",
                            headers: {
                                "content-type":
                                    "application/json",
                                "x-correlation-id":
                                    "correlation-001"
                            },
                            body:
                                JSON.stringify({
                                    customerId:
                                        "customer-001",
                                    items: [
                                        {
                                            productId:
                                                "washing-machine-01",
                                            quantity:
                                                2,
                                            unitPrice:
                                                4999.95
                                        }
                                    ]
                                })
                        }
                    );

                assert.equal(
                    response.status,
                    201
                );

                const body =
                    await response.json();

                assert.deepEqual(
                    body,
                    {
                        orderId:
                            "order-001",
                        status:
                            "Created",
                        totalAmount:
                            9999.9,
                        correlationId:
                            "correlation-001"
                    }
                );

                assert.equal(
                    publisher.events.length,
                    1
                );

                assert.deepEqual(
                    publisher.events[0],
                    {
                        eventId:
                            "event-001",
                        eventType:
                            "OrderCreated",
                        occurredAt:
                            fixedTime,
                        correlationId:
                            "correlation-001",
                        data: {
                            orderId:
                                "order-001",
                            customerId:
                                "customer-001",
                            items: [
                                {
                                    productId:
                                        "washing-machine-01",
                                    quantity:
                                        2,
                                    unitPrice:
                                        4999.95
                                }
                            ],
                            totalAmount:
                                9999.9
                        }
                    }
                );
            }
        );
    }
);

test(
    "logs order requests with structured context",
    async () => {
        const publisher =
            new RecordingOrderCreatedPublisher();

        const logger =
            new RecordingOrderAppLogger();

        const app =
            createOrderApp({
                orderService:
                    createOrderService([
                        "order-001",
                        "event-001"
                    ]),
                orderCreatedPublisher:
                    publisher,
                logger,
                readinessProbe:
                    readyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const invalidResponse =
                    await fetch(
                        `${baseUrl}/orders`,
                        {
                            method: "POST",
                            headers: {
                                "content-type":
                                    "application/json"
                            },
                            body:
                                JSON.stringify({
                                    customerId:
                                        "customer-001",
                                    items: []
                                })
                        }
                    );

                assert.equal(
                    invalidResponse.status,
                    400
                );

                assert.equal(
                    logger.warningLogs.length,
                    1
                );

                assert.equal(
                    logger.warningLogs[0]
                        ?.message,
                    "Rejected invalid order request"
                );

                const validationIssues =
                    logger.warningLogs[0]
                        ?.context
                        ?.validationIssues;

                assert.ok(
                    Array.isArray(
                        validationIssues
                    )
                );

                const validResponse =
                    await fetch(
                        `${baseUrl}/orders`,
                        {
                            method: "POST",
                            headers: {
                                "content-type":
                                    "application/json",
                                "x-correlation-id":
                                    "correlation-001"
                            },
                            body:
                                JSON.stringify({
                                    customerId:
                                        "customer-001",
                                    items: [
                                        {
                                            productId:
                                                "washing-machine-01",
                                            quantity:
                                                2,
                                            unitPrice:
                                                4999.95
                                        }
                                    ]
                                })
                        }
                    );

                assert.equal(
                    validResponse.status,
                    201
                );

                assert.deepEqual(
                    logger.infoLogs,
                    [
                        {
                            message:
                                "Created order",
                            context: {
                                orderId:
                                    "order-001",
                                eventId:
                                    "event-001",
                                customerId:
                                    "customer-001",
                                correlationId:
                                    "correlation-001",
                                totalAmount:
                                    9999.9,
                                itemCount:
                                    1
                            }
                        }
                    ]
                );
            }
        );
    }
);

class RecordingOrderAppLogger
    implements AppLogger {
    readonly infoLogs: {
        message: string;
        context?: LogContext;
    }[] = [];

    readonly warningLogs: {
        message: string;
        context?: LogContext;
    }[] = [];

    readonly errorLogs: {
        message: string;
        error?: unknown;
        context?: LogContext;
    }[] = [];

    info(
        message: string,
        context?: LogContext
    ): void {
        this.infoLogs.push({
            message,
            context
        });
    }

    warn(
        message: string,
        context?: LogContext
    ): void {
        this.warningLogs.push({
            message,
            context
        });
    }

    error(
        message: string,
        error?: unknown,
        context?: LogContext
    ): void {
        this.errorLogs.push({
            message,
            error,
            context
        });
    }

    child(
        _context: LogContext
    ): AppLogger {
        return this;
    }
}

class RecordingOrderCreatedPublisher
    implements OrderCreatedPublisher {
    readonly events:
        OrderCreatedEvent[] = [];

    async publishOrderCreated(
        event: OrderCreatedEvent
    ): Promise<void> {
        this.events.push(
            event
        );
    }
}

function createOrderService(
    generatedIds: string[]
): OrderService {
    let currentIndex = 0;

    return new OrderService({
        generateId: () => {
            const generatedId =
                generatedIds[
                currentIndex
                ];

            if (!generatedId) {
                throw new Error(
                    "The test did not provide " +
                    "enough generated IDs."
                );
            }

            currentIndex += 1;

            return generatedId;
        },

        getCurrentTime: () =>
            fixedTime
    });
}

async function withTestServer(
    app:
        ReturnType<
            typeof createOrderApp
        >,
    action:
        (
            baseUrl: string
        ) => Promise<void>
): Promise<void> {
    const server =
        app.listen(
            0,
            "127.0.0.1"
        );

    await once(
        server,
        "listening"
    );

    const address =
        server.address();

    if (
        address === null ||
        typeof address === "string"
    ) {
        await closeServer(
            server
        );

        throw new Error(
            "The test server did not " +
            "receive a TCP address."
        );
    }

    const tcpAddress:
        AddressInfo = address;

    const baseUrl =
        `http://127.0.0.1:` +
        `${tcpAddress.port}`;

    try {
        await action(
            baseUrl
        );
    } finally {
        await closeServer(
            server
        );
    }
}

function closeServer(
    server: Server
): Promise<void> {
    return new Promise(
        (
            resolve,
            reject
        ) => {
            server.close(
                error => {
                    if (error) {
                        reject(
                            error
                        );

                        return;
                    }

                    resolve();
                }
            );
        }
    );
}