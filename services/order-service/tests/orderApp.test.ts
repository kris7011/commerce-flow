import assert from "node:assert/strict";
import {
    once
} from "node:events";
import type {
    Server
} from "node:http";
import type {
    AddressInfo
} from "node:net";
import test from "node:test";
import {
    fileURLToPath
} from "node:url";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import {
    createOrderApp,
    type OrderAppDependencies
} from "../src/app.js";
import {
    InMemoryOrderStore
} from "../src/inMemoryOrderStore.js";
import {
    OrderService
} from "../src/orderService.js";
import type {
    OrderStore
} from "../src/orderStore.js";

const fixedTime =
    "2026-08-02T12:00:00.000Z";

const ORDER_ID =
    "6f1c2a52-6a4b-4f7e-9a59-3c2b9f0d1a01";

const EVENT_ID =
    "0a8b6c1d-2e3f-4a5b-8c7d-9e0f1a2b3c4d";

const validOrder = {
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
};

const silentLogger:
    AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

interface LogEntry {
    readonly message: string;
    readonly context: LogContext | undefined;
}

class RecordingLogger
    implements AppLogger {
    readonly infos: LogEntry[] = [];
    readonly warnings: LogEntry[] = [];
    readonly errors: LogEntry[] = [];

    info(message: string, context?: LogContext): void {
        this.infos.push({
            message,
            context
        });
    }

    warn(message: string, context?: LogContext): void {
        this.warnings.push({
            message,
            context
        });
    }

    error(message: string, _error?: unknown, context?: LogContext): void {
        this.errors.push({
            message,
            context
        });
    }

    child(): AppLogger {
        return this;
    }
}

function sequentialIds(
    ...ids: string[]
): () => string {
    let index = 0;

    return () => {
        const id =
            ids[index];

        index += 1;

        if (!id) {
            throw new Error(
                "The test did not provide enough generated ids."
            );
        }

        return id;
    };
}

function createApp(
    overrides:
        Partial<OrderAppDependencies> = {}
) {
    return createOrderApp({
        orderService:
            new OrderService({
                generateId:
                    sequentialIds(
                        ORDER_ID,
                        EVENT_ID,
                        "unused-1",
                        "unused-2",
                        "unused-3",
                        "unused-4"
                    ),
                getCurrentTime:
                    () => fixedTime
            }),
        orderStore:
            new InMemoryOrderStore(
                () => fixedTime
            ),
        logger:
            silentLogger,
        rabbitMqReadinessProbe: {
            isReady: () => true
        },
        databaseReadinessProbe: {
            isReady: () => true
        },
        ...overrides
    });
}

async function withServer(
    app: ReturnType<typeof createOrderApp>,
    action: (baseUrl: string) => Promise<void>
): Promise<void> {
    const server: Server =
        app.listen(
            0,
            "127.0.0.1"
        );

    await once(
        server,
        "listening"
    );

    const {
        port
    } = server.address() as AddressInfo;

    try {
        await action(
            `http://127.0.0.1:${port}`
        );
    } finally {
        server.close();
        await once(
            server,
            "close"
        );
    }
}

function postOrder(
    baseUrl: string,
    body: unknown,
    // A supplied correlation id keeps the generated ids for the order
    // and its event.
    headers: Record<string, string> = {
        "idempotency-key": "key-001",
        "x-correlation-id": "correlation-001"
    }
): Promise<Response> {
    return fetch(
        `${baseUrl}/orders`,
        {
            method: "POST",
            headers: {
                "content-type": "application/json",
                ...headers
            },
            body:
                typeof body === "string"
                    ? body
                    : JSON.stringify(body)
        }
    );
}

test(
    "returns the Order Service health response",
    async () => {
        await withServer(
            createApp(),
            async baseUrl => {
                const response =
                    await fetch(`${baseUrl}/health`);

                assert.equal(response.status, 200);
                assert.deepEqual(
                    await response.json(),
                    {
                        status: "Healthy",
                        service: "order-service"
                    }
                );
            }
        );
    }
);

for (const [rabbitMq, database, expectedStatus] of [
    [true, true, 200],
    [false, true, 503],
    [true, false, 503]
] as const) {
    test(
        `readiness is ${expectedStatus} with RabbitMQ ${rabbitMq ? "up" : "down"} ` +
        `and PostgreSQL ${database ? "up" : "down"}`,
        async () => {
            await withServer(
                createApp({
                    rabbitMqReadinessProbe: {
                        isReady: () => rabbitMq
                    },
                    databaseReadinessProbe: {
                        isReady: async () => {
                            if (!database) {
                                throw new Error("connection refused");
                            }

                            return true;
                        }
                    }
                }),
                async baseUrl => {
                    const response =
                        await fetch(`${baseUrl}/ready`);

                    assert.equal(response.status, expectedStatus);
                    assert.deepEqual(
                        await response.json(),
                        {
                            status: expectedStatus === 200 ? "Ready" : "NotReady",
                            service: "order-service",
                            dependencies: {
                                rabbitMq: rabbitMq ? "Ready" : "NotReady",
                                database: database ? "Ready" : "NotReady"
                            }
                        }
                    );
                }
            );
        }
    );
}

test(
    "creates an order, stores its OrderCreated event and wakes the outbox relay",
    async () => {
        const store =
            new InMemoryOrderStore(
                () => fixedTime
            );

        let wakeUps = 0;

        await withServer(
            createApp({
                orderStore: store,
                onOrderAccepted: () => {
                    wakeUps += 1;
                }
            }),
            async baseUrl => {
                const response =
                    await postOrder(
                        baseUrl,
                        validOrder,
                        {
                            "idempotency-key": "key-001",
                            "x-correlation-id": "correlation-001"
                        }
                    );

                assert.equal(response.status, 201);
                assert.equal(response.headers.get("x-correlation-id"), "correlation-001");
                assert.equal(response.headers.get("idempotent-replayed"), null);
                assert.deepEqual(
                    await response.json(),
                    {
                        orderId: ORDER_ID,
                        status: "Created",
                        totalAmount: 9999.9,
                        correlationId: "correlation-001"
                    }
                );
            }
        );

        assert.equal(wakeUps, 1);

        const details =
            await store.getOrderDetails(ORDER_ID);

        assert.equal(details?.order.status, "Created");
        assert.equal(details?.publication?.eventId, EVENT_ID);
        assert.equal(details?.publication?.publishedAt, null);
        assert.deepEqual(
            details?.timeline.map(row => row.kind),
            ["Accepted"]
        );
    }
);

test(
    "a retry with the same key and request returns the original order without creating another",
    async () => {
        const store =
            new InMemoryOrderStore(
                () => fixedTime
            );

        let wakeUps = 0;

        await withServer(
            createApp({
                orderStore: store,
                onOrderAccepted: () => {
                    wakeUps += 1;
                }
            }),
            async baseUrl => {
                const first =
                    await postOrder(baseUrl, validOrder);
                const firstBody =
                    await first.json();

                const retry =
                    await postOrder(
                        baseUrl,
                        validOrder,
                        {
                            "idempotency-key": "key-001",
                            // A retry may carry another correlation id;
                            // the original order's id is returned.
                            "x-correlation-id": "correlation-retry"
                        }
                    );

                assert.equal(retry.status, 200);
                assert.equal(retry.headers.get("idempotent-replayed"), "true");
                assert.deepEqual(await retry.json(), firstBody);
                assert.equal(retry.headers.get("x-correlation-id"), firstBody.correlationId);
            }
        );

        assert.equal(wakeUps, 1);
        assert.equal((await store.listRecentOrders(10)).length, 1);
        assert.deepEqual(
            (await store.getOrderDetails(ORDER_ID))?.timeline.map(row => row.kind),
            ["Accepted", "Replayed"]
        );
    }
);

test(
    "the same key with a different request is rejected with 422",
    async () => {
        const store =
            new InMemoryOrderStore();

        await withServer(
            createApp({
                orderStore: store
            }),
            async baseUrl => {
                await postOrder(baseUrl, validOrder);

                const response =
                    await postOrder(
                        baseUrl,
                        {
                            ...validOrder,
                            items: [
                                {
                                    ...validOrder.items[0],
                                    quantity: 3
                                }
                            ]
                        }
                    );

                assert.equal(response.status, 422);
                assert.match(
                    (await response.json()).error,
                    /already used for a different order/
                );
            }
        );

        assert.equal((await store.listRecentOrders(10)).length, 1);
    }
);

for (const [description, headers, pattern] of [
    ["missing", {}, /header is required/],
    ["empty", { "idempotency-key": "" }, /header is required/],
    ["too long", { "idempotency-key": "k".repeat(256) }, /1 to 255 visible ASCII/],
    ["space-containing", { "idempotency-key": "key 001" }, /visible ASCII/]
] as const) {
    test(
        `rejects a ${description} Idempotency-Key before storing anything`,
        async () => {
            const store =
                new InMemoryOrderStore();

            await withServer(
                createApp({
                    orderStore: store
                }),
                async baseUrl => {
                    const response =
                        await postOrder(
                            baseUrl,
                            validOrder,
                            headers as Record<string, string>
                        );

                    assert.equal(response.status, 400);
                    assert.match((await response.json()).error, pattern);
                }
            );

            assert.equal((await store.listRecentOrders(10)).length, 0);
        }
    );
}

test(
    "rejects an invalid order and logs the validation issues",
    async () => {
        const logger =
            new RecordingLogger();

        const store =
            new InMemoryOrderStore();

        await withServer(
            createApp({
                logger,
                orderStore: store
            }),
            async baseUrl => {
                const response =
                    await postOrder(
                        baseUrl,
                        {
                            customerId: "customer-001",
                            items: []
                        }
                    );

                assert.equal(response.status, 400);
            }
        );

        assert.equal(logger.warnings[0]?.message, "Rejected invalid order request");
        assert.ok(Array.isArray(logger.warnings[0]?.context?.validationIssues));
        assert.equal((await store.listRecentOrders(10)).length, 0);
    }
);

test(
    "logs a created order with structured context",
    async () => {
        const logger =
            new RecordingLogger();

        await withServer(
            createApp({
                logger
            }),
            async baseUrl => {
                await postOrder(
                    baseUrl,
                    validOrder,
                    {
                        "idempotency-key": "key-001",
                        "x-correlation-id": "correlation-001"
                    }
                );
            }
        );

        const created =
            logger.infos.find(entry => entry.message === "Created order");

        assert.deepEqual(
            created?.context,
            {
                orderId: ORDER_ID,
                eventId: EVENT_ID,
                customerId: "customer-001",
                correlationId: "correlation-001",
                totalAmount: 9999.9,
                itemCount: 1
            }
        );
    }
);

test(
    "answers 503 with Retry-After when the order cannot be stored",
    async () => {
        const failingStore:
            OrderStore = {
            submitOrder: () => Promise.reject(new Error("connection terminated")),
            recordWorkflowEvent: () => Promise.reject(new Error("unused")),
            getOrderDetails: () => Promise.reject(new Error("unused")),
            listRecentOrders: () => Promise.reject(new Error("unused"))
        };

        const logger =
            new RecordingLogger();

        await withServer(
            createApp({
                orderStore: failingStore,
                logger
            }),
            async baseUrl => {
                const response =
                    await postOrder(baseUrl, validOrder);

                assert.equal(response.status, 503);
                assert.equal(response.headers.get("retry-after"), "5");
                assert.match(
                    (await response.json()).error,
                    /Retry with the same Idempotency-Key/
                );
            }
        );

        assert.equal(logger.errors[0]?.message, "Could not store the order");
    }
);

test(
    "answers malformed JSON with a 400 JSON error",
    async () => {
        await withServer(
            createApp(),
            async baseUrl => {
                const response =
                    await postOrder(baseUrl, "{not json");

                assert.equal(response.status, 400);
                assert.deepEqual(
                    await response.json(),
                    {
                        error: "The request body is not valid JSON."
                    }
                );
            }
        );
    }
);

test(
    "lists recent orders and validates the limit",
    async () => {
        await withServer(
            createApp(),
            async baseUrl => {
                await postOrder(baseUrl, validOrder);

                const list =
                    await fetch(`${baseUrl}/orders?limit=5`);

                assert.equal(list.status, 200);
                assert.deepEqual(
                    (await list.json()).orders.map(
                        (order: { orderId: string }) => order.orderId
                    ),
                    [ORDER_ID]
                );

                for (const limit of ["0", "101", "abc", "1.5"]) {
                    const invalid =
                        await fetch(`${baseUrl}/orders?limit=${limit}`);

                    assert.equal(invalid.status, 400, `limit=${limit}`);
                }
            }
        );
    }
);

test(
    "returns the order with its timeline, and 404 for an unknown order",
    async () => {
        const store =
            new InMemoryOrderStore(
                () => fixedTime
            );

        await withServer(
            createApp({
                orderStore: store
            }),
            async baseUrl => {
                await postOrder(baseUrl, validOrder);

                await store.recordWorkflowEvent({
                    eventId: "payment-event-1",
                    eventType: "PaymentAuthorized",
                    orderId: ORDER_ID,
                    correlationId: "c",
                    occurredAt: "2026-08-02T12:00:01.000Z",
                    data: {
                        orderId: ORDER_ID,
                        paymentId: "payment-1",
                        amount: 9999.9,
                        items: validOrder.items
                    }
                });

                const response =
                    await fetch(`${baseUrl}/orders/${ORDER_ID}`);

                assert.equal(response.status, 200);

                const view =
                    await response.json();

                assert.equal(view.order.status, "PaymentAuthorized");
                assert.equal(view.outcome, "InProgress");
                assert.deepEqual(
                    view.entries.map((entry: { kind: string }) => entry.kind),
                    ["Accepted", "PublicationPending", "Event"]
                );
                assert.equal(view.entries[2].simulated, true);
                assert.equal(view.publication.eventId, EVENT_ID);

                const missing =
                    await fetch(`${baseUrl}/orders/00000000-0000-4000-8000-000000000000`);

                assert.equal(missing.status, 404);
            }
        );
    }
);

test(
    "serves the timeline page with a strict Content-Security-Policy",
    async () => {
        await withServer(
            createApp({
                publicDirectory:
                    fileURLToPath(
                        new URL(
                            "../public/",
                            import.meta.url
                        )
                    )
            }),
            async baseUrl => {
                const page =
                    await fetch(`${baseUrl}/`);

                assert.equal(page.status, 200);
                assert.match(page.headers.get("content-type") ?? "", /text\/html/);
                assert.match(
                    page.headers.get("content-security-policy") ?? "",
                    /default-src 'none'; script-src 'self'/
                );

                const html =
                    await page.text();

                assert.doesNotMatch(html, /(src|href)="(https?:)?\/\//);
                assert.doesNotMatch(html, /<script>/);

                const script =
                    await fetch(`${baseUrl}/timeline.js`);

                assert.equal(script.status, 200);
                assert.doesNotMatch(await script.text(), /innerHTML|insertAdjacentHTML|eval\(/);
            }
        );
    }
);
