import assert from "node:assert/strict";
import {
    once
} from "node:events";
import type {
    AddressInfo
} from "node:net";
import test from "node:test";
import {
    COMMERCE_TOPOLOGY,
    QUEUES,
    ROUTING_KEYS,
    type QueueBinding
} from "@commerce-flow/contracts";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    RabbitMqClient
} from "@commerce-flow/messaging";
import {
    createTestVirtualHost,
    type TestVirtualHost
} from "@commerce-flow/testing";
import * as amqp from "amqplib";
import {
    createOrderApp
} from "../src/app.js";
import {
    OrderService
} from "../src/orderService.js";

/*
 * The Order Service must never answer 201 for an order whose
 * OrderCreated event the broker discarded. Runs against a fresh
 * RabbitMQ virtual host, i.e. before any consumer has started.
 */

const silentLogger:
    AppLogger = {
    info: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    child: () => silentLogger
};

async function postOrder(
    virtualHost: TestVirtualHost,
    topology: readonly QueueBinding[]
): Promise<Response> {
    const rabbitMq =
        new RabbitMqClient(
            virtualHost.url,
            "commerce.events",
            {
                maxConnectionRetries: 3,
                retryDelayInMs: 50,
                publishTimeoutInMs: 3_000,
                topology
            },
            {
                logger:
                    silentLogger
            }
        );

    const app =
        createOrderApp({
            orderService:
                new OrderService(),
            orderCreatedPublisher: {
                publishOrderCreated:
                    event =>
                        rabbitMq.publish(
                            ROUTING_KEYS.OrderCreated,
                            event
                        )
            },
            logger:
                silentLogger,
            readinessProbe: {
                isReady: () => true
            }
        });

    const server =
        app.listen(
            0,
            "127.0.0.1"
        );

    await once(
        server,
        "listening"
    );

    try {
        const {
            port
        } = server.address() as AddressInfo;

        return await fetch(
            `http://127.0.0.1:${port}/orders`,
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
                        items: [
                            {
                                productId:
                                    "dryer-01",
                                quantity: 1,
                                unitPrice: 2999
                            }
                        ]
                    })
            }
        );
    } finally {
        server.close();
        await rabbitMq.close();
    }
}

async function countMessages(
    virtualHost: TestVirtualHost,
    queueName: string
): Promise<number> {
    const connection =
        await amqp.connect(
            virtualHost.url
        );

    try {
        const channel =
            await connection.createChannel();

        return (
            await channel.checkQueue(
                queueName
            )
        ).messageCount;
    } finally {
        await connection.close();
    }
}

test(
    "answers 503 when OrderCreated cannot be routed to any queue",
    {
        timeout: 20_000
    },
    async () => {
        const virtualHost =
            await createTestVirtualHost();

        try {
            const response =
                await postOrder(
                    virtualHost,
                    []
                );

            assert.equal(
                response.status,
                503
            );

            assert.equal(
                response.headers.get(
                    "retry-after"
                ),
                "5"
            );
        } finally {
            await virtualHost.delete();
        }
    }
);

test(
    "accepts the order when the provisioned topology holds the event for the Payment Service",
    {
        timeout: 20_000
    },
    async () => {
        const virtualHost =
            await createTestVirtualHost();

        try {
            const response =
                await postOrder(
                    virtualHost,
                    COMMERCE_TOPOLOGY
                );

            assert.equal(
                response.status,
                201
            );

            assert.equal(
                await countMessages(
                    virtualHost,
                    QUEUES.paymentOrderCreated.queueName
                ),
                1,
                "OrderCreated must wait in the Payment Service queue."
            );
        } finally {
            await virtualHost.delete();
        }
    }
);
