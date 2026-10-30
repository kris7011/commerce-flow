import assert from "node:assert/strict";
import {
    randomUUID
} from "node:crypto";
import test from "node:test";
import {
    createE2eEnvironment
} from "./support/environment.js";
import {
    ServiceCluster,
    createOrder,
    getJson,
    getStock,
    waitForNotification,
    waitForOrderTimeline,
    waitUntil
} from "./support/serviceCluster.js";

test(
    "processes successful and failed orders through the complete workflow",
    {
        timeout: 90_000
    },
    async () => {
        const environment =
            await createE2eEnvironment();

        const cluster =
            new ServiceCluster(
                environment,
                3100
            );

        try {
            await cluster.startAll();

            const inventoryUrl =
                cluster.url(
                    "inventory-service"
                );

            const notificationUrl =
                cluster.url(
                    "notification-service"
                );

            // A fresh database starts with the seeded demo catalogue.
            assert.deepEqual(
                await getStock(
                    inventoryUrl
                ),
                {
                    "dishwasher-01": 5,
                    "dryer-01": 3,
                    "washing-machine-01": 10
                }
            );

            const successfulCorrelationId =
                `e2e-success-${randomUUID()}`;

            const successfulKey =
                randomUUID();

            const successfulItems = [
                {
                    productId: "washing-machine-01",
                    quantity: 1,
                    unitPrice: 749.95
                }
            ];

            const successfulOrder =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    successfulCorrelationId,
                    successfulItems,
                    successfulKey
                );

            // A client retry (for example after a timeout) with the same
            // key returns the same order instead of creating a second one.
            const retriedOrder =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    // Same correlation id: the helper derives the
                    // customer from it, so the request is identical.
                    successfulCorrelationId,
                    successfulItems,
                    successfulKey,
                    200
                );

            assert.deepEqual(
                retriedOrder,
                successfulOrder
            );

            assert.equal(
                successfulOrder.totalAmount,
                749.95
            );

            const deliveryNotification =
                await waitForNotification(
                    notificationUrl,
                    successfulCorrelationId,
                    "DeliveryBooked",
                    20_000
                );

            assert.equal(
                deliveryNotification.orderId,
                successfulOrder.orderId
            );

            assert.match(
                deliveryNotification.message,
                /Delivery has been booked/
            );

            const successfulTimeline =
                await waitForOrderTimeline(
                    cluster.url(
                        "order-service"
                    ),
                    successfulOrder.orderId,
                    "DeliveryBooked",
                    20_000
                );

            assert.equal(
                successfulTimeline.outcome,
                "Completed"
            );

            assert.deepEqual(
                successfulTimeline.entries
                    .filter(entry => entry.kind === "Event")
                    .map(entry => [entry.eventType, entry.service, entry.simulated])
                    .sort(),
                [
                    ["DeliveryBooked", "delivery-service", true],
                    ["InventoryReserved", "inventory-service", false],
                    ["PaymentAuthorized", "payment-service", true]
                ]
            );

            assert.deepEqual(
                successfulTimeline.entries
                    .filter(entry => entry.kind !== "Event")
                    .map(entry => entry.kind)
                    .sort(),
                [
                    "Accepted",
                    "Published",
                    "Replayed"
                ]
            );

            const failedCorrelationId =
                `e2e-failure-${randomUUID()}`;

            const failedOrder =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    failedCorrelationId,
                    [
                        {
                            productId: "dryer-01",
                            quantity: 103,
                            unitPrice: 599.95
                        }
                    ]
                );

            const failureNotification =
                await waitForNotification(
                    notificationUrl,
                    failedCorrelationId,
                    "InventoryReservationFailed",
                    20_000
                );

            assert.equal(
                failureNotification.orderId,
                failedOrder.orderId
            );

            assert.match(
                failureNotification.message,
                /Inventory reservation failed/
            );

            const failedTimeline =
                await waitForOrderTimeline(
                    cluster.url(
                        "order-service"
                    ),
                    failedOrder.orderId,
                    "InventoryReservationFailed",
                    20_000
                );

            assert.equal(
                failedTimeline.outcome,
                "Failed"
            );

            assert.deepEqual(
                await getStock(
                    inventoryUrl
                ),
                {
                    "dishwasher-01": 5,
                    "dryer-01": 3,
                    "washing-machine-01": 9
                }
            );

            // The relay marks an event as published after the broker
            // confirm, which can be a moment after consumers have
            // already processed it.
            await waitUntil(
                async () =>
                    (await getJson<{
                        pendingCount: number;
                    }>(
                        `${inventoryUrl}/diagnostics/outbox`
                    )).pendingCount === 0,
                10_000,
                "both inventory results to be marked as published"
            );
        } catch (error) {
            throw new Error(
                "CommerceFlow end-to-end test failed.\n\n" +
                cluster.formatLogs(),
                {
                    cause: error
                }
            );
        } finally {
            await cluster.stopAll();
            await environment.dispose();
        }
    }
);

test(
    "completes orders accepted while every downstream service was still offline",
    {
        timeout: 90_000
    },
    async () => {
        const environment =
            await createE2eEnvironment();

        const cluster =
            new ServiceCluster(
                environment,
                3150
            );

        try {
            // Only the publisher runs; the virtual host is brand new,
            // so no consumer has ever declared its queue.
            await cluster.startAll([
                "order-service"
            ]);

            const correlationId =
                `e2e-publisher-first-${randomUUID()}`;

            const order =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    correlationId,
                    [
                        {
                            productId: "dishwasher-01",
                            quantity: 1,
                            unitPrice: 3499
                        }
                    ]
                );

            await cluster.startAll([
                "notification-service",
                "delivery-service",
                "inventory-service",
                "payment-service"
            ]);

            const notification =
                await waitForNotification(
                    cluster.url(
                        "notification-service"
                    ),
                    correlationId,
                    "DeliveryBooked",
                    30_000
                );

            assert.equal(
                notification.orderId,
                order.orderId
            );
        } catch (error) {
            throw new Error(
                "Publisher-first end-to-end test failed.\n\n" +
                cluster.formatLogs(),
                {
                    cause: error
                }
            );
        } finally {
            await cluster.stopAll();
            await environment.dispose();
        }
    }
);
