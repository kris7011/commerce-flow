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

            const successfulOrder =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    successfulCorrelationId,
                    [
                        {
                            productId: "washing-machine-01",
                            quantity: 1,
                            unitPrice: 749.95
                        }
                    ]
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
