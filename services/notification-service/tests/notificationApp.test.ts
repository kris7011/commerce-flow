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
import type {
    DeliveryBookedEvent
} from "@commerce-flow/contracts";
import {
    createNotificationApp
} from "../src/app.js";
import {
    NotificationService
} from "../src/notificationService.js";

const fixedTime =
    "2026-08-06T08:00:00.000Z";

test(
    "returns the Notification Service health response",
    async () => {
        const notificationService =
            createNotificationService([]);

        const app =
            createNotificationApp({
                notificationReader:
                    notificationService
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
                        status: "Healthy",
                        service:
                            "notification-service"
                    }
                );
            }
        );
    }
);

test(
    "returns the currently stored notifications",
    async () => {
        const notificationService =
            createNotificationService([
                "notification-001"
            ]);

        notificationService
            .createNotification(
                createDeliveryBookedEvent()
            );

        const app =
            createNotificationApp({
                notificationReader:
                    notificationService
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/notifications`
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
                        notifications: [
                            {
                                notificationId:
                                    "notification-001",
                                orderId:
                                    "order-001",
                                type:
                                    "DeliveryBooked",
                                message:
                                    "Delivery has been " +
                                    "booked with " +
                                    "DefaultCarrier. " +
                                    "Estimated delivery " +
                                    "date: 2026-08-09.",
                                correlationId:
                                    "correlation-001",
                                createdAt:
                                    fixedTime
                            }
                        ]
                    }
                );
            }
        );
    }
);

function createNotificationService(
    generatedIds: string[]
): NotificationService {
    let currentIndex = 0;

    return new NotificationService({
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

function createDeliveryBookedEvent():
    DeliveryBookedEvent {
    return {
        eventId:
            "delivery-event-001",
        eventType:
            "DeliveryBooked",
        occurredAt:
            "2026-08-06T07:59:00.000Z",
        correlationId:
            "correlation-001",
        data: {
            orderId:
                "order-001",
            deliveryId:
                "delivery-001",
            carrier:
                "DefaultCarrier",
            estimatedDeliveryDate:
                "2026-08-09"
        }
    };
}

async function withTestServer(
    app:
        ReturnType<
            typeof createNotificationApp
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
        await closeServer(server);

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
        await action(baseUrl);
    } finally {
        await closeServer(server);
    }
}

function closeServer(
    server: Server
): Promise<void> {
    return new Promise(
        (resolve, reject) => {
            server.close(error => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve();
            });
        }
    );
}