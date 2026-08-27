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
    createInventoryApp,
    type ReadinessProbe
} from "../src/app.js";
import {
    InMemoryInventoryRepository
} from "../src/inMemoryInventoryRepository.js";

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

test(
    "returns the Inventory Service health response",
    async () => {
        const repository =
            createRepository();

        const app =
            createInventoryApp({
                stockReader:
                    repository,
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
                            "inventory-service"
                    }
                );
            }
        );
    }
);

test(
    "returns ready when RabbitMQ is available",
    async () => {
        const repository =
            createRepository();

        const app =
            createInventoryApp({
                stockReader:
                    repository,
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
                            "inventory-service",
                        dependencies: {
                            rabbitMq:
                                "Ready"
                        }
                    }
                );
            }
        );
    }
);

test(
    "returns not ready when RabbitMQ is unavailable",
    async () => {
        const repository =
            createRepository();

        const app =
            createInventoryApp({
                stockReader:
                    repository,
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
                            "inventory-service",
                        dependencies: {
                            rabbitMq:
                                "NotReady"
                        }
                    }
                );
            }
        );
    }
);

test(
    "returns the current inventory stock",
    async () => {
        const repository =
            createRepository();

        repository.reserve([
            {
                productId:
                    "washing-machine-01",
                quantity: 2,
                unitPrice: 4999
            }
        ]);

        const app =
            createInventoryApp({
                stockReader:
                    repository,
                readinessProbe:
                    readyProbe
            });

        await withTestServer(
            app,
            async baseUrl => {
                const response =
                    await fetch(
                        `${baseUrl}/stock`
                    );

                assert.equal(
                    response.status,
                    200
                );

                const body =
                    await response.json() as {
                        stock:
                        Record<
                            string,
                            number
                        >;
                    };

                assert.deepEqual(
                    body,
                    {
                        stock: {
                            "washing-machine-01":
                                8,
                            "dishwasher-01":
                                5,
                            "dryer-01":
                                3
                        }
                    }
                );
            }
        );
    }
);

function createRepository():
    InMemoryInventoryRepository {
    return new InMemoryInventoryRepository({
        "washing-machine-01": 10,
        "dishwasher-01": 5,
        "dryer-01": 3
    });
}

async function withTestServer(
    app:
        ReturnType<
            typeof createInventoryApp
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