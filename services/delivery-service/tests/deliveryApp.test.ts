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
    createDeliveryApp
} from "../src/app.js";

test(
    "returns the Delivery Service health response",
    async () => {
        const app =
            createDeliveryApp();

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
                            "delivery-service"
                    }
                );
            }
        );
    }
);

async function withTestServer(
    app:
        ReturnType<
            typeof createDeliveryApp
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