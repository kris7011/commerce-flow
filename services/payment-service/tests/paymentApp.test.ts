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
import {
    createPaymentApp
} from "../src/app.js";

test(
    "returns the Payment Service health response",
    async () => {
        const app =
            createPaymentApp();

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
                            "payment-service"
                    }
                );
            }
        );
    }
);

async function withTestServer(
    app:
        ReturnType<
            typeof createPaymentApp
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