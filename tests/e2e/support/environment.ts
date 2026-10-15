import {
    randomBytes
} from "node:crypto";
import {
    createTestDatabase,
    type TestDatabase
} from "@commerce-flow/testing";

/*
 * Every end-to-end test file runs against its own RabbitMQ virtual host
 * and its own PostgreSQL database. Durable queues and stock left behind
 * by earlier (or failed) runs can therefore never leak into a test, and
 * the development database is never modified.
 */

const managementUrl =
    new URL(
        process.env.RABBITMQ_MANAGEMENT_URL ??
        "http://guest:guest@localhost:15672"
    );

const brokerUrl =
    new URL(
        process.env.RABBITMQ_URL ??
        "amqp://guest:guest@localhost:5672"
    );

export interface E2eEnvironment {
    readonly virtualHost: string;
    readonly rabbitMqUrl: string;
    readonly database: TestDatabase;

    dispose(): Promise<void>;
}

export async function createE2eEnvironment():
    Promise<E2eEnvironment> {
    const virtualHost =
        `commerce-flow-e2e-${randomBytes(4).toString("hex")}`;

    await managementRequest(
        "PUT",
        `/api/vhosts/${encodeURIComponent(virtualHost)}`
    );

    await managementRequest(
        "PUT",
        `/api/permissions/${encodeURIComponent(virtualHost)}/` +
        `${encodeURIComponent(decodeURIComponent(brokerUrl.username))}`,
        {
            configure: ".*",
            write: ".*",
            read: ".*"
        }
    );

    let database:
        TestDatabase;

    try {
        database =
            await createTestDatabase();
    } catch (error) {
        await deleteVirtualHost(
            virtualHost
        );

        throw error;
    }

    const rabbitMqUrl =
        new URL(
            brokerUrl
        );

    rabbitMqUrl.pathname =
        `/${encodeURIComponent(virtualHost)}`;

    return {
        virtualHost,
        rabbitMqUrl:
            rabbitMqUrl.toString(),
        database,

        async dispose() {
            await Promise.allSettled([
                database.drop(),
                deleteVirtualHost(
                    virtualHost
                )
            ]);
        }
    };
}

async function deleteVirtualHost(
    virtualHost: string
): Promise<void> {
    await managementRequest(
        "DELETE",
        `/api/vhosts/${encodeURIComponent(virtualHost)}`
    );
}

async function managementRequest(
    method: string,
    path: string,
    body?: unknown
): Promise<unknown> {
    const url =
        new URL(
            path,
            `${managementUrl.protocol}//${managementUrl.host}`
        );

    const credentials =
        Buffer.from(
            `${decodeURIComponent(managementUrl.username)}:` +
            `${decodeURIComponent(managementUrl.password)}`
        ).toString(
            "base64"
        );

    const response =
        await fetch(
            url,
            {
                method,
                headers: {
                    authorization:
                        `Basic ${credentials}`,
                    "content-type":
                        "application/json"
                },
                body:
                    body === undefined
                        ? undefined
                        : JSON.stringify(
                            body
                        )
            }
        );

    if (!response.ok) {
        throw new Error(
            `RabbitMQ management ${method} ${path} failed ` +
            `with HTTP ${response.status}.`
        );
    }

    const text =
        await response.text();

    return text.length > 0
        ? JSON.parse(
            text
        )
        : undefined;
}
