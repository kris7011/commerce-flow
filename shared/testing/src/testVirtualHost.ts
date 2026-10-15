import {
    randomBytes
} from "node:crypto";

/*
 * A dedicated RabbitMQ virtual host per test (file). A fresh virtual
 * host has no queues and no bindings, so tests start from a known,
 * empty broker topology and cannot see messages from other runs.
 */

export const TEST_VIRTUAL_HOST_PREFIX =
    "commerce-flow-test-";

const TEST_VIRTUAL_HOST_PATTERN =
    /^commerce-flow-test-[a-f0-9]{8}$/;

export interface TestVirtualHost {
    readonly name: string;

    /** AMQP URL that connects to this virtual host. */
    readonly url: string;

    delete(): Promise<void>;
}

export interface CreateTestVirtualHostOptions {
    readonly brokerUrl?: string;
    readonly managementUrl?: string;
}

export function assertTestVirtualHostName(
    name: string
): void {
    if (!TEST_VIRTUAL_HOST_PATTERN.test(name)) {
        throw new Error(
            `Refusing to delete RabbitMQ virtual host '${name}': ` +
            `it is not a dedicated test virtual host.`
        );
    }
}

export async function createTestVirtualHost(
    options: CreateTestVirtualHostOptions = {}
): Promise<TestVirtualHost> {
    const brokerUrl =
        new URL(
            options.brokerUrl ??
            process.env.RABBITMQ_URL ??
            "amqp://guest:guest@localhost:5672"
        );

    const managementUrl =
        new URL(
            options.managementUrl ??
            process.env.RABBITMQ_MANAGEMENT_URL ??
            "http://guest:guest@localhost:15672"
        );

    const name =
        `${TEST_VIRTUAL_HOST_PREFIX}${randomBytes(4).toString("hex")}`;

    const encodedName =
        encodeURIComponent(
            name
        );

    await managementRequest(
        managementUrl,
        "PUT",
        `/api/vhosts/${encodedName}`
    );

    try {
        await managementRequest(
            managementUrl,
            "PUT",
            `/api/permissions/${encodedName}/` +
            `${encodeURIComponent(decodeURIComponent(brokerUrl.username))}`,
            {
                configure: ".*",
                write: ".*",
                read: ".*"
            }
        );
    } catch (error) {
        await managementRequest(
            managementUrl,
            "DELETE",
            `/api/vhosts/${encodedName}`
        ).catch(
            () => undefined
        );

        throw error;
    }

    const url =
        new URL(
            brokerUrl
        );

    url.pathname =
        `/${encodedName}`;

    let deleted =
        false;

    return {
        name,
        url:
            url.toString(),

        async delete(): Promise<void> {
            if (deleted) {
                return;
            }

            deleted =
                true;

            assertTestVirtualHostName(
                name
            );

            await managementRequest(
                managementUrl,
                "DELETE",
                `/api/vhosts/${encodedName}`
            );
        }
    };
}

async function managementRequest(
    managementUrl: URL,
    method: string,
    path: string,
    body?: unknown
): Promise<void> {
    const credentials =
        Buffer.from(
            `${decodeURIComponent(managementUrl.username)}:` +
            `${decodeURIComponent(managementUrl.password)}`
        ).toString(
            "base64"
        );

    const response =
        await fetch(
            new URL(
                path,
                `${managementUrl.protocol}//${managementUrl.host}`
            ),
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
}
