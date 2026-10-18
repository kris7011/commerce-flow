import * as amqp from "amqplib";
import {
    COMMERCE_TOPOLOGY
} from "@commerce-flow/contracts";
import {
    SERVICE_NAMES,
    type Endpoints,
    type ServiceName
} from "./endpoints.js";

/*
 * Read-only HTTP probes against the running system: service readiness,
 * Inventory's stock and outbox diagnostics, Notification's results and
 * RabbitMQ queue depths.
 */

export interface HttpResult {
    readonly status: number;
    readonly body: unknown;
}

export async function requestJson(
    url: string,
    init: RequestInit = {},
    timeoutInMs = 5_000
): Promise<HttpResult> {
    const response =
        await fetch(
            url,
            {
                ...init,
                signal:
                    AbortSignal.timeout(
                        timeoutInMs
                    )
            }
        );

    const text =
        await response.text();

    let body: unknown = text;

    try {
        body = JSON.parse(text);
    } catch {
        // Keep the raw text.
    }

    return {
        status:
            response.status,
        body
    };
}

export interface ServiceReadiness {
    readonly service: ServiceName;
    readonly ready: boolean;
    readonly detail: string;
}

export async function readReadiness(
    endpoints: Endpoints
): Promise<ServiceReadiness[]> {
    return await Promise.all(
        SERVICE_NAMES.map(async service => {
            try {
                const {
                    status,
                    body
                } =
                    await requestJson(
                        `${endpoints.services[service]}/ready`,
                        {},
                        2_000
                    );

                return {
                    service,
                    ready:
                        status === 200,
                    detail:
                        describeDependencies(body) ??
                        `HTTP ${status}`
                };
            } catch {
                return {
                    service,
                    ready: false,
                    detail:
                        "not reachable"
                };
            }
        })
    );
}

export async function readStock(
    endpoints: Endpoints
): Promise<Record<string, number>> {
    const {
        status,
        body
    } =
        await requestJson(
            `${endpoints.services["inventory-service"]}/stock`
        );

    if (
        status !== 200 ||
        !isRecord(body) ||
        !isRecord(body.stock)
    ) {
        throw new Error(
            `GET /stock answered HTTP ${status}.`
        );
    }

    return Object.fromEntries(
        Object.entries(body.stock)
            .filter(
                (entry): entry is [string, number] =>
                    typeof entry[1] === "number"
            )
    );
}

export async function readOutboxDiagnostics(
    endpoints: Endpoints
): Promise<Record<string, unknown> | undefined> {
    try {
        const {
            status,
            body
        } =
            await requestJson(
                `${endpoints.services["inventory-service"]}/diagnostics/outbox`
            );

        return status === 200 && isRecord(body)
            ? body
            : undefined;
    } catch {
        return undefined;
    }
}

export interface NotificationSummary {
    readonly orderId: string;
    readonly type: string;
    readonly message: string;
}

export async function readNotifications(
    endpoints: Endpoints
): Promise<NotificationSummary[]> {
    const {
        status,
        body
    } =
        await requestJson(
            `${endpoints.services["notification-service"]}/notifications`
        );

    if (
        status !== 200 ||
        !isRecord(body) ||
        !Array.isArray(body.notifications)
    ) {
        throw new Error(
            `GET /notifications answered HTTP ${status}.`
        );
    }

    return body.notifications
        .filter(isRecord)
        .map(notification => ({
            orderId:
                String(notification.orderId),
            type:
                String(notification.type),
            message:
                String(notification.message)
        }));
}

export interface QueueDepth {
    readonly name: string;

    /** Messages ready for delivery (not yet handed to a consumer). */
    readonly messages: number;
    readonly consumers: number;

    /** The queue does not exist (no service has declared it yet). */
    readonly missing?: boolean;
}

/** The consumer queues and their dead-letter queues, in topology order. */
export const OBSERVED_QUEUES:
    readonly string[] =
    COMMERCE_TOPOLOGY.flatMap(
        queue => [
            queue.queueName,
            `${queue.queueName}.dead-letter`
        ]
    );

/**
 * Current queue depths, read over AMQP (`queue.declare` passive). Unlike
 * the management API, whose statistics lag by several seconds, this
 * returns the broker's live count of ready messages.
 */
export async function readQueueDepths(
    endpoints: Endpoints
): Promise<QueueDepth[]> {
    const connection =
        await amqp.connect(
            endpoints.rabbitMqUrl
        );

    try {
        const depths: QueueDepth[] = [];

        for (const name of OBSERVED_QUEUES) {
            // A passive check of a missing queue closes the channel, so
            // each check gets its own.
            const channel =
                await connection.createChannel();

            try {
                const {
                    messageCount,
                    consumerCount
                } =
                    await channel.checkQueue(
                        name
                    );

                depths.push({
                    name,
                    messages: messageCount,
                    consumers: consumerCount
                });

                await channel.close();
            } catch {
                depths.push({
                    name,
                    messages: 0,
                    consumers: 0,
                    missing: true
                });
            }
        }

        return depths;
    } finally {
        await connection
            .close()
            .catch(() => undefined);
    }
}

export function countDeadLetters(
    queues: readonly QueueDepth[]
): number {
    return queues
        .filter(queue => queue.name.endsWith(".dead-letter"))
        .reduce(
            (sum, queue) => sum + queue.messages,
            0
        );
}

function describeDependencies(
    body: unknown
): string | undefined {
    if (
        !isRecord(body) ||
        !isRecord(body.dependencies)
    ) {
        return undefined;
    }

    return Object.entries(body.dependencies)
        .map(
            ([name, status]) =>
                `${name}: ${String(status)}`
        )
        .join(", ");
}

function isRecord(
    value: unknown
): value is Record<string, unknown> {
    return (
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
    );
}
