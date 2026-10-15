import assert from "node:assert/strict";
import {
    spawn,
    type ChildProcess
} from "node:child_process";
import {
    once
} from "node:events";
import path from "node:path";
import {
    fileURLToPath
} from "node:url";
import type {
    E2eEnvironment
} from "./environment.js";

export type ServiceName =
    | "order-service"
    | "payment-service"
    | "inventory-service"
    | "delivery-service"
    | "notification-service";

/** Start order: consumers first, so their queues exist before events flow. */
export const ALL_SERVICES:
    readonly ServiceName[] = [
        "notification-service",
        "delivery-service",
        "inventory-service",
        "payment-service",
        "order-service"
    ];

const PORT_VARIABLES:
    Readonly<Record<ServiceName, string>> = {
        "order-service": "ORDER_SERVICE_PORT",
        "payment-service": "PAYMENT_SERVICE_PORT",
        "inventory-service": "INVENTORY_SERVICE_PORT",
        "delivery-service": "DELIVERY_SERVICE_PORT",
        "notification-service": "NOTIFICATION_SERVICE_PORT"
    };

const PORT_OFFSETS:
    Readonly<Record<ServiceName, number>> = {
        "order-service": 1,
        "payment-service": 2,
        "inventory-service": 3,
        "delivery-service": 4,
        "notification-service": 5
    };

const repositoryRoot =
    fileURLToPath(
        new URL(
            "../../../",
            import.meta.url
        )
    );

interface RunningService {
    readonly name: ServiceName;
    readonly process: ChildProcess;
    readonly output: string[];
}

export interface ReadinessResponse {
    readonly status: "Ready" | "NotReady";
    readonly dependencies: Readonly<Record<string, "Ready" | "NotReady">>;
}

export interface OrderItem {
    readonly productId: string;
    readonly quantity: number;
    readonly unitPrice: number;
}

export interface CreatedOrderResponse {
    readonly orderId: string;
    readonly status: "Created";
    readonly totalAmount: number;
    readonly correlationId: string;
}

export interface CustomerNotification {
    readonly notificationId: string;
    readonly orderId: string;
    readonly type: "DeliveryBooked" | "InventoryReservationFailed";
    readonly message: string;
    readonly correlationId: string;
}

/**
 * Runs the real service entry points as child processes against an
 * isolated environment, and collects their structured logs so failures
 * can be diagnosed from the test output.
 */
export class ServiceCluster {
    private readonly running =
        new Map<ServiceName, RunningService>();

    private readonly history:
        RunningService[] = [];

    constructor(
        private readonly environment: E2eEnvironment,
        private readonly portBase: number
    ) {
    }

    url(
        name: ServiceName
    ): string {
        return `http://127.0.0.1:${this.portBase + PORT_OFFSETS[name]}`;
    }

    start(
        name: ServiceName
    ): void {
        assert.equal(
            this.running.has(
                name
            ),
            false,
            `${name} is already running.`
        );

        const output: string[] = [];

        const child =
            spawn(
                process.execPath,
                [
                    "--import",
                    "tsx",
                    path.join(
                        repositoryRoot,
                        `services/${name}/src/index.ts`
                    )
                ],
                {
                    cwd:
                        repositoryRoot,
                    env: {
                        ...process.env,
                        RABBITMQ_URL:
                            this.environment.rabbitMqUrl,
                        DATABASE_URL:
                            this.environment.database.connectionString,
                        OUTBOX_POLL_INTERVAL_MS:
                            "200",
                        [PORT_VARIABLES[name]]:
                            String(
                                this.portBase +
                                PORT_OFFSETS[name]
                            )
                    },
                    stdio: [
                        "ignore",
                        "pipe",
                        "pipe"
                    ],
                    windowsHide:
                        true
                }
            );

        for (const stream of [child.stdout, child.stderr]) {
            stream?.setEncoding(
                "utf8"
            );

            stream?.on(
                "data",
                (chunk: string) => {
                    output.push(
                        chunk
                    );
                }
            );
        }

        const service = {
            name,
            process: child,
            output
        };

        this.running.set(
            name,
            service
        );

        this.history.push(
            service
        );
    }

    async startAll(
        names: readonly ServiceName[] = ALL_SERVICES,
        timeoutInMs = 30_000
    ): Promise<void> {
        for (const name of names) {
            this.start(
                name
            );

            await this.waitForReadiness(
                name,
                timeoutInMs
            );
        }
    }

    async readiness(
        name: ServiceName
    ): Promise<{
        httpStatus: number;
        body: ReadinessResponse;
    }> {
        const response =
            await fetch(
                `${this.url(name)}/ready`
            );

        return {
            httpStatus:
                response.status,
            body:
                await response.json() as ReadinessResponse
        };
    }

    async waitForHealth(
        name: ServiceName,
        timeoutInMs: number
    ): Promise<void> {
        await this.waitForService(
            name,
            timeoutInMs,
            "healthy",
            async () => {
                const response =
                    await fetch(
                        `${this.url(name)}/health`
                    );

                return response.status === 200;
            }
        );
    }

    async waitForReadiness(
        name: ServiceName,
        timeoutInMs: number
    ): Promise<void> {
        await this.waitForService(
            name,
            timeoutInMs,
            "ready",
            async () =>
                (await this.readiness(name)).httpStatus === 200
        );
    }

    /** Resolves once the service has written a log entry with `message`. */
    async waitForLog(
        name: ServiceName,
        message: string,
        predicate: (context: Record<string, unknown>) => boolean,
        timeoutInMs: number
    ): Promise<void> {
        await waitUntil(
            () =>
                this.logEntries(name).some(
                    entry =>
                        entry.message === message &&
                        predicate(
                            entry.context ?? {}
                        )
                ),
            timeoutInMs,
            `${name} to log '${message}'`
        );
    }

    logEntries(
        name: ServiceName
    ): {
        message: string;
        context?: Record<string, unknown>;
    }[] {
        return this.history
            .filter(
                service =>
                    service.name === name
            )
            .flatMap(
                service =>
                    service.output
                        .join("")
                        .split(/\r?\n/)
            )
            .flatMap(
                line => {
                    try {
                        const entry =
                            JSON.parse(
                                line
                            ) as {
                                message?: unknown;
                                context?: Record<string, unknown>;
                            };

                        return typeof entry.message === "string"
                            ? [
                                {
                                    message:
                                        entry.message,
                                    context:
                                        entry.context
                                }
                            ]
                            : [];
                    } catch {
                        return [];
                    }
                }
            );
    }

    processId(
        name: ServiceName
    ): number | undefined {
        return this.running.get(
            name
        )?.process.pid;
    }

    isRunning(
        name: ServiceName
    ): boolean {
        const service =
            this.running.get(
                name
            );

        return (
            service !== undefined &&
            !hasExited(
                service.process
            )
        );
    }

    /**
     * Stops a service. "SIGKILL" simulates a crash: no shutdown hooks
     * run and in-flight work is abandoned.
     */
    async stop(
        name: ServiceName,
        signal: "SIGTERM" | "SIGKILL" = "SIGTERM"
    ): Promise<void> {
        const service =
            this.running.get(
                name
            );

        if (!service) {
            return;
        }

        this.running.delete(
            name
        );

        await terminate(
            service.process,
            signal
        );
    }

    async stopAll(): Promise<void> {
        for (const name of [...this.running.keys()].reverse()) {
            await this.stop(
                name
            );
        }
    }

    formatLogs(): string {
        if (this.history.length === 0) {
            return "No services were started.";
        }

        return this.history
            .map(
                service =>
                    `--- ${service.name} (pid ${service.process.pid}) ---\n` +
                    (service.output.join("").trim() || "(no output)")
            )
            .join(
                "\n\n"
            );
    }

    private async waitForService(
        name: ServiceName,
        timeoutInMs: number,
        state: string,
        probe: () => Promise<boolean>
    ): Promise<void> {
        await waitUntil(
            async () => {
                const service =
                    this.running.get(
                        name
                    );

                if (
                    !service ||
                    hasExited(
                        service.process
                    )
                ) {
                    throw new Error(
                        `${name} exited before becoming ${state}.`
                    );
                }

                try {
                    return await probe();
                } catch {
                    return false;
                }
            },
            timeoutInMs,
            `${name} to become ${state}`
        );
    }
}

export async function createOrder(
    orderServiceUrl: string,
    correlationId: string,
    items: readonly OrderItem[]
): Promise<CreatedOrderResponse> {
    const response =
        await fetch(
            `${orderServiceUrl}/orders`,
            {
                method: "POST",
                headers: {
                    "content-type":
                        "application/json",
                    "x-correlation-id":
                        correlationId
                },
                body:
                    JSON.stringify({
                        customerId:
                            `customer-${correlationId}`,
                        items
                    })
            }
        );

    const body =
        await response.json();

    assert.equal(
        response.status,
        201,
        `Expected HTTP 201 from POST /orders, received ` +
        `${response.status}: ${JSON.stringify(body)}`
    );

    return body as CreatedOrderResponse;
}

export async function getNotifications(
    notificationServiceUrl: string
): Promise<CustomerNotification[]> {
    const body =
        await getJson<{
            notifications: CustomerNotification[];
        }>(
            `${notificationServiceUrl}/notifications`
        );

    return body.notifications;
}

export async function waitForNotification(
    notificationServiceUrl: string,
    correlationId: string,
    type: CustomerNotification["type"],
    timeoutInMs: number
): Promise<CustomerNotification> {
    let found:
        CustomerNotification | undefined;

    await waitUntil(
        async () => {
            found =
                (await getNotifications(
                    notificationServiceUrl
                )).find(
                    notification =>
                        notification.correlationId === correlationId &&
                        notification.type === type
                );

            return found !== undefined;
        },
        timeoutInMs,
        `a ${type} notification for '${correlationId}'`
    );

    assert.ok(
        found
    );

    return found;
}

export async function getStock(
    inventoryServiceUrl: string
): Promise<Record<string, number>> {
    return (
        await getJson<{
            stock: Record<string, number>;
        }>(
            `${inventoryServiceUrl}/stock`
        )
    ).stock;
}

export async function getJson<T>(
    url: string
): Promise<T> {
    const response =
        await fetch(
            url
        );

    if (!response.ok) {
        throw new Error(
            `GET ${url} returned HTTP ${response.status}.`
        );
    }

    return await response.json() as T;
}

export async function waitUntil(
    condition: () => Promise<boolean> | boolean,
    timeoutInMs: number,
    description: string
): Promise<void> {
    const deadline =
        Date.now() +
        timeoutInMs;

    let lastError:
        unknown;

    while (Date.now() < deadline) {
        try {
            if (await condition()) {
                return;
            }
        } catch (error) {
            if (
                error instanceof Error &&
                /exited before/.test(
                    error.message
                )
            ) {
                throw error;
            }

            lastError =
                error;
        }

        await new Promise(
            resolve =>
                setTimeout(
                    resolve,
                    100
                )
        );
    }

    throw new Error(
        `Timed out after ${timeoutInMs} ms waiting for ${description}.` +
        (
            lastError instanceof Error
                ? ` Last error: ${lastError.message}`
                : ""
        )
    );
}

function hasExited(
    child: ChildProcess
): boolean {
    return (
        child.exitCode !== null ||
        child.signalCode !== null
    );
}

async function terminate(
    child: ChildProcess,
    signal: "SIGTERM" | "SIGKILL"
): Promise<void> {
    if (hasExited(child)) {
        return;
    }

    const exited =
        once(
            child,
            "exit"
        ).catch(
            () => undefined
        );

    child.kill(
        signal
    );

    const timedOut =
        await Promise.race([
            exited.then(
                () => false
            ),
            new Promise<boolean>(
                resolve =>
                    setTimeout(
                        () => resolve(true),
                        10_000
                    ).unref()
            )
        ]);

    if (
        timedOut &&
        !hasExited(child)
    ) {
        child.kill(
            "SIGKILL"
        );

        await exited;
    }
}
