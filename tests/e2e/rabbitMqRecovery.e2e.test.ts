import assert from "node:assert/strict";
import {
    spawnSync
} from "node:child_process";
import {
    randomUUID
} from "node:crypto";
import {
    createConnection
} from "node:net";
import test from "node:test";
import {
    fileURLToPath
} from "node:url";
import {
    createE2eEnvironment
} from "./support/environment.js";
import {
    ALL_SERVICES,
    ServiceCluster,
    createOrder,
    waitForNotification,
    waitForOrderTimeline,
    waitUntil
} from "./support/serviceCluster.js";

const repositoryRoot =
    fileURLToPath(
        new URL(
            "../../",
            import.meta.url
        )
    );

test(
    "recovers the complete workflow after RabbitMQ becomes available",
    {
        timeout: 150_000
    },
    async () => {
        const environment =
            await createE2eEnvironment();

        const cluster =
            new ServiceCluster(
                environment,
                3200
            );

        try {
            runDockerCompose(
                "stop",
                "rabbitmq"
            );

            await waitForTcpPortState(
                5672,
                false,
                15_000
            );

            for (const name of ALL_SERVICES) {
                cluster.start(
                    name
                );
            }

            await Promise.all(
                ALL_SERVICES.map(
                    name =>
                        cluster.waitForHealth(
                            name,
                            20_000
                        )
                )
            );

            const processIds =
                new Map(
                    ALL_SERVICES.map(
                        name => [
                            name,
                            cluster.processId(
                                name
                            )
                        ]
                    )
                );

            for (const name of ALL_SERVICES) {
                const readiness =
                    await cluster.readiness(
                        name
                    );

                assert.equal(
                    readiness.httpStatus,
                    503,
                    `${name} must not be ready while RabbitMQ is down.`
                );

                assert.equal(
                    readiness.body.dependencies.rabbitMq,
                    "NotReady"
                );
            }

            // The broker is down, but the order is durable: it is
            // accepted now and its event is published after recovery.
            const offlineCorrelationId =
                `e2e-offline-${randomUUID()}`;

            const offlineOrder =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    offlineCorrelationId,
                    [
                        {
                            productId: "dishwasher-01",
                            quantity: 1,
                            unitPrice: 3999
                        }
                    ]
                );

            runDockerCompose(
                "up",
                "-d",
                "--wait",
                "rabbitmq"
            );

            await Promise.all(
                ALL_SERVICES.map(
                    name =>
                        cluster.waitForReadiness(
                            name,
                            60_000
                        )
                )
            );

            for (const name of ALL_SERVICES) {
                assert.equal(
                    cluster.processId(
                        name
                    ),
                    processIds.get(
                        name
                    ),
                    `${name} must recover without restarting.`
                );
            }

            const correlationId =
                `e2e-recovery-${randomUUID()}`;

            const order =
                await createOrder(
                    cluster.url(
                        "order-service"
                    ),
                    correlationId,
                    [
                        {
                            productId: "washing-machine-01",
                            quantity: 1,
                            unitPrice: 4999
                        }
                    ]
                );

            const notification =
                await waitForNotification(
                    cluster.url(
                        "notification-service"
                    ),
                    correlationId,
                    "DeliveryBooked",
                    20_000
                );

            assert.equal(
                notification.orderId,
                order.orderId
            );

            const offlineTimeline =
                await waitForOrderTimeline(
                    cluster.url(
                        "order-service"
                    ),
                    offlineOrder.orderId,
                    "DeliveryBooked",
                    30_000
                );

            assert.equal(
                offlineTimeline.outcome,
                "Completed"
            );
        } catch (error) {
            throw new Error(
                "RabbitMQ recovery end-to-end test failed.\n\n" +
                cluster.formatLogs(),
                {
                    cause: error
                }
            );
        } finally {
            await cluster.stopAll();

            try {
                runDockerCompose(
                    "up",
                    "-d",
                    "--wait",
                    "rabbitmq"
                );
            } catch {
                // Do not hide the original failure during cleanup.
            }

            await environment.dispose();
        }
    }
);

function runDockerCompose(
    ...argumentsToDocker: string[]
): void {
    const result =
        spawnSync(
            "docker",
            [
                "compose",
                ...argumentsToDocker
            ],
            {
                cwd:
                    repositoryRoot,
                encoding:
                    "utf8",
                windowsHide:
                    true
            }
        );

    if (result.error) {
        throw result.error;
    }

    if (result.status !== 0) {
        throw new Error(
            `docker compose ${argumentsToDocker.join(" ")} failed ` +
            `with exit code ${String(result.status)}.\n` +
            `${result.stderr || result.stdout}`
        );
    }
}

async function waitForTcpPortState(
    port: number,
    expectedOpen: boolean,
    timeoutInMs: number
): Promise<void> {
    await waitUntil(
        async () =>
            (await canConnect(port)) === expectedOpen,
        timeoutInMs,
        `TCP port ${port} to become ${expectedOpen ? "open" : "closed"}`
    );
}

function canConnect(
    port: number
): Promise<boolean> {
    return new Promise(
        resolve => {
            const socket =
                createConnection({
                    host: "127.0.0.1",
                    port
                });

            const finish =
                (open: boolean): void => {
                    socket.removeAllListeners();
                    socket.destroy();
                    resolve(open);
                };

            socket.setTimeout(500);
            socket.once("connect", () => finish(true));
            socket.once("timeout", () => finish(false));
            socket.once("error", () => finish(false));
        }
    );
}
