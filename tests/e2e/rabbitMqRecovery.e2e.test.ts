import assert from "node:assert/strict";
import {
    spawn,
    spawnSync,
    type ChildProcess
} from "node:child_process";
import {
    randomUUID
} from "node:crypto";
import {
    once
} from "node:events";
import {
    createConnection
} from "node:net";
import path from "node:path";
import test from "node:test";
import {
    fileURLToPath
} from "node:url";

interface ServiceDefinition {
    readonly name: string;
    readonly entryPoint: string;
    readonly port: number;
    readonly portEnvironmentVariable: string;
}

interface RunningService {
    readonly definition:
    ServiceDefinition;

    readonly process:
    ChildProcess;

    readonly standardOutput:
    string[];

    readonly standardError:
    string[];
}

interface HealthResponse {
    status: string;
    service: string;
}

interface ReadinessResponse {
    status:
    | "Ready"
    | "NotReady";

    service: string;

    dependencies: {
        rabbitMq:
        | "Ready"
        | "NotReady";
    };
}

interface CreatedOrderResponse {
    orderId: string;
    status: "Created";
    totalAmount: number;
    correlationId: string;
}

interface CustomerNotification {
    notificationId: string;
    orderId: string;

    type:
    | "DeliveryBooked"
    | "InventoryReservationFailed";

    message: string;
    correlationId: string;
    createdAt: string;
}

interface NotificationsResponse {
    notifications:
    CustomerNotification[];
}

const repositoryRoot =
    fileURLToPath(
        new URL(
            "../../",
            import.meta.url
        )
    );

const rabbitMqUrl =
    "amqp://guest:guest@localhost:5672";

const orderServiceUrl =
    "http://127.0.0.1:3201";

const notificationServiceUrl =
    "http://127.0.0.1:3205";

const serviceDefinitions:
    readonly ServiceDefinition[] = [
        {
            name:
                "notification-service",
            entryPoint:
                "services/notification-service/src/index.ts",
            port:
                3205,
            portEnvironmentVariable:
                "NOTIFICATION_SERVICE_PORT"
        },
        {
            name:
                "delivery-service",
            entryPoint:
                "services/delivery-service/src/index.ts",
            port:
                3204,
            portEnvironmentVariable:
                "DELIVERY_SERVICE_PORT"
        },
        {
            name:
                "inventory-service",
            entryPoint:
                "services/inventory-service/src/index.ts",
            port:
                3203,
            portEnvironmentVariable:
                "INVENTORY_SERVICE_PORT"
        },
        {
            name:
                "payment-service",
            entryPoint:
                "services/payment-service/src/index.ts",
            port:
                3202,
            portEnvironmentVariable:
                "PAYMENT_SERVICE_PORT"
        },
        {
            name:
                "order-service",
            entryPoint:
                "services/order-service/src/index.ts",
            port:
                3201,
            portEnvironmentVariable:
                "ORDER_SERVICE_PORT"
        }
    ];

test(
    "recovers the complete workflow after RabbitMQ becomes available",
    {
        timeout:
            90_000
    },
    async () => {
        const runningServices:
            RunningService[] = [];

        try {
            runDockerCompose(
                "stop",
                "rabbitmq"
            );

            await waitForTcpPortState(
                5672,
                false,
                10_000
            );

            for (
                const definition of
                serviceDefinitions
            ) {
                runningServices.push(
                    startService(
                        definition
                    )
                );
            }

            await Promise.all(
                runningServices.map(
                    runningService =>
                        waitForServiceHealth(
                            runningService,
                            15_000
                        )
                )
            );

            const processIds =
                new Map<
                    string,
                    number
                >();

            for (
                const runningService of
                runningServices
            ) {
                const processId =
                    runningService
                        .process.pid;

                assert.notEqual(
                    processId,
                    undefined,
                    `${runningService.definition.name} ` +
                    `did not receive a process id.`
                );

                processIds.set(
                    runningService
                        .definition.name,
                    processId as number
                );
            }

            const initialReadiness =
                await Promise.all(
                    runningServices.map(
                        runningService =>
                            getReadiness(
                                runningService
                            )
                    )
                );

            for (
                const result of
                initialReadiness
            ) {
                assert.equal(
                    result.httpStatus,
                    503,
                    `${result.service} should be ` +
                    `HTTP 503 while RabbitMQ is unavailable.`
                );

                assert.equal(
                    result.body.status,
                    "NotReady"
                );

                assert.equal(
                    result.body
                        .dependencies
                        .rabbitMq,
                    "NotReady"
                );
            }

            runDockerCompose(
                "up",
                "-d",
                "--wait",
                "rabbitmq"
            );

            await waitForTcpPortState(
                5672,
                true,
                15_000
            );

            await Promise.all(
                runningServices.map(
                    runningService =>
                        waitForServiceReadiness(
                            runningService,
                            45_000
                        )
                )
            );

            for (
                const runningService of
                runningServices
            ) {
                assert.equal(
                    hasProcessExited(
                        runningService
                            .process
                    ),
                    false,
                    `${runningService.definition.name} ` +
                    `exited during RabbitMQ recovery.`
                );

                assert.equal(
                    runningService
                        .process.pid,
                    processIds.get(
                        runningService
                            .definition.name
                    ),
                    `${runningService.definition.name} ` +
                    `did not remain on the original process.`
                );
            }

            const correlationId =
                `e2e-recovery-${randomUUID()}`;

            const order =
                await createOrder(
                    correlationId
                );

            assert.equal(
                order.status,
                "Created"
            );

            assert.equal(
                order.correlationId,
                correlationId
            );

            const notification =
                await waitForNotification(
                    correlationId,
                    15_000
                );

            assert.equal(
                notification.orderId,
                order.orderId
            );

            assert.equal(
                notification.correlationId,
                correlationId
            );

            assert.equal(
                notification.type,
                "DeliveryBooked"
            );

            assert.match(
                notification.message,
                /Delivery has been booked/
            );
        } catch (error) {
            throw new Error(
                "RabbitMQ recovery end-to-end test failed." +
                "\n\n" +
                formatServiceLogs(
                    runningServices
                ),
                {
                    cause:
                        error
                }
            );
        } finally {
            for (
                const runningService of
                [...runningServices]
                    .reverse()
            ) {
                await stopService(
                    runningService
                );
            }

            try {
                runDockerCompose(
                    "up",
                    "-d",
                    "--wait",
                    "rabbitmq"
                );
            } catch {
                // Do not hide the original
                // test failure during cleanup.
            }
        }
    }
);

function startService(
    definition:
        ServiceDefinition
): RunningService {
    const standardOutput:
        string[] = [];

    const standardError:
        string[] = [];

    const entryPoint =
        path.join(
            repositoryRoot,
            definition.entryPoint
        );

    const serviceProcess =
        spawn(
            process.execPath,
            [
                "--import",
                "tsx",
                entryPoint
            ],
            {
                cwd:
                    repositoryRoot,

                env: {
                    ...process.env,

                    RABBITMQ_URL:
                        rabbitMqUrl,

                    [definition
                        .portEnvironmentVariable]:
                        String(
                            definition.port
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

    serviceProcess.stdout
        ?.setEncoding(
            "utf8"
        );

    serviceProcess.stderr
        ?.setEncoding(
            "utf8"
        );

    serviceProcess.stdout?.on(
        "data",
        (chunk: string) => {
            standardOutput.push(
                chunk
            );
        }
    );

    serviceProcess.stderr?.on(
        "data",
        (chunk: string) => {
            standardError.push(
                chunk
            );
        }
    );

    return {
        definition,
        process:
            serviceProcess,
        standardOutput,
        standardError
    };
}

async function waitForServiceHealth(
    runningService:
        RunningService,
    timeoutInMs:
        number
): Promise<void> {
    const deadline =
        Date.now() +
        timeoutInMs;

    const healthUrl =
        `http://127.0.0.1:` +
        `${runningService.definition.port}` +
        `/health`;

    let lastError:
        unknown;

    while (
        Date.now() <
        deadline
    ) {
        if (
            hasProcessExited(
                runningService
                    .process
            )
        ) {
            throw new Error(
                `${runningService.definition.name} ` +
                `exited before becoming healthy.`
            );
        }

        try {
            const response =
                await fetch(
                    healthUrl
                );

            if (
                response.status ===
                200
            ) {
                const body =
                    await response.json() as HealthResponse;

                if (
                    body.status ===
                    "Healthy"
                ) {
                    return;
                }
            }
        } catch (error) {
            lastError =
                error;
        }

        await delay(
            200
        );
    }

    throw new Error(
        `Timed out waiting for ` +
        `${runningService.definition.name} ` +
        `health endpoint. ` +
        `Last error: ` +
        `${getErrorMessage(lastError)}`
    );
}

async function getReadiness(
    runningService:
        RunningService
): Promise<{
    service: string;
    httpStatus: number;
    body: ReadinessResponse;
}> {
    const readinessUrl =
        `http://127.0.0.1:` +
        `${runningService.definition.port}` +
        `/ready`;

    const response =
        await fetch(
            readinessUrl
        );

    const body =
        await response.json() as ReadinessResponse;

    return {
        service:
            runningService
                .definition.name,

        httpStatus:
            response.status,

        body
    };
}

async function waitForServiceReadiness(
    runningService:
        RunningService,
    timeoutInMs:
        number
): Promise<void> {
    const deadline =
        Date.now() +
        timeoutInMs;

    let lastStatus:
        string =
        "No response";

    while (
        Date.now() <
        deadline
    ) {
        if (
            hasProcessExited(
                runningService
                    .process
            )
        ) {
            throw new Error(
                `${runningService.definition.name} ` +
                `exited before becoming ready.`
            );
        }

        try {
            const result =
                await getReadiness(
                    runningService
                );

            lastStatus =
                `HTTP ${result.httpStatus}: ` +
                `${JSON.stringify(result.body)}`;

            if (
                result.httpStatus ===
                200 &&
                result.body.status ===
                "Ready" &&
                result.body
                    .dependencies
                    .rabbitMq ===
                "Ready"
            ) {
                return;
            }
        } catch (error) {
            lastStatus =
                getErrorMessage(
                    error
                );
        }

        await delay(
            250
        );
    }

    throw new Error(
        `Timed out waiting for ` +
        `${runningService.definition.name} ` +
        `to recover RabbitMQ readiness. ` +
        `Last status: ${lastStatus}`
    );
}

async function createOrder(
    correlationId:
        string
): Promise<CreatedOrderResponse> {
    const response =
        await fetch(
            `${orderServiceUrl}/orders`,
            {
                method:
                    "POST",

                headers: {
                    "content-type":
                        "application/json",

                    "x-correlation-id":
                        correlationId
                },

                body:
                    JSON.stringify({
                        customerId:
                            "recovery-test-customer",

                        items: [
                            {
                                productId:
                                    "washing-machine-01",

                                quantity:
                                    1,

                                unitPrice:
                                    4999
                            }
                        ]
                    })
            }
        );

    const body =
        await response.json();

    assert.equal(
        response.status,
        201,
        `Expected recovered Order Service ` +
        `to return HTTP 201, but received ` +
        `${response.status}: ` +
        `${JSON.stringify(body)}`
    );

    return body as
        CreatedOrderResponse;
}

async function waitForNotification(
    correlationId:
        string,
    timeoutInMs:
        number
): Promise<CustomerNotification> {
    const deadline =
        Date.now() +
        timeoutInMs;

    while (
        Date.now() <
        deadline
    ) {
        const response =
            await fetch(
                `${notificationServiceUrl}` +
                `/notifications`
            );

        assert.equal(
            response.status,
            200
        );

        const body =
            await response.json() as NotificationsResponse;

        const matchingNotification =
            body.notifications.find(
                notification =>
                    notification
                        .correlationId ===
                    correlationId
            );

        if (
            matchingNotification
        ) {
            return matchingNotification;
        }

        await delay(
            200
        );
    }

    throw new Error(
        `Timed out waiting for ` +
        `recovery notification with ` +
        `correlationId '${correlationId}'.`
    );
}

function runDockerCompose(
    ...argumentsToDocker:
        string[]
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

    if (
        result.error
    ) {
        throw result.error;
    }

    if (
        result.status !==
        0
    ) {
        throw new Error(
            `docker compose ` +
            `${argumentsToDocker.join(" ")} ` +
            `failed with exit code ` +
            `${String(result.status)}.` +
            `\nstdout:\n` +
            `${result.stdout || "(empty)"}` +
            `\nstderr:\n` +
            `${result.stderr || "(empty)"}`
        );
    }
}

async function waitForTcpPortState(
    port:
        number,
    expectedOpen:
        boolean,
    timeoutInMs:
        number
): Promise<void> {
    const deadline =
        Date.now() +
        timeoutInMs;

    while (
        Date.now() <
        deadline
    ) {
        const isOpen =
            await canConnectToPort(
                port
            );

        if (
            isOpen ===
            expectedOpen
        ) {
            return;
        }

        await delay(
            200
        );
    }

    throw new Error(
        `Timed out waiting for TCP port ` +
        `${port} to become ` +
        `${expectedOpen ? "open" : "closed"}.`
    );
}

function canConnectToPort(
    port:
        number
): Promise<boolean> {
    return new Promise(
        resolve => {
            const socket =
                createConnection({
                    host:
                        "127.0.0.1",
                    port
                });

            let completed =
                false;

            const finish = (
                result:
                    boolean
            ): void => {
                if (
                    completed
                ) {
                    return;
                }

                completed =
                    true;

                socket
                    .removeAllListeners();

                socket.destroy();

                resolve(
                    result
                );
            };

            socket.setTimeout(
                500
            );

            socket.once(
                "connect",
                () => {
                    finish(
                        true
                    );
                }
            );

            socket.once(
                "timeout",
                () => {
                    finish(
                        false
                    );
                }
            );

            socket.once(
                "error",
                () => {
                    finish(
                        false
                    );
                }
            );
        }
    );
}

async function stopService(
    runningService:
        RunningService
): Promise<void> {
    const serviceProcess =
        runningService
            .process;

    if (
        hasProcessExited(
            serviceProcess
        )
    ) {
        return;
    }

    const exitPromise =
        once(
            serviceProcess,
            "exit"
        ).catch(
            () =>
                undefined
        );

    try {
        serviceProcess.kill(
            "SIGINT"
        );
    } catch {
        return;
    }

    await Promise.race([
        exitPromise,
        delay(
            3_000
        )
    ]);

    if (
        hasProcessExited(
            serviceProcess
        )
    ) {
        return;
    }

    try {
        serviceProcess.kill(
            "SIGKILL"
        );
    } catch {
        return;
    }

    await Promise.race([
        exitPromise,
        delay(
            1_000
        )
    ]);
}

function hasProcessExited(
    serviceProcess:
        ChildProcess
): boolean {
    return (
        serviceProcess
            .exitCode !==
        null ||
        serviceProcess
            .signalCode !==
        null
    );
}

function formatServiceLogs(
    runningServices:
        readonly RunningService[]
): string {
    if (
        runningServices.length ===
        0
    ) {
        return (
            "No services were started."
        );
    }

    return runningServices
        .map(
            runningService => {
                const standardOutput =
                    runningService
                        .standardOutput
                        .join("")
                        .trim();

                const standardError =
                    runningService
                        .standardError
                        .join("")
                        .trim();

                return [
                    `--- ${runningService.definition.name} stdout ---`,
                    standardOutput ||
                    "(empty)",
                    `--- ${runningService.definition.name} stderr ---`,
                    standardError ||
                    "(empty)"
                ].join(
                    "\n"
                );
            }
        )
        .join(
            "\n\n"
        );
}

function delay(
    milliseconds:
        number
): Promise<void> {
    return new Promise(
        resolve => {
            setTimeout(
                resolve,
                milliseconds
            );
        }
    );
}

function getErrorMessage(
    error:
        unknown
): string {
    if (
        error instanceof
        Error
    ) {
        return error.message;
    }

    return String(
        error
    );
}
