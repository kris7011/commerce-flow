import assert from "node:assert/strict";
import {
    spawn,
    type ChildProcess
} from "node:child_process";
import {
    randomUUID
} from "node:crypto";
import {
    once
} from "node:events";
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
    readonly definition: ServiceDefinition;
    readonly process: ChildProcess;
    readonly standardOutput: string[];
    readonly standardError: string[];
}

interface HealthResponse {
    status: string;
    service: string;
}

interface CreatedOrderResponse {
    orderId: string;
    status: "Created";
    totalAmount: number;
    correlationId: string;
}

interface StockResponse {
    stock: Record<string, number>;
}

type NotificationType =
    | "DeliveryBooked"
    | "InventoryReservationFailed";

interface CustomerNotification {
    notificationId: string;
    orderId: string;
    type: NotificationType;
    message: string;
    correlationId: string;
    createdAt: string;
}

interface NotificationsResponse {
    notifications: CustomerNotification[];
}

interface OrderItem {
    productId: string;
    quantity: number;
    unitPrice: number;
}

const repositoryRoot =
    fileURLToPath(
        new URL(
            "../../",
            import.meta.url
        )
    );

const rabbitMqUrl =
    process.env.RABBITMQ_URL ??
    "amqp://guest:guest@localhost:5672";

const orderServiceUrl =
    "http://127.0.0.1:3101";

const inventoryServiceUrl =
    "http://127.0.0.1:3103";

const notificationServiceUrl =
    "http://127.0.0.1:3105";

const serviceDefinitions:
    readonly ServiceDefinition[] = [
        {
            name: "notification-service",
            entryPoint:
                "services/notification-service/src/index.ts",
            port: 3105,
            portEnvironmentVariable:
                "NOTIFICATION_SERVICE_PORT"
        },
        {
            name: "delivery-service",
            entryPoint:
                "services/delivery-service/src/index.ts",
            port: 3104,
            portEnvironmentVariable:
                "DELIVERY_SERVICE_PORT"
        },
        {
            name: "inventory-service",
            entryPoint:
                "services/inventory-service/src/index.ts",
            port: 3103,
            portEnvironmentVariable:
                "INVENTORY_SERVICE_PORT"
        },
        {
            name: "payment-service",
            entryPoint:
                "services/payment-service/src/index.ts",
            port: 3102,
            portEnvironmentVariable:
                "PAYMENT_SERVICE_PORT"
        },
        {
            name: "order-service",
            entryPoint:
                "services/order-service/src/index.ts",
            port: 3101,
            portEnvironmentVariable:
                "ORDER_SERVICE_PORT"
        }
    ];

test(
    "processes successful and failed orders through the complete workflow",
    {
        timeout: 60_000
    },
    async () => {
        const runningServices:
            RunningService[] = [];

        try {
            for (
                const definition of
                serviceDefinitions
            ) {
                const runningService =
                    startService(definition);

                runningServices.push(
                    runningService
                );

                await waitForServiceHealth(
                    runningService,
                    20_000
                );
            }

            const initialStock =
                await getJson<StockResponse>(
                    `${inventoryServiceUrl}/stock`
                );

            const initialWashingMachineStock =
                getRequiredStock(
                    initialStock,
                    "washing-machine-01"
                );

            const initialDryerStock =
                getRequiredStock(
                    initialStock,
                    "dryer-01"
                );

            const successfulCorrelationId =
                `e2e-success-${randomUUID()}`;

            const successfulOrder =
                await createOrder(
                    successfulCorrelationId,
                    "e2e-success-customer",
                    [
                        {
                            productId:
                                "washing-machine-01",
                            quantity: 1,
                            unitPrice: 749.95
                        }
                    ]
                );

            assert.equal(
                successfulOrder.status,
                "Created"
            );

            assert.equal(
                successfulOrder.correlationId,
                successfulCorrelationId
            );

            assert.equal(
                successfulOrder.totalAmount,
                749.95
            );

            const deliveryNotification =
                await waitForNotification(
                    successfulCorrelationId,
                    "DeliveryBooked",
                    15_000
                );

            assert.equal(
                deliveryNotification.orderId,
                successfulOrder.orderId
            );

            assert.equal(
                deliveryNotification.correlationId,
                successfulCorrelationId
            );

            assert.match(
                deliveryNotification.message,
                /Delivery has been booked/
            );

            const stockAfterSuccessfulOrder =
                await getJson<StockResponse>(
                    `${inventoryServiceUrl}/stock`
                );

            assert.equal(
                getRequiredStock(
                    stockAfterSuccessfulOrder,
                    "washing-machine-01"
                ),
                initialWashingMachineStock - 1
            );

            const failedCorrelationId =
                `e2e-failure-${randomUUID()}`;

            const failedOrder =
                await createOrder(
                    failedCorrelationId,
                    "e2e-failure-customer",
                    [
                        {
                            productId:
                                "dryer-01",
                            quantity:
                                initialDryerStock + 100,
                            unitPrice: 599.95
                        }
                    ]
                );

            assert.equal(
                failedOrder.status,
                "Created"
            );

            assert.equal(
                failedOrder.correlationId,
                failedCorrelationId
            );

            const failureNotification =
                await waitForNotification(
                    failedCorrelationId,
                    "InventoryReservationFailed",
                    15_000
                );

            assert.equal(
                failureNotification.orderId,
                failedOrder.orderId
            );

            assert.equal(
                failureNotification.correlationId,
                failedCorrelationId
            );

            assert.match(
                failureNotification.message,
                /Inventory reservation failed/
            );

            const stockAfterFailedOrder =
                await getJson<StockResponse>(
                    `${inventoryServiceUrl}/stock`
                );

            assert.equal(
                getRequiredStock(
                    stockAfterFailedOrder,
                    "dryer-01"
                ),
                initialDryerStock
            );

            assert.equal(
                getRequiredStock(
                    stockAfterFailedOrder,
                    "washing-machine-01"
                ),
                initialWashingMachineStock - 1
            );
        } catch (error) {
            throw new Error(
                "CommerceFlow end-to-end test failed." +
                "\n\n" +
                formatServiceLogs(
                    runningServices
                ),
                {
                    cause: error
                }
            );
        } finally {
            for (
                const runningService of
                [...runningServices].reverse()
            ) {
                await stopService(
                    runningService
                );
            }
        }
    }
);

function startService(
    definition: ServiceDefinition
): RunningService {
    const standardOutput: string[] = [];
    const standardError: string[] = [];

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
                cwd: repositoryRoot,
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
                windowsHide: true
            }
        );

    serviceProcess.stdout?.setEncoding(
        "utf8"
    );

    serviceProcess.stderr?.setEncoding(
        "utf8"
    );

    serviceProcess.stdout?.on(
        "data",
        (chunk: string) => {
            standardOutput.push(chunk);
        }
    );

    serviceProcess.stderr?.on(
        "data",
        (chunk: string) => {
            standardError.push(chunk);
        }
    );

    return {
        definition,
        process: serviceProcess,
        standardOutput,
        standardError
    };
}

async function waitForServiceHealth(
    runningService: RunningService,
    timeoutInMs: number
): Promise<void> {
    const deadline =
        Date.now() + timeoutInMs;

    let lastError: unknown;

    const healthUrl =
        `http://127.0.0.1:` +
        `${runningService.definition.port}` +
        `/health`;

    while (Date.now() < deadline) {
        if (
            hasProcessExited(
                runningService.process
            )
        ) {
            throw new Error(
                `${runningService.definition.name} ` +
                `exited before becoming healthy.`
            );
        }

        try {
            const response =
                await fetch(healthUrl);

            if (response.ok) {
                const health =
                    await response.json() as HealthResponse;

                if (
                    health.status === "Healthy"
                ) {
                    return;
                }
            }
        } catch (error) {
            lastError = error;
        }

        await delay(200);
    }

    throw new Error(
        `Timed out while waiting for ` +
        `${runningService.definition.name} ` +
        `at ${healthUrl}. ` +
        `Last error: ${getErrorMessage(lastError)}`
    );
}

async function createOrder(
    correlationId: string,
    customerId: string,
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
                body: JSON.stringify({
                    customerId,
                    items
                })
            }
        );

    const responseBody =
        await response.json();

    assert.equal(
        response.status,
        201,
        `Expected order creation to return ` +
        `HTTP 201, but received ` +
        `${response.status}: ` +
        `${JSON.stringify(responseBody)}`
    );

    return responseBody as CreatedOrderResponse;
}

async function waitForNotification(
    correlationId: string,
    expectedType: NotificationType,
    timeoutInMs: number
): Promise<CustomerNotification> {
    const deadline =
        Date.now() + timeoutInMs;

    while (Date.now() < deadline) {
        const response =
            await getJson<NotificationsResponse>(
                `${notificationServiceUrl}` +
                `/notifications`
            );

        const matchingNotification =
            response.notifications.find(
                notification => {
                    return (
                        notification
                            .correlationId ===
                        correlationId &&
                        notification.type ===
                        expectedType
                    );
                }
            );

        if (matchingNotification) {
            return matchingNotification;
        }

        await delay(200);
    }

    throw new Error(
        `Timed out while waiting for ` +
        `${expectedType} notification with ` +
        `correlationId '${correlationId}'.`
    );
}

async function getJson<T>(
    url: string
): Promise<T> {
    const response =
        await fetch(url);

    if (!response.ok) {
        throw new Error(
            `GET ${url} returned ` +
            `HTTP ${response.status}.`
        );
    }

    return await response.json() as T;
}

function getRequiredStock(
    response: StockResponse,
    productId: string
): number {
    const stock =
        response.stock[productId];

    assert.equal(
        typeof stock,
        "number",
        `Expected stock for '${productId}'.`
    );

    return stock;
}

async function stopService(
    runningService: RunningService
): Promise<void> {
    const serviceProcess =
        runningService.process;

    if (hasProcessExited(serviceProcess)) {
        return;
    }

    const exitPromise =
        once(
            serviceProcess,
            "exit"
        ).catch(() => undefined);

    try {
        serviceProcess.kill("SIGINT");
    } catch {
        return;
    }

    await Promise.race([
        exitPromise,
        delay(3_000)
    ]);

    if (!hasProcessExited(serviceProcess)) {
        try {
            serviceProcess.kill(
                "SIGKILL"
            );
        } catch {
            return;
        }

        await Promise.race([
            exitPromise,
            delay(1_000)
        ]);
    }
}

function hasProcessExited(
    serviceProcess: ChildProcess
): boolean {
    return (
        serviceProcess.exitCode !== null ||
        serviceProcess.signalCode !== null
    );
}

function formatServiceLogs(
    runningServices:
        readonly RunningService[]
): string {
    if (runningServices.length === 0) {
        return "No services were started.";
    }

    return runningServices
        .map(runningService => {
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
                standardOutput || "(empty)",
                `--- ${runningService.definition.name} stderr ---`,
                standardError || "(empty)"
            ].join("\n");
        })
        .join("\n\n");
}

function delay(
    milliseconds: number
): Promise<void> {
    return new Promise(resolve => {
        setTimeout(
            resolve,
            milliseconds
        );
    });
}

function getErrorMessage(
    error: unknown
): string {
    if (error instanceof Error) {
        return error.message;
    }

    return String(error);
}