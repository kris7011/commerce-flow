import {
    ConfigurationError,
    readInteger,
    readPort,
    readUrl,
    type Environment
} from "@commerce-flow/runtime";
import {
    DEVELOPMENT_RABBITMQ_URL
} from "@commerce-flow/messaging";
import {
    DEFAULT_DATABASE_URL
} from "./database.js";

export interface OrderConfig {
    readonly port: number;
    readonly databaseUrl: string;
    readonly rabbitMqUrl: string;
    readonly publishTimeoutInMs: number;
    readonly outbox: {
        readonly batchSize: number;
        readonly pollIntervalInMs: number;
        readonly leaseInMs: number;
    };
}

export function readOrderConfig(
    environment: Environment
): OrderConfig {
    const publishTimeoutInMs =
        readInteger(
            environment,
            "RABBITMQ_PUBLISH_TIMEOUT_MS",
            10_000,
            {
                min: 100,
                max: 120_000
            }
        );

    const leaseInMs =
        readInteger(
            environment,
            "OUTBOX_LEASE_MS",
            30_000,
            {
                min: 1_000,
                max: 600_000
            }
        );

    // A lease shorter than a slow publish would let a second relay
    // claim and publish the same event while the first still waits.
    if (leaseInMs < publishTimeoutInMs * 2) {
        throw new ConfigurationError(
            "OUTBOX_LEASE_MS must be at least twice " +
            "RABBITMQ_PUBLISH_TIMEOUT_MS."
        );
    }

    return {
        port:
            readPort(
                environment,
                "ORDER_SERVICE_PORT",
                3001
            ),
        databaseUrl:
            readUrl(
                environment,
                "DATABASE_URL",
                DEFAULT_DATABASE_URL,
                [
                    "postgres:",
                    "postgresql:"
                ]
            ),
        rabbitMqUrl:
            readUrl(
                environment,
                "RABBITMQ_URL",
                DEVELOPMENT_RABBITMQ_URL,
                [
                    "amqp:",
                    "amqps:"
                ]
            ),
        publishTimeoutInMs,
        outbox: {
            batchSize:
                readInteger(
                    environment,
                    "OUTBOX_BATCH_SIZE",
                    50,
                    {
                        min: 1,
                        max: 1_000
                    }
                ),
            pollIntervalInMs:
                readInteger(
                    environment,
                    "OUTBOX_POLL_INTERVAL_MS",
                    1_000,
                    {
                        min: 10,
                        max: 60_000
                    }
                ),
            leaseInMs
        }
    };
}
