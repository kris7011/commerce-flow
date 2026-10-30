import type {
    Pool
} from "pg";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    createDatabasePool,
    DATABASE_READINESS_TIMEOUT_IN_MS,
    isDatabaseReady
} from "@commerce-flow/persistence";

export {
    DEFAULT_DATABASE_URL
} from "@commerce-flow/persistence";

export const ORDER_OUTBOX_TABLE =
    "order_outbox";

export interface OrderDatabasePoolOptions {
    readonly connectionString?: string;
    readonly logger?: AppLogger;
    readonly maxConnections?: number;
}

export function createOrderDatabasePool(
    options:
        OrderDatabasePoolOptions = {}
): Pool {
    return createDatabasePool({
        ...options,
        applicationName:
            "order-service"
    });
}

export async function isOrderDatabaseReady(
    pool: Pick<Pool, "query">,
    timeoutInMs =
        DATABASE_READINESS_TIMEOUT_IN_MS
): Promise<boolean> {
    return isDatabaseReady(
        pool,
        "SELECT 1 FROM orders LIMIT 1",
        timeoutInMs
    );
}
