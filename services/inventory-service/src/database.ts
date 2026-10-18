import {
    Pool
} from "pg";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    migrateInventoryDatabase,
    type MigrationResult
} from "./migrations.js";

/** Development-only default; production must set DATABASE_URL. */
export const DEFAULT_DATABASE_URL =
    "postgresql://commerce_flow:" +
    "commerce_flow@" +
    "localhost:5432/" +
    "commerce_flow";

export interface InventoryDatabasePoolOptions {
    readonly connectionString?: string;
    readonly logger?: AppLogger;
    readonly maxConnections?: number;
}

export function createInventoryDatabasePool(
    options:
        InventoryDatabasePoolOptions = {}
): Pool {
    const pool =
        new Pool({
            connectionString:
                options.connectionString ??
                process.env.DATABASE_URL ??
                DEFAULT_DATABASE_URL,
            application_name:
                "inventory-service",
            max:
                options.maxConnections ??
                10,
            // Fail fast while PostgreSQL is unreachable instead of
            // queueing requests indefinitely.
            connectionTimeoutMillis:
                5_000,
            idleTimeoutMillis:
                30_000,
            statement_timeout:
                15_000
        });

    // An idle client can fail (e.g. when PostgreSQL restarts).
    // Without a listener that error would crash the process; the
    // pool discards the client and reconnects on the next query.
    pool.on(
        "error",
        error => {
            options.logger?.warn(
                "Idle PostgreSQL client failed",
                {
                    errorMessage:
                        error.message
                }
            );
        }
    );

    return pool;
}

/**
 * Upper bound for the readiness query. While PostgreSQL is unreachable
 * a query waits for the pool's connection timeout (5 s); a readiness
 * probe must answer well within typical probe timeouts (about 1 s)
 * instead.
 */
export const DATABASE_READINESS_TIMEOUT_IN_MS =
    1_000;

export async function isInventoryDatabaseReady(
    pool: Pick<Pool, "query">,
    timeoutInMs =
        DATABASE_READINESS_TIMEOUT_IN_MS
): Promise<boolean> {
    let timer:
        NodeJS.Timeout | undefined;

    const timedOut =
        new Promise<false>(resolve => {
            timer =
                setTimeout(
                    () => resolve(false),
                    timeoutInMs
                );
        });

    // A query that outlives the timeout still finishes in the
    // background (bounded by the pool's own timeouts); its result is
    // ignored.
    const query =
        pool.query(
            `
                SELECT 1
                FROM inventory_stock
                LIMIT 1
            `
        ).then(
            () => true,
            () => false
        );

    try {
        return await Promise.race([
            query,
            timedOut
        ]);
    } finally {
        clearTimeout(timer);
    }
}

/**
 * Applies pending schema migrations and seeds the demo catalogue.
 * Safe to call concurrently from several processes.
 */
export async function initializeInventoryDatabase(
    pool: Pool
): Promise<MigrationResult> {
    const result =
        await migrateInventoryDatabase(
            pool
        );

    await seedDemoInventoryStock(
        pool
    );

    return result;
}

export const DEMO_INVENTORY_STOCK:
    Readonly<Record<string, number>> = {
        "washing-machine-01": 10,
        "dishwasher-01": 5,
        "dryer-01": 3
    };

/**
 * Inserts the demo products if they do not exist yet. Existing
 * quantities are never overwritten.
 */
export async function seedDemoInventoryStock(
    pool: Pool,
    stock:
        Readonly<Record<string, number>> =
        DEMO_INVENTORY_STOCK
): Promise<void> {
    const entries =
        Object.entries(
            stock
        );

    await pool.query(
        `
            INSERT INTO inventory_stock (
                product_id,
                quantity
            )
            SELECT *
            FROM UNNEST(
                $1::text[],
                $2::integer[]
            )
            ON CONFLICT (product_id)
            DO NOTHING
        `,
        [
            entries.map(
                ([productId]) =>
                    productId
            ),
            entries.map(
                ([, quantity]) =>
                    quantity
            )
        ]
    );
}
