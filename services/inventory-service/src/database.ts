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

export async function isInventoryDatabaseReady(
    pool: Pool
): Promise<boolean> {
    try {
        await pool.query(
            `
                SELECT 1
                FROM inventory_stock
                LIMIT 1
            `
        );

        return true;
    } catch {
        return false;
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
