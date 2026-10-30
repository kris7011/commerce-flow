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
import {
    migrateInventoryDatabase,
    type MigrationResult
} from "./migrations.js";

export {
    DATABASE_READINESS_TIMEOUT_IN_MS,
    DEFAULT_DATABASE_URL
} from "@commerce-flow/persistence";

export interface InventoryDatabasePoolOptions {
    readonly connectionString?: string;
    readonly logger?: AppLogger;
    readonly maxConnections?: number;
}

export function createInventoryDatabasePool(
    options:
        InventoryDatabasePoolOptions = {}
): Pool {
    return createDatabasePool({
        ...options,
        applicationName:
            "inventory-service"
    });
}

export async function isInventoryDatabaseReady(
    pool: Pick<Pool, "query">,
    timeoutInMs =
        DATABASE_READINESS_TIMEOUT_IN_MS
): Promise<boolean> {
    return isDatabaseReady(
        pool,
        "SELECT 1 FROM inventory_stock LIMIT 1",
        timeoutInMs
    );
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
