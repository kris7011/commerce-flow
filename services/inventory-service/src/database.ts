import {
    Pool,
    type PoolClient
} from "pg";

const DEFAULT_DATABASE_URL =
    "postgresql://commerce_flow:" +
    "commerce_flow@" +
    "localhost:5432/" +
    "commerce_flow";

const DATABASE_INITIALIZATION_LOCK_NAME =
    "commerce-flow.inventory-database-bootstrap";

export function createInventoryDatabasePool(
    connectionString:
        string =
        process.env.DATABASE_URL ??
        DEFAULT_DATABASE_URL
): Pool {
    return new Pool({
        connectionString
    });
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

export async function initializeInventoryDatabase(
    pool: Pool
): Promise<void> {
    const client =
        await pool.connect();

    try {
        await client.query(
            "BEGIN"
        );

        await acquireInventoryDatabaseInitializationLock(
            client
        );

        await createInventoryStockTable(
            client
        );

        await createInventoryOutboxTable(
            client
        );

        await seedInitialInventoryStock(
            client
        );

        await client.query(
            "COMMIT"
        );
    } catch (error) {
        try {
            await client.query(
                "ROLLBACK"
            );
        } catch {
            // Preserve the original error.
        }

        throw error;
    } finally {
        client.release();
    }
}

async function createInventoryStockTable(
    client: PoolClient
): Promise<void> {
    await client.query(`
        CREATE TABLE IF NOT EXISTS inventory_stock (
            product_id TEXT PRIMARY KEY,
            quantity INTEGER NOT NULL,
            CONSTRAINT inventory_stock_quantity_nonnegative
                CHECK (quantity >= 0)
        )
    `);
}

async function acquireInventoryDatabaseInitializationLock(
    client: PoolClient
): Promise<void> {
    await client.query(
        `
            SELECT pg_advisory_xact_lock(
                hashtext($1::text)::bigint
            )
        `,
        [
            DATABASE_INITIALIZATION_LOCK_NAME
        ]
    );
}

async function createInventoryOutboxTable(
    client: PoolClient
): Promise<void> {
    await client.query(`
        CREATE TABLE IF NOT EXISTS inventory_outbox (
            sequence_id BIGINT
                GENERATED ALWAYS AS IDENTITY
                PRIMARY KEY,

            event_id UUID NOT NULL,

            event_type TEXT NOT NULL,

            routing_key TEXT NOT NULL,

            payload JSONB NOT NULL,

            occurred_at TIMESTAMPTZ NOT NULL,

            created_at TIMESTAMPTZ NOT NULL
                DEFAULT NOW(),

            published_at TIMESTAMPTZ,

            CONSTRAINT inventory_outbox_event_id_unique
                UNIQUE (event_id)
        )
    `);

    await client.query(`
        CREATE INDEX IF NOT EXISTS
            inventory_outbox_pending_sequence_idx
        ON inventory_outbox (
            sequence_id
        )
        WHERE published_at IS NULL
    `);
}

async function seedInitialInventoryStock(
    client: PoolClient
): Promise<void> {
    await client.query(
        `
            INSERT INTO inventory_stock (
                product_id,
                quantity
            )
            VALUES
                ($1, $2),
                ($3, $4),
                ($5, $6)
            ON CONFLICT (product_id)
            DO NOTHING
        `,
        [
            "washing-machine-01",
            10,
            "dishwasher-01",
            5,
            "dryer-01",
            3
        ]
    );
}
