import {
    Pool,
    type PoolClient
} from "pg";

const DEFAULT_DATABASE_URL =
    "postgresql://commerce_flow:" +
    "commerce_flow@" +
    "localhost:5432/" +
    "commerce_flow";

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

export async function initializeInventoryDatabase(
    pool: Pool
): Promise<void> {
    const client =
        await pool.connect();

    try {
        await client.query(
            "BEGIN"
        );

        await createInventoryStockTable(
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
