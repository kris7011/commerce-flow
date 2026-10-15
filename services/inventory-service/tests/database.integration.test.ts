import assert from "node:assert/strict";
import test from "node:test";
import {
    createTestDatabase,
    type TestDatabase
} from "@commerce-flow/testing";
import {
    initializeInventoryDatabase,
    seedDemoInventoryStock
} from "../src/database.js";
import {
    INVENTORY_MIGRATIONS,
    migrateInventoryDatabase,
    type Migration
} from "../src/migrations.js";

const latestVersion =
    INVENTORY_MIGRATIONS.length;

async function withFreshDatabase(
    action: (
        database: TestDatabase
    ) => Promise<void>
): Promise<void> {
    const database =
        await createTestDatabase();

    try {
        await action(
            database
        );
    } finally {
        await database.drop();
    }
}

test(
    "applies all migrations once and is idempotent",
    async () => {
        await withFreshDatabase(
            async database => {
                const pool =
                    database.createPool();

                const first =
                    await migrateInventoryDatabase(
                        pool
                    );

                const second =
                    await migrateInventoryDatabase(
                        pool
                    );

                assert.deepEqual(
                    first.appliedVersions,
                    INVENTORY_MIGRATIONS.map(
                        migration =>
                            migration.version
                    )
                );

                assert.deepEqual(
                    second.appliedVersions,
                    []
                );

                assert.equal(
                    second.currentVersion,
                    latestVersion
                );

                const columns =
                    await pool.query<{
                        columnName: string;
                    }>(
                        `
                            SELECT column_name AS "columnName"
                            FROM information_schema.columns
                            WHERE table_name = 'inventory_outbox'
                            ORDER BY ordinal_position
                        `
                    );

                assert.deepEqual(
                    columns.rows.map(
                        row =>
                            row.columnName
                    ),
                    [
                        "sequence_id",
                        "event_id",
                        "event_type",
                        "routing_key",
                        "payload",
                        "occurred_at",
                        "created_at",
                        "published_at",
                        "attempts",
                        "available_at",
                        "claimed_by",
                        "last_error"
                    ]
                );

                const reservationTable =
                    await pool.query<{
                        relationName: string | null;
                    }>(
                        `SELECT to_regclass('public.inventory_reservations')::text AS "relationName"`
                    );

                assert.equal(
                    reservationTable.rows[0]?.relationName,
                    "inventory_reservations"
                );
            }
        );
    }
);

test(
    "serializes concurrent migrations so each version is applied exactly once",
    async () => {
        await withFreshDatabase(
            async database => {
                const pools =
                    Array.from(
                        {
                            length: 4
                        },
                        () =>
                            database.createPool()
                    );

                const results =
                    await Promise.all(
                        pools.map(
                            pool =>
                                initializeInventoryDatabase(
                                    pool
                                )
                        )
                    );

                const appliedVersions =
                    results
                        .flatMap(
                            result =>
                                result.appliedVersions
                        )
                        .sort();

                assert.deepEqual(
                    appliedVersions,
                    INVENTORY_MIGRATIONS.map(
                        migration =>
                            migration.version
                    )
                );

                const recorded =
                    await pools[0]!.query<{
                        count: string;
                    }>(
                        `SELECT count(*) AS count FROM inventory_schema_migrations`
                    );

                assert.equal(
                    Number(
                        recorded.rows[0]?.count
                    ),
                    latestVersion
                );
            }
        );
    }
);

test(
    "adopts a database created by the earlier unversioned bootstrap",
    async () => {
        await withFreshDatabase(
            async database => {
                const pool =
                    database.createPool();

                // Schema exactly as created before migrations existed.
                await pool.query(`
                    CREATE TABLE inventory_stock (
                        product_id TEXT PRIMARY KEY,
                        quantity INTEGER NOT NULL,
                        CONSTRAINT inventory_stock_quantity_nonnegative
                            CHECK (quantity >= 0)
                    );

                    CREATE TABLE inventory_outbox (
                        sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        event_id UUID NOT NULL,
                        event_type TEXT NOT NULL,
                        routing_key TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        occurred_at TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        published_at TIMESTAMPTZ,
                        CONSTRAINT inventory_outbox_event_id_unique UNIQUE (event_id)
                    );

                    INSERT INTO inventory_stock VALUES ('dryer-01', 1);

                    INSERT INTO inventory_outbox (
                        event_id, event_type, routing_key, payload, occurred_at
                    )
                    VALUES (
                        gen_random_uuid(), 'InventoryReserved',
                        'inventory.reserved', '{}'::jsonb, NOW()
                    );
                `);

                await initializeInventoryDatabase(
                    pool
                );

                const stock =
                    await pool.query<{
                        quantity: number;
                    }>(
                        `SELECT quantity FROM inventory_stock WHERE product_id = 'dryer-01'`
                    );

                assert.equal(
                    stock.rows[0]?.quantity,
                    1,
                    "Existing stock must not be overwritten by seeding."
                );

                const outbox =
                    await pool.query<{
                        attempts: number;
                        publishedAt: Date | null;
                    }>(
                        `
                            SELECT
                                attempts,
                                published_at AS "publishedAt"
                            FROM inventory_outbox
                        `
                    );

                assert.deepEqual(
                    outbox.rows,
                    [
                        {
                            attempts: 0,
                            publishedAt: null
                        }
                    ],
                    "Pending legacy events must remain publishable."
                );
            }
        );
    }
);

test(
    "rolls back every pending migration when one fails",
    async () => {
        await withFreshDatabase(
            async database => {
                const pool =
                    database.createPool();

                const failingMigration:
                    Migration = {
                        version:
                            latestVersion + 1,
                        name:
                            "failing_migration",

                        async up(client): Promise<void> {
                            await client.query(
                                "CREATE TABLE should_not_exist (id INTEGER)"
                            );

                            throw new Error(
                                "Simulated migration failure"
                            );
                        }
                    };

                await assert.rejects(
                    migrateInventoryDatabase(
                        pool,
                        [
                            ...INVENTORY_MIGRATIONS,
                            failingMigration
                        ]
                    ),
                    /Simulated migration failure/
                );

                const relations =
                    await pool.query<{
                        name: string | null;
                    }>(
                        `
                            SELECT to_regclass(name)::text AS name
                            FROM UNNEST(ARRAY[
                                'public.inventory_stock',
                                'public.should_not_exist',
                                'public.inventory_schema_migrations'
                            ]) AS name
                        `
                    );

                assert.deepEqual(
                    relations.rows.map(
                        row =>
                            row.name
                    ),
                    [
                        null,
                        null,
                        null
                    ]
                );
            }
        );
    }
);

test(
    "rejects migrations that are not contiguous",
    async () => {
        await withFreshDatabase(
            async database => {
                await assert.rejects(
                    migrateInventoryDatabase(
                        database.createPool(),
                        [
                            INVENTORY_MIGRATIONS[0]!,
                            INVENTORY_MIGRATIONS[2]!
                        ]
                    ),
                    /must be contiguous/
                );
            }
        );
    }
);

test(
    "seeds demo stock without overwriting existing quantities",
    async () => {
        await withFreshDatabase(
            async database => {
                const pool =
                    database.createPool();

                await initializeInventoryDatabase(
                    pool
                );

                await pool.query(
                    `UPDATE inventory_stock SET quantity = 0 WHERE product_id = 'dryer-01'`
                );

                await seedDemoInventoryStock(
                    pool
                );

                const result =
                    await pool.query<{
                        productId: string;
                        quantity: number;
                    }>(
                        `
                            SELECT product_id AS "productId", quantity
                            FROM inventory_stock
                            ORDER BY product_id
                        `
                    );

                assert.deepEqual(
                    result.rows,
                    [
                        {
                            productId: "dishwasher-01",
                            quantity: 5
                        },
                        {
                            productId: "dryer-01",
                            quantity: 0
                        },
                        {
                            productId: "washing-machine-01",
                            quantity: 10
                        }
                    ]
                );
            }
        );
    }
);
