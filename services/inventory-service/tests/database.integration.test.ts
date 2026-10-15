import assert from "node:assert/strict";
import test from "node:test";
import {
    createInventoryDatabasePool,
    initializeInventoryDatabase
} from "../src/database.js";

interface ColumnRow {
    columnName: string;
}

interface IndexRow {
    indexDefinition: string;
}

interface RelationRow {
    relationName:
    string |
    null;
}

const pool =
    createInventoryDatabasePool();

test.before(
    async () => {
        await initializeInventoryDatabase(
            pool
        );
    }
);

test.after(
    async () => {
        await pool.end();
    }
);

test(
    "initializes the inventory outbox schema idempotently",
    async () => {
        await initializeInventoryDatabase(
            pool
        );

        await initializeInventoryDatabase(
            pool
        );

        const columnResult =
            await pool.query<ColumnRow>(
                `
                    SELECT
                        column_name AS "columnName"
                    FROM information_schema.columns
                    WHERE
                        table_schema = 'public'
                        AND table_name =
                            'inventory_outbox'
                    ORDER BY ordinal_position
                `
            );

        assert.deepEqual(
            columnResult.rows.map(
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
                "published_at"
            ]
        );

        const indexResult =
            await pool.query<IndexRow>(
                `
                    SELECT
                        indexdef AS "indexDefinition"
                    FROM pg_indexes
                    WHERE
                        schemaname = 'public'
                        AND tablename =
                            'inventory_outbox'
                        AND indexname =
                            'inventory_outbox_pending_sequence_idx'
                `
            );

        assert.equal(
            indexResult.rows.length,
            1
        );

        assert.match(
            indexResult.rows[0]
                ?.indexDefinition ??
            "",
            /published_at IS NULL/
        );
    }
);

test(
    "serializes concurrent database initialization",
    async () => {
        const secondPool =
            createInventoryDatabasePool();

        try {
            await pool.query(
                `
                    DROP TABLE IF EXISTS
                        inventory_outbox
                    CASCADE
                `
            );

            await Promise.all([
                initializeInventoryDatabase(
                    pool
                ),
                initializeInventoryDatabase(
                    secondPool
                )
            ]);

            const relationResult =
                await pool.query<RelationRow>(
                    `
                        SELECT
                            to_regclass(
                                'public.inventory_outbox'
                            )::text AS "relationName"
                    `
                );

            assert.equal(
                relationResult.rows[0]
                    ?.relationName,
                "inventory_outbox"
            );
        } finally {
            await secondPool.end();
        }
    }
);
