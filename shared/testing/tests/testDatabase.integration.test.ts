import assert from "node:assert/strict";
import test from "node:test";
import pg from "pg";
import {
    assertConnectedToTestDatabase,
    createTestDatabase,
    getAdminDatabaseUrl
} from "../src/testDatabase.js";

test(
    "creates an isolated database and removes it again",
    async () => {
        const database =
            await createTestDatabase();

        try {
            const pool =
                database.createPool();

            await assertConnectedToTestDatabase(
                pool
            );

            await pool.query(
                "CREATE TABLE isolation_probe (id INTEGER)"
            );
        } finally {
            await database.drop();
        }

        const admin =
            new pg.Client({
                connectionString:
                    getAdminDatabaseUrl()
            });

        await admin.connect();

        try {
            const result =
                await admin.query(
                    "SELECT 1 FROM pg_database WHERE datname = $1",
                    [
                        database.name
                    ]
                );

            assert.equal(
                result.rowCount,
                0
            );
        } finally {
            await admin.end();
        }
    }
);

test(
    "gives concurrent callers separate databases",
    async () => {
        const databases =
            await Promise.all([
                createTestDatabase(),
                createTestDatabase()
            ]);

        try {
            assert.notEqual(
                databases[0].name,
                databases[1].name
            );
        } finally {
            await Promise.all(
                databases.map(
                    database =>
                        database.drop()
                )
            );
        }
    }
);
