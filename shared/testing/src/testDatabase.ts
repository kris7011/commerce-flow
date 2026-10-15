import {
    randomBytes
} from "node:crypto";
import pg from "pg";

/*
 * Every integration and end-to-end test run gets its own, freshly
 * created PostgreSQL database. Tests therefore never share schema or
 * data, can run in parallel and never touch the development database.
 *
 * Destructive helpers refuse to operate on any database whose name
 * does not carry the dedicated test prefix.
 */

export const TEST_DATABASE_PREFIX =
    "commerce_flow_test_";

const TEST_DATABASE_NAME_PATTERN =
    /^commerce_flow_test_[a-z0-9]+_[a-f0-9]{8}$/;

const DEFAULT_ADMIN_DATABASE_URL =
    "postgresql://commerce_flow:commerce_flow@localhost:5432/postgres";

/** Test databases older than this are treated as leftovers of crashed runs. */
const STALE_TEST_DATABASE_AGE_IN_MS =
    6 * 60 * 60 * 1000;

export interface TestDatabase {
    readonly name: string;
    readonly connectionString: string;

    createPool(
        config?: Omit<pg.PoolConfig, "connectionString">
    ): pg.Pool;

    /** Drops the database, terminating any remaining connections. */
    drop(): Promise<void>;
}

export interface CreateTestDatabaseOptions {
    readonly adminConnectionString?: string;
}

export function getAdminDatabaseUrl(): string {
    return (
        process.env.TEST_DATABASE_ADMIN_URL ??
        DEFAULT_ADMIN_DATABASE_URL
    );
}

export function isTestDatabaseName(
    name: string
): boolean {
    return TEST_DATABASE_NAME_PATTERN.test(
        name
    );
}

export function assertTestDatabaseName(
    name: string
): void {
    if (!isTestDatabaseName(name)) {
        throw new Error(
            `Refusing to run a destructive operation against ` +
            `'${name}': it is not a dedicated test database ` +
            `(expected the '${TEST_DATABASE_PREFIX}' prefix).`
        );
    }
}

/**
 * Verifies, on the live connection, that the current database is a
 * dedicated test database. Call this before truncating or deleting
 * data so a misconfigured connection string cannot destroy real data.
 */
export async function assertConnectedToTestDatabase(
    queryable: Pick<pg.Pool, "query">
): Promise<void> {
    const result =
        await queryable.query<{
            databaseName: string;
        }>(
            `SELECT current_database() AS "databaseName"`
        );

    const databaseName =
        result.rows[0]?.databaseName ??
        "";

    assertTestDatabaseName(
        databaseName
    );
}

export async function createTestDatabase(
    options: CreateTestDatabaseOptions = {}
): Promise<TestDatabase> {
    const adminConnectionString =
        options.adminConnectionString ??
        getAdminDatabaseUrl();

    const name =
        `${TEST_DATABASE_PREFIX}` +
        `${Date.now().toString(36)}_` +
        `${randomBytes(4).toString("hex")}`;

    assertTestDatabaseName(
        name
    );

    await withAdminClient(
        adminConnectionString,
        async client => {
            await dropStaleTestDatabases(
                client
            );

            await client.query(
                `CREATE DATABASE ${quoteIdentifier(name)}`
            );
        }
    );

    const connectionString =
        replaceDatabaseName(
            adminConnectionString,
            name
        );

    const pools:
        pg.Pool[] = [];

    let dropped =
        false;

    return {
        name,
        connectionString,

        createPool(
            config = {}
        ): pg.Pool {
            const pool =
                new pg.Pool({
                    ...config,
                    connectionString
                });

            // Connections are terminated when the database is
            // dropped; that must not crash the test process.
            pool.on(
                "error",
                () => undefined
            );

            pools.push(
                pool
            );

            return pool;
        },

        async drop(): Promise<void> {
            if (dropped) {
                return;
            }

            dropped =
                true;

            await Promise.allSettled(
                pools.map(
                    pool =>
                        pool.ended
                            ? Promise.resolve()
                            : pool.end()
                )
            );

            await withAdminClient(
                adminConnectionString,
                async client => {
                    await dropTestDatabase(
                        client,
                        name
                    );
                }
            );
        }
    };
}

async function dropTestDatabase(
    client: pg.Client,
    name: string
): Promise<void> {
    assertTestDatabaseName(
        name
    );

    await client.query(
        `DROP DATABASE IF EXISTS ${quoteIdentifier(name)} WITH (FORCE)`
    );
}

async function dropStaleTestDatabases(
    client: pg.Client
): Promise<void> {
    const result =
        await client.query<{
            name: string;
        }>(
            `
                SELECT datname AS name
                FROM pg_database
                WHERE datname LIKE $1
            `,
            [
                `${TEST_DATABASE_PREFIX}%`
            ]
        );

    const now =
        Date.now();

    for (const { name } of result.rows) {
        if (!isTestDatabaseName(name)) {
            continue;
        }

        const createdAt =
            Number.parseInt(
                name
                    .slice(
                        TEST_DATABASE_PREFIX.length
                    )
                    .split("_")[0] ??
                "",
                36
            );

        if (
            Number.isFinite(createdAt) &&
            now - createdAt >
            STALE_TEST_DATABASE_AGE_IN_MS
        ) {
            await dropTestDatabase(
                client,
                name
            ).catch(
                () => undefined
            );
        }
    }
}

async function withAdminClient<T>(
    connectionString: string,
    action: (
        client: pg.Client
    ) => Promise<T>
): Promise<T> {
    const client =
        new pg.Client({
            connectionString,
            connectionTimeoutMillis:
                10_000
        });

    await client.connect();

    try {
        return await action(
            client
        );
    } finally {
        await client.end();
    }
}

function replaceDatabaseName(
    connectionString: string,
    databaseName: string
): string {
    const url =
        new URL(
            connectionString
        );

    url.pathname =
        `/${databaseName}`;

    return url.toString();
}

function quoteIdentifier(
    identifier: string
): string {
    return `"${identifier.replaceAll('"', '""')}"`;
}
