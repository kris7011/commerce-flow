import type {
    Pool,
    PoolClient
} from "pg";

/*
 * Versioned, forward-only schema migrations, shared by every service
 * that owns tables.
 *
 * All pending migrations run in a single transaction guarded by a
 * transaction-scoped advisory lock, so concurrently starting service
 * instances (or parallel test bootstraps) apply each migration exactly
 * once and a failure leaves the schema unchanged. Each service keeps
 * its own history table and lock name, so services migrate
 * independently even when they share a PostgreSQL database.
 *
 * Never edit a released migration — add a new one instead.
 */

export interface Migration {
    readonly version: number;
    readonly name: string;

    up(
        client: PoolClient
    ): Promise<void>;
}

export interface MigrationResult {
    readonly appliedVersions: readonly number[];
    readonly currentVersion: number;
}

export interface MigrationPlan {
    /** Table that records the applied versions, e.g. `order_schema_migrations`. */
    readonly historyTable: string;

    /** Name hashed into the advisory lock that serialises migrations. */
    readonly lockName: string;

    readonly migrations: readonly Migration[];
}

const TABLE_NAME_PATTERN =
    /^[a-z_][a-z0-9_]{0,62}$/;

export async function migrateDatabase(
    pool: Pool,
    plan: MigrationPlan
): Promise<MigrationResult> {
    const {
        historyTable,
        lockName,
        migrations
    } = plan;

    if (!TABLE_NAME_PATTERN.test(historyTable)) {
        throw new Error(
            `Invalid migration history table name '${historyTable}'.`
        );
    }

    assertMigrationsAreOrdered(
        migrations
    );

    const client =
        await pool.connect();

    let connectionError:
        Error | undefined;

    const onConnectionError =
        (error: Error): void => {
            connectionError =
                error;
        };

    client.on(
        "error",
        onConnectionError
    );

    try {
        await client.query(
            "BEGIN"
        );

        await client.query(
            `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`,
            [
                lockName
            ]
        );

        await client.query(`
            CREATE TABLE IF NOT EXISTS ${historyTable} (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL
                    DEFAULT NOW()
            )
        `);

        const appliedResult =
            await client.query<{
                version: number;
            }>(
                `SELECT version FROM ${historyTable}`
            );

        const alreadyApplied =
            new Set(
                appliedResult.rows.map(
                    row =>
                        row.version
                )
            );

        const appliedVersions:
            number[] = [];

        for (const migration of migrations) {
            if (
                alreadyApplied.has(
                    migration.version
                )
            ) {
                continue;
            }

            await migration.up(
                client
            );

            await client.query(
                `
                    INSERT INTO ${historyTable} (
                        version,
                        name
                    )
                    VALUES ($1, $2)
                `,
                [
                    migration.version,
                    migration.name
                ]
            );

            appliedVersions.push(
                migration.version
            );
        }

        await client.query(
            "COMMIT"
        );

        return {
            appliedVersions,
            currentVersion:
                Math.max(
                    0,
                    ...alreadyApplied,
                    ...appliedVersions
                )
        };
    } catch (error) {
        await client
            .query(
                "ROLLBACK"
            )
            .catch(
                () => undefined
            );

        throw error;
    } finally {
        client.off(
            "error",
            onConnectionError
        );

        client.release(
            connectionError
        );
    }
}

function assertMigrationsAreOrdered(
    migrations: readonly Migration[]
): void {
    migrations.forEach(
        (migration, index) => {
            if (
                migration.version !==
                index + 1
            ) {
                throw new Error(
                    `Migration '${migration.name}' has version ` +
                    `${migration.version}; expected ${index + 1}. ` +
                    `Migrations must be contiguous and ordered.`
                );
            }
        }
    );
}
