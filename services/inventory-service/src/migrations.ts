import type {
    Pool,
    PoolClient
} from "pg";

/*
 * Versioned, forward-only schema migrations.
 *
 * All pending migrations run in a single transaction guarded by a
 * transaction-scoped advisory lock, so concurrently starting service
 * instances (or parallel test bootstraps) apply each migration exactly
 * once and a failure leaves the schema unchanged.
 *
 * Migrations 1 and 2 use IF NOT EXISTS because those tables were
 * created by an earlier, unversioned bootstrap; databases created
 * before versioning was introduced are adopted without error.
 * Never edit a released migration — add a new one instead.
 */

export interface Migration {
    readonly version: number;
    readonly name: string;

    up(
        client: PoolClient
    ): Promise<void>;
}

const MIGRATION_LOCK_NAME =
    "commerce-flow.inventory-schema-migrations";

export const INVENTORY_MIGRATIONS:
    readonly Migration[] = [
        {
            version: 1,
            name: "create_inventory_stock",

            async up(client): Promise<void> {
                await client.query(`
                    CREATE TABLE IF NOT EXISTS inventory_stock (
                        product_id TEXT PRIMARY KEY,
                        quantity INTEGER NOT NULL,
                        CONSTRAINT inventory_stock_quantity_nonnegative
                            CHECK (quantity >= 0)
                    )
                `);
            }
        },
        {
            version: 2,
            name: "create_inventory_outbox",

            async up(client): Promise<void> {
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
                    ON inventory_outbox (sequence_id)
                    WHERE published_at IS NULL
                `);
            }
        },
        {
            version: 3,
            name: "add_outbox_delivery_tracking_and_reservations",

            async up(client): Promise<void> {
                // available_at doubles as the claim lease: a relay
                // claims a row by pushing available_at into the
                // future, so a crashed relay's claim simply expires.
                // attempts is incremented per claim and acts as a
                // fencing token for the claimant's follow-up update.
                await client.query(`
                    ALTER TABLE inventory_outbox
                        ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0
                            CONSTRAINT inventory_outbox_attempts_nonnegative
                                CHECK (attempts >= 0),
                        ADD COLUMN available_at TIMESTAMPTZ NOT NULL
                            DEFAULT NOW(),
                        ADD COLUMN claimed_by TEXT,
                        ADD COLUMN last_error TEXT
                `);

                // One row per order: the primary key is the durable
                // idempotency guarantee for PaymentAuthorized
                // processing, independent of the message's event id.
                await client.query(`
                    CREATE TABLE inventory_reservations (
                        order_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL
                            CONSTRAINT inventory_reservations_status_valid
                                CHECK (status IN ('Reserved', 'Rejected')),
                        reservation_id UUID
                            CONSTRAINT inventory_reservations_reservation_id_unique
                                UNIQUE,
                        source_event_id TEXT NOT NULL,
                        result_event_id UUID NOT NULL
                            CONSTRAINT inventory_reservations_result_event_id_unique
                                UNIQUE,
                        items JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                            DEFAULT NOW(),
                        CONSTRAINT inventory_reservations_reservation_id_matches_status
                            CHECK (
                                (status = 'Reserved') =
                                (reservation_id IS NOT NULL)
                            )
                    )
                `);
            }
        }
    ];

export interface MigrationResult {
    readonly appliedVersions: readonly number[];
    readonly currentVersion: number;
}

export async function migrateInventoryDatabase(
    pool: Pool,
    migrations:
        readonly Migration[] =
        INVENTORY_MIGRATIONS
): Promise<MigrationResult> {
    assertMigrationsAreOrdered(
        migrations
    );

    const client =
        await pool.connect();

    try {
        await client.query(
            "BEGIN"
        );

        await client.query(
            `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`,
            [
                MIGRATION_LOCK_NAME
            ]
        );

        await client.query(`
            CREATE TABLE IF NOT EXISTS inventory_schema_migrations (
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
                `SELECT version FROM inventory_schema_migrations`
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
                    INSERT INTO inventory_schema_migrations (
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
        client.release();
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
