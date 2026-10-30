import type {
    Pool
} from "pg";
import {
    migrateDatabase,
    type Migration,
    type MigrationResult
} from "@commerce-flow/persistence";

/*
 * Versioned, forward-only schema migrations.
 *
 * The runner (one transaction, advisory lock, history table) lives in
 * @commerce-flow/persistence; this file only lists Inventory's
 * migrations.
 *
 * Migrations 1 and 2 use IF NOT EXISTS because those tables were
 * created by an earlier, unversioned bootstrap; databases created
 * before versioning was introduced are adopted without error.
 * Never edit a released migration — add a new one instead.
 */

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

export type {
    Migration,
    MigrationResult
};

export async function migrateInventoryDatabase(
    pool: Pool,
    migrations:
        readonly Migration[] =
        INVENTORY_MIGRATIONS
): Promise<MigrationResult> {
    return migrateDatabase(
        pool,
        {
            historyTable:
                "inventory_schema_migrations",
            lockName:
                MIGRATION_LOCK_NAME,
            migrations
        }
    );
}
