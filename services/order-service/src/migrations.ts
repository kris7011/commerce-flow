import type {
    Pool
} from "pg";
import {
    migrateDatabase,
    type Migration,
    type MigrationResult
} from "@commerce-flow/persistence";

/*
 * Order Service schema. The service owns its tables (prefixed `order`)
 * and its own migration history, so it migrates independently of the
 * Inventory Service even when both use the same PostgreSQL database.
 *
 * Never edit a released migration — add a new one instead.
 */

const MIGRATION_LOCK_NAME =
    "commerce-flow.order-schema-migrations";

export const ORDER_MIGRATIONS:
    readonly Migration[] = [
        {
            version: 1,
            name: "create_orders_timeline_and_outbox",

            async up(client): Promise<void> {
                // The unique idempotency key is the durable guarantee
                // that a retried submission cannot create a second
                // order, whichever instance or connection handles it.
                await client.query(`
                    CREATE TABLE orders (
                        order_id UUID PRIMARY KEY,
                        idempotency_key TEXT NOT NULL
                            CONSTRAINT orders_idempotency_key_unique
                                UNIQUE,
                        request_fingerprint TEXT NOT NULL,
                        customer_id TEXT NOT NULL,
                        items JSONB NOT NULL,
                        total_amount NUMERIC NOT NULL
                            CONSTRAINT orders_total_amount_nonnegative
                                CHECK (total_amount >= 0),
                        correlation_id TEXT NOT NULL,
                        created_event_id UUID NOT NULL
                            CONSTRAINT orders_created_event_id_unique
                                UNIQUE,
                        status TEXT NOT NULL
                            CONSTRAINT orders_status_valid
                                CHECK (status IN (
                                    'Created',
                                    'PaymentAuthorized',
                                    'InventoryReserved',
                                    'InventoryReservationFailed',
                                    'DeliveryBooked'
                                )),
                        created_at TIMESTAMPTZ NOT NULL
                            DEFAULT clock_timestamp(),
                        updated_at TIMESTAMPTZ NOT NULL
                            DEFAULT clock_timestamp()
                    )
                `);

                await client.query(`
                    CREATE INDEX orders_created_at_idx
                    ON orders (created_at DESC)
                `);

                // The order's history. Events are unique by event id,
                // so a redelivered event increments `deliveries`
                // instead of adding a second row.
                await client.query(`
                    CREATE TABLE order_timeline (
                        entry_id BIGINT
                            GENERATED ALWAYS AS IDENTITY
                            PRIMARY KEY,
                        order_id UUID NOT NULL
                            REFERENCES orders (order_id),
                        kind TEXT NOT NULL
                            CONSTRAINT order_timeline_kind_valid
                                CHECK (kind IN ('Accepted', 'Replayed', 'Event')),
                        event_id TEXT
                            CONSTRAINT order_timeline_event_id_unique
                                UNIQUE,
                        event_type TEXT,
                        occurred_at TIMESTAMPTZ NOT NULL,
                        recorded_at TIMESTAMPTZ NOT NULL
                            DEFAULT clock_timestamp(),
                        deliveries INTEGER NOT NULL
                            DEFAULT 1
                            CONSTRAINT order_timeline_deliveries_positive
                                CHECK (deliveries >= 1),
                        details JSONB,
                        CONSTRAINT order_timeline_event_has_id
                            CHECK (
                                (kind = 'Event') =
                                (event_id IS NOT NULL AND event_type IS NOT NULL)
                            )
                    )
                `);

                await client.query(`
                    CREATE INDEX order_timeline_order_idx
                    ON order_timeline (order_id, entry_id)
                `);

                // Same shape as the Inventory outbox, so the shared
                // PostgresOutboxStore and OutboxRelay work unchanged.
                await client.query(`
                    CREATE TABLE order_outbox (
                        sequence_id BIGINT
                            GENERATED ALWAYS AS IDENTITY
                            PRIMARY KEY,
                        event_id UUID NOT NULL
                            CONSTRAINT order_outbox_event_id_unique
                                UNIQUE,
                        event_type TEXT NOT NULL,
                        routing_key TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        occurred_at TIMESTAMPTZ NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                            DEFAULT NOW(),
                        published_at TIMESTAMPTZ,
                        attempts INTEGER NOT NULL DEFAULT 0
                            CONSTRAINT order_outbox_attempts_nonnegative
                                CHECK (attempts >= 0),
                        available_at TIMESTAMPTZ NOT NULL
                            DEFAULT NOW(),
                        claimed_by TEXT,
                        last_error TEXT
                    )
                `);

                await client.query(`
                    CREATE INDEX order_outbox_pending_sequence_idx
                    ON order_outbox (sequence_id)
                    WHERE published_at IS NULL
                `);
            }
        }
    ];

export type {
    Migration,
    MigrationResult
};

export async function migrateOrderDatabase(
    pool: Pool,
    migrations:
        readonly Migration[] =
        ORDER_MIGRATIONS
): Promise<MigrationResult> {
    return migrateDatabase(
        pool,
        {
            historyTable:
                "order_schema_migrations",
            lockName:
                MIGRATION_LOCK_NAME,
            migrations
        }
    );
}
