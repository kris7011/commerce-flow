import type {
    Pool
} from "pg";
import type {
    OutboxMessage,
    OutboxStatistics,
    OutboxStore
} from "./outbox.js";

interface OutboxRow {
    sequenceId: string;
    eventId: string;
    eventType: string;
    routingKey: string;
    payload: unknown;
    attempts: number;
}

interface StatisticsRow {
    pendingCount: string;
    oldestPendingCreatedAt: Date | null;
    maxPendingAttempts: number | null;
}

const MAX_STORED_ERROR_LENGTH =
    2_000;

/** Outbox table names are interpolated into SQL, so only plain identifiers are allowed. */
const TABLE_NAME_PATTERN =
    /^[a-z_][a-z0-9_]{0,62}$/;

/**
 * PostgreSQL outbox store for one service's outbox table.
 *
 * Every operation is a single short statement: no transaction is held
 * open while the relay talks to RabbitMQ. Claiming pushes available_at
 * into the future (a lease), so events claimed by a relay that crashes
 * become due again once the lease expires.
 */
export class PostgresOutboxStore
    implements OutboxStore {
    private readonly table: string;

    constructor(
        private readonly pool: Pool,
        tableName: string
    ) {
        if (!TABLE_NAME_PATTERN.test(tableName)) {
            throw new Error(
                `Invalid outbox table name '${tableName}'.`
            );
        }

        this.table =
            tableName;
    }

    async claimBatch(
        claimant: string,
        limit: number,
        leaseInMs: number
    ): Promise<OutboxMessage[]> {
        const result =
            await this.pool.query<OutboxRow>(
                `
                    WITH due AS (
                        SELECT sequence_id
                        FROM ${this.table}
                        WHERE
                            published_at IS NULL
                            AND available_at <= clock_timestamp()
                        ORDER BY sequence_id
                        LIMIT $1
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE ${this.table} AS outbox
                    SET
                        available_at =
                            clock_timestamp() +
                            make_interval(secs => $2::double precision / 1000),
                        claimed_by = $3,
                        attempts = outbox.attempts + 1
                    FROM due
                    WHERE outbox.sequence_id = due.sequence_id
                    RETURNING
                        outbox.sequence_id::text AS "sequenceId",
                        outbox.event_id::text AS "eventId",
                        outbox.event_type AS "eventType",
                        outbox.routing_key AS "routingKey",
                        outbox.payload,
                        outbox.attempts
                `,
                [
                    limit,
                    leaseInMs,
                    claimant
                ]
            );

        return result.rows.sort(
            (left, right) =>
                compareSequenceIds(
                    left.sequenceId,
                    right.sequenceId
                )
        );
    }

    async markPublished(
        messages: readonly OutboxMessage[]
    ): Promise<void> {
        if (messages.length === 0) {
            return;
        }

        // Not fenced by the claim: once the broker has confirmed the
        // event it is published, whoever recorded it first.
        await this.pool.query(
            `
                UPDATE ${this.table}
                SET
                    published_at = clock_timestamp(),
                    claimed_by = NULL,
                    last_error = NULL
                WHERE
                    sequence_id = ANY($1::bigint[])
                    AND published_at IS NULL
            `,
            [
                messages.map(
                    message =>
                        message.sequenceId
                )
            ]
        );
    }

    async releaseForRetry(
        message: OutboxMessage,
        claimant: string,
        retryDelayInMs: number,
        errorMessage: string
    ): Promise<boolean> {
        const result =
            await this.pool.query(
                `
                    UPDATE ${this.table}
                    SET
                        available_at =
                            clock_timestamp() +
                            make_interval(secs => $4::double precision / 1000),
                        claimed_by = NULL,
                        last_error = $5
                    WHERE
                        sequence_id = $1::bigint
                        AND claimed_by = $2
                        AND attempts = $3
                        AND published_at IS NULL
                `,
                [
                    message.sequenceId,
                    claimant,
                    message.attempts,
                    retryDelayInMs,
                    errorMessage.slice(
                        0,
                        MAX_STORED_ERROR_LENGTH
                    )
                ]
            );

        return result.rowCount === 1;
    }

    async getStatistics(): Promise<OutboxStatistics> {
        const result =
            await this.pool.query<StatisticsRow>(
                `
                    SELECT
                        count(*) AS "pendingCount",
                        min(created_at) AS "oldestPendingCreatedAt",
                        max(attempts) AS "maxPendingAttempts"
                    FROM ${this.table}
                    WHERE published_at IS NULL
                `
            );

        const row =
            result.rows[0];

        return {
            pendingCount:
                Number(
                    row?.pendingCount ??
                    0
                ),
            oldestPendingCreatedAt:
                row?.oldestPendingCreatedAt
                    ?.toISOString() ??
                null,
            maxPendingAttempts:
                row?.maxPendingAttempts ??
                0
        };
    }
}

function compareSequenceIds(
    left: string,
    right: string
): number {
    const difference =
        BigInt(left) -
        BigInt(right);

    return difference === 0n
        ? 0
        : difference < 0n
            ? -1
            : 1;
}
