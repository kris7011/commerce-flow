import {
    Pool
} from "pg";
import type {
    AppLogger
} from "@commerce-flow/logging";

/** Development-only default; production must set DATABASE_URL. */
export const DEFAULT_DATABASE_URL =
    "postgresql://commerce_flow:" +
    "commerce_flow@" +
    "localhost:5432/" +
    "commerce_flow";

export interface DatabasePoolOptions {
    /** Shown in pg_stat_activity, e.g. `order-service`. */
    readonly applicationName: string;
    readonly connectionString?: string;
    readonly logger?: AppLogger;
    readonly maxConnections?: number;
}

export function createDatabasePool(
    options: DatabasePoolOptions
): Pool {
    const pool =
        new Pool({
            connectionString:
                options.connectionString ??
                process.env.DATABASE_URL ??
                DEFAULT_DATABASE_URL,
            application_name:
                options.applicationName,
            max:
                options.maxConnections ??
                10,
            // Fail fast while PostgreSQL is unreachable instead of
            // queueing requests indefinitely.
            connectionTimeoutMillis:
                5_000,
            idleTimeoutMillis:
                30_000,
            statement_timeout:
                15_000
        });

    // An idle client can fail (e.g. when PostgreSQL restarts).
    // Without a listener that error would crash the process; the
    // pool discards the client and reconnects on the next query.
    pool.on(
        "error",
        error => {
            options.logger?.warn(
                "Idle PostgreSQL client failed",
                {
                    errorMessage:
                        error.message
                }
            );
        }
    );

    return pool;
}

/**
 * Upper bound for a readiness query. While PostgreSQL is unreachable
 * a query waits for the pool's connection timeout (5 s); a readiness
 * probe must answer well within typical probe timeouts (about 1 s)
 * instead.
 */
export const DATABASE_READINESS_TIMEOUT_IN_MS =
    1_000;

/**
 * True when `readinessQuery` succeeds within the timeout. The query
 * should touch a table the service owns, so a missing schema also
 * counts as not ready.
 */
export async function isDatabaseReady(
    pool: Pick<Pool, "query">,
    readinessQuery: string,
    timeoutInMs =
        DATABASE_READINESS_TIMEOUT_IN_MS
): Promise<boolean> {
    let timer:
        NodeJS.Timeout | undefined;

    const timedOut =
        new Promise<false>(resolve => {
            timer =
                setTimeout(
                    () => resolve(false),
                    timeoutInMs
                );
        });

    // A query that outlives the timeout still finishes in the
    // background (bounded by the pool's own timeouts); its result is
    // ignored.
    const query =
        pool.query(
            readinessQuery
        ).then(
            () => true,
            () => false
        );

    try {
        return await Promise.race([
            query,
            timedOut
        ]);
    } finally {
        clearTimeout(timer);
    }
}
