import type {
    Pool,
    PoolClient
} from "pg";

/**
 * Runs `work` in one transaction on a dedicated client: COMMIT when it
 * resolves, ROLLBACK when it throws. A client whose connection broke is
 * discarded by the pool instead of being handed out again.
 */
export async function withTransaction<T>(
    pool: Pool,
    work: (
        client: PoolClient
    ) => Promise<T>
): Promise<T> {
    const client =
        await pool.connect();

    let connectionBroken:
        Error | undefined;

    // A checked-out client that loses its connection emits "error";
    // without a listener that would crash the process. The in-flight
    // query rejects as well, which aborts the work.
    const onConnectionError =
        (error: Error): void => {
            connectionBroken =
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

        const result =
            await work(
                client
            );

        await client.query(
            "COMMIT"
        );

        return result;
    } catch (error) {
        try {
            await client.query(
                "ROLLBACK"
            );
        } catch (rollbackError) {
            connectionBroken =
                rollbackError instanceof Error
                    ? rollbackError
                    : new Error(
                        "ROLLBACK failed"
                    );
        }

        throw error;
    } finally {
        client.off(
            "error",
            onConnectionError
        );

        client.release(
            connectionBroken
        );
    }
}
