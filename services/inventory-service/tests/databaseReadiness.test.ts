import assert from "node:assert/strict";
import test from "node:test";
import type {
    Pool
} from "pg";
import {
    isInventoryDatabaseReady
} from "../src/database.js";

function poolAnswering(
    query: () => Promise<unknown>
): Pick<Pool, "query"> {
    return {
        query
    } as unknown as Pick<Pool, "query">;
}

test(
    "reports ready when the readiness query succeeds",
    async () => {
        assert.equal(
            await isInventoryDatabaseReady(
                poolAnswering(async () => ({ rows: [] }))
            ),
            true
        );
    }
);

test(
    "reports not ready when the readiness query fails",
    async () => {
        assert.equal(
            await isInventoryDatabaseReady(
                poolAnswering(async () => {
                    throw new Error("connect ECONNREFUSED");
                })
            ),
            false
        );
    }
);

test(
    "answers within its timeout while PostgreSQL does not respond",
    async () => {
        let settle: (() => void) | undefined;

        // Like a pool waiting for its connection timeout.
        const hanging =
            new Promise<unknown>(resolve => {
                settle = () => resolve({ rows: [] });
            });

        const startedAt =
            Date.now();

        const ready =
            await isInventoryDatabaseReady(
                poolAnswering(() => hanging),
                50
            );

        assert.equal(ready, false);
        assert.ok(
            Date.now() - startedAt < 1_000,
            "The probe should not wait for the hanging query."
        );

        settle?.();
    }
);
