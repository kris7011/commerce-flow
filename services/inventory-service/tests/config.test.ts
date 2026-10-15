import assert from "node:assert/strict";
import test from "node:test";
import {
    readInventoryConfig
} from "../src/config.js";

test(
    "uses development defaults when nothing is configured",
    () => {
        const config =
            readInventoryConfig({});

        assert.equal(
            config.port,
            3003
        );

        assert.deepEqual(
            config.outbox,
            {
                batchSize: 50,
                pollIntervalInMs: 1_000,
                leaseInMs: 30_000
            }
        );
    }
);

test(
    "rejects an outbox lease that a slow publish could outlive",
    () => {
        assert.throws(
            () =>
                readInventoryConfig({
                    RABBITMQ_PUBLISH_TIMEOUT_MS: "10000",
                    OUTBOX_LEASE_MS: "15000"
                }),
            /OUTBOX_LEASE_MS must be at least twice/
        );
    }
);

test(
    "rejects a non-PostgreSQL database URL",
    () => {
        assert.throws(
            () =>
                readInventoryConfig({
                    DATABASE_URL:
                        "http://localhost/inventory"
                }),
            /DATABASE_URL must use one of/
        );
    }
);
