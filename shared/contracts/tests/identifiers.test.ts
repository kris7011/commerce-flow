import assert from "node:assert/strict";
import test from "node:test";
import {
    deriveDeterministicId
} from "../src/identifiers.js";

const uuidPattern =
    /^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

test(
    "derives the same UUID-formatted id for the same inputs",
    () => {
        const first =
            deriveDeterministicId(
                "payment-authorized",
                "order-event-001"
            );

        const second =
            deriveDeterministicId(
                "payment-authorized",
                "order-event-001"
            );

        assert.equal(
            first,
            second
        );

        assert.match(
            first,
            uuidPattern
        );
    }
);

test(
    "derives different ids for different namespaces or inputs",
    () => {
        const ids =
            new Set([
                deriveDeterministicId(
                    "payment-authorized",
                    "order-event-001"
                ),
                deriveDeterministicId(
                    "payment-id",
                    "order-event-001"
                ),
                deriveDeterministicId(
                    "payment-authorized",
                    "order-event-002"
                ),
                // Part boundaries are significant.
                deriveDeterministicId(
                    "namespace",
                    "ab",
                    "c"
                ),
                deriveDeterministicId(
                    "namespace",
                    "a",
                    "bc"
                )
            ]);

        assert.equal(
            ids.size,
            5
        );
    }
);
