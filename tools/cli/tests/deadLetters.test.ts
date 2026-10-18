import assert from "node:assert/strict";
import test from "node:test";
import {
    REPLAY_COUNT_HEADER,
    REPLAYED_FROM_HEADER,
    readDeathRecord,
    replayHeaders,
    resolveQueuePair,
    summarizeDeadLetter
} from "../src/deadLetters.js";

test(
    "resolves a consumer queue or its dead-letter queue to the same pair",
    () => {
        const expected = {
            queueName: "inventory-service.payment-authorized",
            deadLetterQueueName: "inventory-service.payment-authorized.dead-letter"
        };

        assert.deepEqual(
            resolveQueuePair("inventory-service.payment-authorized"),
            expected
        );

        assert.deepEqual(
            resolveQueuePair("inventory-service.payment-authorized.dead-letter"),
            expected
        );
    }
);

test(
    "refuses queues outside the commerce topology",
    () => {
        for (const name of ["", "commerce.events", "other.queue.dead-letter"]) {
            assert.throws(
                () => resolveQueuePair(name),
                /Unknown queue/
            );
        }
    }
);

test(
    "reads the latest x-death entry as decoded by amqplib",
    () => {
        assert.deepEqual(
            readDeathRecord({
                "x-death": [
                    {
                        reason: "rejected",
                        queue: "inventory-service.payment-authorized",
                        count: 2,
                        exchange: "commerce.events",
                        time: {
                            "!": "timestamp",
                            value: 1_760_000_000
                        },
                        "routing-keys": ["payment.authorized"]
                    }
                ]
            }),
            {
                reason: "rejected",
                queue: "inventory-service.payment-authorized",
                count: 2,
                time: "2025-10-09T08:53:20.000Z",
                routingKeys: ["payment.authorized"]
            }
        );

        assert.equal(readDeathRecord(undefined), undefined);
        assert.equal(readDeathRecord({ "x-death": "invalid" }), undefined);
    }
);

test(
    "drops the death bookkeeping and counts replays",
    () => {
        const headers =
            replayHeaders(
                {
                    "x-death": [],
                    "x-first-death-reason": "rejected",
                    "x-last-death-queue": "q",
                    "x-custom": "kept",
                    [REPLAY_COUNT_HEADER]: 1
                },
                "q.dead-letter",
                1
            );

        assert.deepEqual(headers, {
            "x-custom": "kept",
            [REPLAYED_FROM_HEADER]: "q.dead-letter",
            [REPLAY_COUNT_HEADER]: 2
        });
    }
);

test(
    "summarises a message even if its body is not a valid event",
    () => {
        const summary =
            summarizeDeadLetter({
                content: Buffer.from("not json"),
                properties: {
                    messageId: "message-1",
                    type: "PaymentAuthorized",
                    correlationId: "demo-1",
                    headers: {}
                }
            } as unknown as Parameters<typeof summarizeDeadLetter>[0]);

        assert.equal(summary.messageId, "message-1");
        assert.equal(summary.eventType, "PaymentAuthorized");
        assert.equal(summary.orderId, undefined);
        assert.equal(summary.replayCount, 0);
        assert.equal(summary.death, undefined);
        assert.equal(summary.sizeInBytes, 8);
    }
);
