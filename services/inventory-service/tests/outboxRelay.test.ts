import assert from "node:assert/strict";
import test from "node:test";
import type {
    AppLogger,
    LogContext
} from "@commerce-flow/logging";
import type {
    OutboxMessage,
    OutboxPublisher,
    OutboxStatistics,
    OutboxStore
} from "../src/outbox/outbox.js";
import {
    OutboxRelay
} from "../src/outbox/outboxRelay.js";

test(
    "marks events as published only after the publisher confirms them",
    async () => {
        const store =
            new FakeOutboxStore([
                createMessage("1"),
                createMessage("2")
            ]);

        const steps: string[] = [];

        store.onMarkPublished =
            messages => {
                steps.push(
                    `marked:${messages.map(message => message.sequenceId).join(",")}`
                );
            };

        const relay =
            createRelay(
                store,
                {
                    publish: async message => {
                        steps.push(
                            `confirmed:${message.sequenceId}`
                        );
                    }
                }
            );

        assert.equal(
            await relay.processBatch(),
            2
        );

        assert.deepEqual(
            steps,
            [
                "confirmed:1",
                "confirmed:2",
                "marked:1,2"
            ]
        );

        assert.deepEqual(
            store.released,
            []
        );
    }
);

test(
    "schedules a retry with capped exponential backoff for failed publications",
    async () => {
        const store =
            new FakeOutboxStore([
                createMessage("1", 1),
                createMessage("2", 3),
                createMessage("3", 20)
            ]);

        const relay =
            createRelay(
                store,
                {
                    publish: async message => {
                        if (message.sequenceId !== "1") {
                            throw new Error(
                                "channel closed"
                            );
                        }
                    }
                },
                {
                    retryBaseDelayInMs: 1_000,
                    retryMaxDelayInMs: 10_000
                },
                // random() = 1 makes "equal jitter" return the full delay.
                () => 0.999_999
            );

        await relay.processBatch();

        assert.deepEqual(
            store.published.map(
                message =>
                    message.sequenceId
            ),
            [
                "1"
            ]
        );

        assert.deepEqual(
            store.released.map(
                entry => ({
                    sequenceId:
                        entry.message.sequenceId,
                    retryDelayInMs:
                        entry.retryDelayInMs,
                    errorMessage:
                        entry.errorMessage
                })
            ),
            [
                {
                    sequenceId: "2",
                    retryDelayInMs: 4_000,
                    errorMessage: "channel closed"
                },
                {
                    sequenceId: "3",
                    retryDelayInMs: 10_000,
                    errorMessage: "channel closed"
                }
            ]
        );
    }
);

test(
    "applies jitter of up to half the backoff delay",
    async () => {
        const store =
            new FakeOutboxStore([
                createMessage("1", 2)
            ]);

        const relay =
            createRelay(
                store,
                {
                    publish: async () => {
                        throw new Error(
                            "nacked"
                        );
                    }
                },
                {
                    retryBaseDelayInMs: 1_000
                },
                () => 0
            );

        await relay.processBatch();

        assert.equal(
            store.released[0]?.retryDelayInMs,
            1_000
        );
    }
);

test(
    "propagates a failure to mark events as published so they are retried after the lease",
    async () => {
        const store =
            new FakeOutboxStore([
                createMessage("1")
            ]);

        store.onMarkPublished =
            () => {
                throw new Error(
                    "connection terminated"
                );
            };

        const relay =
            createRelay(
                store,
                {
                    publish: async () => undefined
                }
            );

        await assert.rejects(
            relay.processBatch(),
            /connection terminated/
        );

        assert.deepEqual(
            store.released,
            [],
            "A confirmed event must never be released as failed."
        );
    }
);

test(
    "keeps running after store failures and stops when aborted",
    async () => {
        const store =
            new FakeOutboxStore([]);

        let claims =
            0;

        const controller =
            new AbortController();

        store.onClaim =
            () => {
                claims +=
                    1;

                if (claims === 1) {
                    throw new Error(
                        "PostgreSQL unavailable"
                    );
                }

                if (claims === 3) {
                    controller.abort();
                }
            };

        const logger =
            new RecordingLogger();

        const relay =
            new OutboxRelay(
                {
                    store,
                    publisher: {
                        publish: async () => undefined
                    },
                    logger,
                    random: () => 0
                },
                {
                    pollIntervalInMs: 1,
                    retryBaseDelayInMs: 1
                }
            );

        await relay.run(
            controller.signal
        );

        assert.equal(
            claims,
            3
        );

        assert.equal(
            relay.getStatus().running,
            false
        );

        assert.equal(
            relay.getStatus().consecutiveFailures,
            0
        );

        assert.ok(
            logger.errors.includes(
                "Outbox relay cycle failed"
            )
        );
    }
);

test(
    "wakes up immediately when notified instead of waiting for the poll interval",
    async () => {
        const store =
            new FakeOutboxStore([]);

        const controller =
            new AbortController();

        let claims =
            0;

        store.onClaim =
            () => {
                claims +=
                    1;

                if (claims === 2) {
                    controller.abort();
                }
            };

        const relay =
            createRelay(
                store,
                {
                    publish: async () => undefined
                },
                {
                    pollIntervalInMs: 60_000
                }
            );

        const running =
            relay.run(
                controller.signal
            );

        await waitUntil(
            () =>
                claims === 1
        );

        relay.notify();

        await running;

        assert.equal(
            claims,
            2
        );
    }
);

function createRelay(
    store: OutboxStore,
    publisher: OutboxPublisher,
    options: ConstructorParameters<typeof OutboxRelay>[1] = {},
    random: () => number = () => 0.5
): OutboxRelay {
    return new OutboxRelay(
        {
            store,
            publisher,
            logger:
                new RecordingLogger(),
            random
        },
        {
            claimantId:
                "relay-under-test",
            ...options
        }
    );
}

function createMessage(
    sequenceId: string,
    attempts = 1
): OutboxMessage {
    return {
        sequenceId,
        eventId:
            `event-${sequenceId}`,
        eventType:
            "InventoryReserved",
        routingKey:
            "inventory.reserved",
        payload: {},
        attempts
    };
}

async function waitUntil(
    condition: () => boolean
): Promise<void> {
    while (!condition()) {
        await new Promise(
            resolve =>
                setImmediate(
                    resolve
                )
        );
    }
}

class FakeOutboxStore
    implements OutboxStore {
    readonly published:
        OutboxMessage[] = [];

    readonly released: {
        message: OutboxMessage;
        claimant: string;
        retryDelayInMs: number;
        errorMessage: string;
    }[] = [];

    onClaim:
        () => void =
        () => undefined;

    onMarkPublished:
        (messages: readonly OutboxMessage[]) => void =
        () => undefined;

    constructor(
        private pending: OutboxMessage[]
    ) {
    }

    async claimBatch(
        _claimant: string,
        limit: number
    ): Promise<OutboxMessage[]> {
        this.onClaim();

        const claimed =
            this.pending.slice(
                0,
                limit
            );

        this.pending =
            this.pending.slice(
                limit
            );

        return claimed;
    }

    async markPublished(
        messages: readonly OutboxMessage[]
    ): Promise<void> {
        this.onMarkPublished(
            messages
        );

        this.published.push(
            ...messages
        );
    }

    async releaseForRetry(
        message: OutboxMessage,
        claimant: string,
        retryDelayInMs: number,
        errorMessage: string
    ): Promise<boolean> {
        this.released.push({
            message,
            claimant,
            retryDelayInMs,
            errorMessage
        });

        return true;
    }

    async getStatistics(): Promise<OutboxStatistics> {
        return {
            pendingCount:
                this.pending.length,
            oldestPendingCreatedAt:
                null,
            maxPendingAttempts:
                0
        };
    }
}

class RecordingLogger
    implements AppLogger {
    readonly errors: string[] = [];

    info(): void {
        // Not asserted.
    }

    warn(): void {
        // Not asserted.
    }

    error(
        message: string
    ): void {
        this.errors.push(
            message
        );
    }

    child(
        _context: LogContext
    ): AppLogger {
        return this;
    }
}
