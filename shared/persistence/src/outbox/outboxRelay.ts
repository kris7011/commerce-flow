import {
    randomUUID
} from "node:crypto";
import {
    hostname
} from "node:os";
import type {
    AppLogger
} from "@commerce-flow/logging";
import {
    getErrorMessage
} from "@commerce-flow/messaging";
import type {
    OutboxMessage,
    OutboxPublisher,
    OutboxStore
} from "./outbox.js";

export interface OutboxRelayOptions {
    readonly batchSize?: number;

    /**
     * How long a claimed event stays invisible to other relays. Must
     * comfortably exceed the publish confirmation timeout, otherwise
     * a slow publish can be duplicated by another relay.
     */
    readonly leaseInMs?: number;

    /** Fallback polling interval when no notification arrives. */
    readonly pollIntervalInMs?: number;

    readonly retryBaseDelayInMs?: number;
    readonly retryMaxDelayInMs?: number;

    /** Attempts after which failures are logged at error level. */
    readonly alertAfterAttempts?: number;

    /** Identifies this relay instance in claims and logs. */
    readonly claimantId?: string;
}

export interface OutboxRelayDependencies {
    readonly store: OutboxStore;
    readonly publisher: OutboxPublisher;
    readonly logger: AppLogger;

    /** Returns a value in [0, 1); injectable for deterministic tests. */
    readonly random?: () => number;
}

export interface OutboxRelayStatus {
    readonly claimantId: string;
    readonly running: boolean;
    readonly consecutiveFailures: number;
    readonly lastCycleCompletedAt: string | null;
}

const DEFAULTS = {
    batchSize: 50,
    leaseInMs: 30_000,
    pollIntervalInMs: 1_000,
    retryBaseDelayInMs: 1_000,
    retryMaxDelayInMs: 60_000,
    alertAfterAttempts: 10
} as const;

/**
 * Publishes events from the transactional outbox to RabbitMQ.
 *
 * Several relays (in one or many processes) can run concurrently: each
 * claims a disjoint batch with FOR UPDATE SKIP LOCKED, publishes it
 * outside of any database transaction, waits for broker confirms and
 * only then marks the events as published. Failed events are retried
 * with capped exponential backoff and jitter.
 */
export class OutboxRelay {
    readonly claimantId: string;

    private readonly batchSize: number;
    private readonly leaseInMs: number;
    private readonly pollIntervalInMs: number;
    private readonly retryBaseDelayInMs: number;
    private readonly retryMaxDelayInMs: number;
    private readonly alertAfterAttempts: number;
    private readonly random: () => number;

    private running =
        false;

    private consecutiveFailures =
        0;

    private lastCycleCompletedAt:
        string | null = null;

    private notified =
        false;

    private wakeUp:
        (() => void) | null = null;

    constructor(
        private readonly dependencies:
            OutboxRelayDependencies,
        options:
            OutboxRelayOptions = {}
    ) {
        this.batchSize =
            options.batchSize ??
            DEFAULTS.batchSize;

        this.leaseInMs =
            options.leaseInMs ??
            DEFAULTS.leaseInMs;

        this.pollIntervalInMs =
            options.pollIntervalInMs ??
            DEFAULTS.pollIntervalInMs;

        this.retryBaseDelayInMs =
            options.retryBaseDelayInMs ??
            DEFAULTS.retryBaseDelayInMs;

        this.retryMaxDelayInMs =
            options.retryMaxDelayInMs ??
            DEFAULTS.retryMaxDelayInMs;

        this.alertAfterAttempts =
            options.alertAfterAttempts ??
            DEFAULTS.alertAfterAttempts;

        this.claimantId =
            options.claimantId ??
            `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;

        this.random =
            dependencies.random ??
            Math.random;
    }

    /** Wakes the relay immediately, e.g. after an outbox insert. */
    notify(): void {
        this.notified =
            true;

        this.wakeUp?.();
    }

    getStatus(): OutboxRelayStatus {
        return {
            claimantId:
                this.claimantId,
            running:
                this.running,
            consecutiveFailures:
                this.consecutiveFailures,
            lastCycleCompletedAt:
                this.lastCycleCompletedAt
        };
    }

    /**
     * Runs until the signal is aborted. The batch in progress is
     * always completed (publish confirms are bounded by the publisher's
     * timeout), so shutdown never leaves an event half-processed.
     */
    async run(
        signal: AbortSignal
    ): Promise<void> {
        this.running =
            true;

        this.dependencies.logger.info(
            "Outbox relay started",
            {
                claimantId:
                    this.claimantId,
                batchSize:
                    this.batchSize,
                leaseInMs:
                    this.leaseInMs
            }
        );

        try {
            while (!signal.aborted) {
                this.notified =
                    false;

                let claimedCount: number;

                try {
                    claimedCount =
                        await this.processBatch();

                    this.consecutiveFailures =
                        0;

                    this.lastCycleCompletedAt =
                        new Date().toISOString();
                } catch (error) {
                    this.consecutiveFailures +=
                        1;

                    const retryDelayInMs =
                        this.getRetryDelay(
                            this.consecutiveFailures
                        );

                    this.dependencies.logger.error(
                        "Outbox relay cycle failed",
                        error,
                        {
                            claimantId:
                                this.claimantId,
                            consecutiveFailures:
                                this.consecutiveFailures,
                            retryDelayInMs
                        }
                    );

                    await this.waitForWork(
                        retryDelayInMs,
                        signal,
                        false
                    );

                    continue;
                }

                if (
                    claimedCount <
                    this.batchSize
                ) {
                    await this.waitForWork(
                        this.pollIntervalInMs,
                        signal,
                        true
                    );
                }
            }
        } finally {
            this.running =
                false;

            this.dependencies.logger.info(
                "Outbox relay stopped",
                {
                    claimantId:
                        this.claimantId
                }
            );
        }
    }

    /**
     * Claims, publishes and settles one batch. Returns the number of
     * claimed events. Store failures propagate to the caller; publish
     * failures are recorded per event.
     */
    async processBatch():
        Promise<number> {
        const {
            store,
            publisher,
            logger
        } = this.dependencies;

        const claimed =
            await store.claimBatch(
                this.claimantId,
                this.batchSize,
                this.leaseInMs
            );

        if (claimed.length === 0) {
            return 0;
        }

        // Publish the whole batch concurrently; the confirm channel
        // pipelines the messages and confirms them independently.
        const results =
            await Promise.allSettled(
                claimed.map(
                    message =>
                        publisher.publish(
                            message
                        )
                )
            );

        const published:
            OutboxMessage[] = [];

        const failed: {
            message: OutboxMessage;
            error: unknown;
        }[] = [];

        results.forEach(
            (result, index) => {
                const message =
                    claimed[index];

                if (!message) {
                    return;
                }

                if (result.status === "fulfilled") {
                    published.push(
                        message
                    );
                } else {
                    failed.push({
                        message,
                        error:
                            result.reason
                    });
                }
            }
        );

        if (published.length > 0) {
            // If this fails the events stay claimed until the lease
            // expires and are then published again (at-least-once).
            await store.markPublished(
                published
            );

            logger.info(
                "Published outbox events",
                {
                    claimantId:
                        this.claimantId,
                    count:
                        published.length,
                    eventIds:
                        published.map(
                            message =>
                                message.eventId
                        )
                }
            );
        }

        for (const { message, error } of failed) {
            await this.scheduleRetry(
                message,
                error
            );
        }

        return claimed.length;
    }

    private async scheduleRetry(
        message: OutboxMessage,
        error: unknown
    ): Promise<void> {
        const {
            store,
            logger
        } = this.dependencies;

        const retryDelayInMs =
            this.getRetryDelay(
                message.attempts
            );

        const errorMessage =
            getErrorMessage(
                error
            );

        const context = {
            claimantId:
                this.claimantId,
            eventId:
                message.eventId,
            eventType:
                message.eventType,
            routingKey:
                message.routingKey,
            attempts:
                message.attempts,
            retryDelayInMs,
            errorMessage
        };

        if (
            message.attempts >=
            this.alertAfterAttempts
        ) {
            logger.error(
                "Outbox event keeps failing to publish",
                error,
                context
            );
        } else {
            logger.warn(
                "Outbox event publish failed; will retry",
                context
            );
        }

        try {
            const released =
                await store.releaseForRetry(
                    message,
                    this.claimantId,
                    retryDelayInMs,
                    errorMessage
                );

            if (!released) {
                logger.warn(
                    "Outbox claim was lost before the retry could be scheduled",
                    context
                );
            }
        } catch (releaseError) {
            // The lease still expires, so the event is retried anyway.
            logger.error(
                "Could not schedule outbox retry",
                releaseError,
                context
            );
        }
    }

    /** Capped exponential backoff with "equal jitter". */
    private getRetryDelay(
        attempt: number
    ): number {
        const exponentialDelay =
            Math.min(
                this.retryBaseDelayInMs *
                2 ** Math.max(
                    0,
                    attempt - 1
                ),
                this.retryMaxDelayInMs
            );

        return Math.round(
            exponentialDelay / 2 +
            this.random() *
            exponentialDelay / 2
        );
    }

    private async waitForWork(
        timeoutInMs: number,
        signal: AbortSignal,
        wakeOnNotify: boolean
    ): Promise<void> {
        if (
            signal.aborted ||
            (
                wakeOnNotify &&
                this.notified
            )
        ) {
            return;
        }

        await new Promise<void>(
            resolve => {
                const finish =
                    (): void => {
                        clearTimeout(
                            timer
                        );

                        signal.removeEventListener(
                            "abort",
                            finish
                        );

                        this.wakeUp =
                            null;

                        resolve();
                    };

                const timer =
                    setTimeout(
                        finish,
                        timeoutInMs
                    );

                signal.addEventListener(
                    "abort",
                    finish,
                    {
                        once: true
                    }
                );

                if (wakeOnNotify) {
                    this.wakeUp =
                        finish;
                }
            }
        );
    }
}
