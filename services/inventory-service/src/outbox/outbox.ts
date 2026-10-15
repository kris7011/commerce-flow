/*
 * Ports for the transactional outbox relay.
 *
 * Delivery is at-least-once: an event is marked as published only after
 * the broker has confirmed it, so a crash between publishing and marking
 * leads to a re-publication, never to a lost event. Consumers therefore
 * have to be idempotent.
 */

export interface OutboxMessage {
    /** Monotonic outbox position (BIGINT, kept as a string). */
    readonly sequenceId: string;
    readonly eventId: string;
    readonly eventType: string;
    readonly routingKey: string;
    readonly payload: unknown;

    /**
     * Number of claims including the current one. Also used as a
     * fencing token so a relay whose lease expired cannot overwrite
     * the state set by the relay that claimed the event after it.
     */
    readonly attempts: number;
}

export interface OutboxStatistics {
    readonly pendingCount: number;
    readonly oldestPendingCreatedAt: string | null;
    readonly maxPendingAttempts: number;
}

export interface OutboxStore {
    /**
     * Atomically claims up to `limit` due events for `leaseInMs`.
     * Rows claimed by another relay are skipped, never waited for.
     */
    claimBatch(
        claimant: string,
        limit: number,
        leaseInMs: number
    ): Promise<OutboxMessage[]>;

    markPublished(
        messages: readonly OutboxMessage[]
    ): Promise<void>;

    /**
     * Makes a claimed event available again after `retryDelayInMs`.
     * Returns false if the claim was lost in the meantime.
     */
    releaseForRetry(
        message: OutboxMessage,
        claimant: string,
        retryDelayInMs: number,
        errorMessage: string
    ): Promise<boolean>;

    getStatistics(): Promise<OutboxStatistics>;
}

export interface OutboxPublisher {
    /** Resolves only after the broker has confirmed the message. */
    publish(
        message: OutboxMessage
    ): Promise<void>;
}
