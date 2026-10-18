import type * as amqp from "amqplib";
import {
    COMMERCE_TOPOLOGY
} from "@commerce-flow/contracts";
import {
    readEnvelope
} from "./timeline.js";

/*
 * Inspection and replay of dead-lettered messages.
 *
 * A consumer dead-letters a message when it is malformed, violates its
 * contract, or still fails after the in-process retries (for example
 * during a database outage that outlasts them). Once the cause is
 * fixed, replay moves the messages back so they are processed again.
 *
 * Replay is at-least-once: a message is acknowledged on the dead-letter
 * queue only after the broker has confirmed its re-publication. A crash
 * in between leaves a copy in both queues, which the idempotent
 * consumers absorb.
 */

export const DEAD_LETTER_SUFFIX =
    ".dead-letter";

export const REPLAYED_FROM_HEADER =
    "x-commerce-flow-replayed-from";

export const REPLAY_COUNT_HEADER =
    "x-commerce-flow-replay-count";

const CONSUMER_QUEUES:
    readonly string[] =
    COMMERCE_TOPOLOGY.map(
        queue => queue.queueName
    );

export interface QueuePair {
    readonly queueName: string;
    readonly deadLetterQueueName: string;
}

/**
 * Accepts a consumer queue of the topology or its dead-letter queue.
 * Anything else is refused, so the tool cannot move messages into
 * queues it does not know.
 */
export function resolveQueuePair(
    name: string,
    allowedQueues: readonly string[] = CONSUMER_QUEUES
): QueuePair {
    const queueName =
        name.endsWith(DEAD_LETTER_SUFFIX)
            ? name.slice(0, -DEAD_LETTER_SUFFIX.length)
            : name;

    if (!allowedQueues.includes(queueName)) {
        throw new Error(
            `Unknown queue '${name}'. Expected one of: ` +
            `${allowedQueues.join(", ")}.`
        );
    }

    return {
        queueName,
        deadLetterQueueName:
            `${queueName}${DEAD_LETTER_SUFFIX}`
    };
}

export interface DeathRecord {
    /** Why the broker dead-lettered the message (e.g. "rejected"). */
    readonly reason: string;
    readonly queue: string;
    readonly count: number;
    readonly time: string | undefined;
    readonly routingKeys: readonly string[];
}

/** Reads the most recent entry of RabbitMQ's `x-death` header. */
export function readDeathRecord(
    headers: Readonly<Record<string, unknown>> | undefined
): DeathRecord | undefined {
    const deaths =
        headers?.["x-death"];

    if (!Array.isArray(deaths)) {
        return undefined;
    }

    const latest: unknown =
        deaths[0];

    if (
        typeof latest !== "object" ||
        latest === null
    ) {
        return undefined;
    }

    const record =
        latest as Record<string, unknown>;

    return {
        reason:
            String(record.reason ?? "unknown"),
        queue:
            String(record.queue ?? "unknown"),
        count:
            Number(record.count ?? 1),
        time:
            formatAmqpTimestamp(record.time),
        routingKeys:
            Array.isArray(record["routing-keys"])
                ? record["routing-keys"].map(String)
                : []
    };
}

export interface DeadLetterSummary {
    readonly messageId: string | undefined;
    readonly eventType: string | undefined;
    readonly correlationId: string | undefined;
    readonly orderId: string | undefined;
    readonly replayCount: number;
    readonly death: DeathRecord | undefined;
    readonly sizeInBytes: number;
}

export function summarizeDeadLetter(
    message: Pick<amqp.Message, "content" | "properties">
): DeadLetterSummary {
    let body: unknown;

    try {
        body = JSON.parse(
            message.content.toString("utf8")
        );
    } catch {
        body = undefined;
    }

    const envelope =
        readEnvelope(body);

    const headers =
        message.properties.headers ?? {};

    return {
        messageId:
            optionalString(message.properties.messageId) ??
            envelope?.eventId,
        eventType:
            optionalString(message.properties.type) ??
            envelope?.eventType,
        correlationId:
            optionalString(message.properties.correlationId) ??
            envelope?.correlationId,
        orderId:
            envelope?.orderId,
        replayCount:
            Number(headers[REPLAY_COUNT_HEADER] ?? 0),
        death:
            readDeathRecord(headers),
        sizeInBytes:
            message.content.length
    };
}

export interface DeadLetterInspection {
    readonly total: number;
    readonly messages: readonly DeadLetterSummary[];
}

/**
 * Lists up to `limit` dead letters without removing them: the messages
 * are fetched unacknowledged and returned to the queue, in their
 * original order, when the channel closes.
 */
export async function inspectDeadLetters(
    connection: amqp.ChannelModel,
    queue: QueuePair,
    limit = 20
): Promise<DeadLetterInspection> {
    const channel =
        await connection.createChannel();

    try {
        const {
            messageCount
        } =
            await channel.checkQueue(
                queue.deadLetterQueueName
            );

        const messages: DeadLetterSummary[] = [];

        while (messages.length < Math.min(limit, messageCount)) {
            const message =
                await channel.get(
                    queue.deadLetterQueueName,
                    {
                        noAck: false
                    }
                );

            if (message === false) {
                break;
            }

            messages.push(
                summarizeDeadLetter(message)
            );
        }

        return {
            total: messageCount,
            messages
        };
    } finally {
        // Closing without acknowledging requeues every fetched message.
        await channel
            .close()
            .catch(() => undefined);
    }
}

export interface ReplayOptions {
    /** Replay at most this many messages. */
    readonly limit?: number;

    readonly onReplayed?: (summary: DeadLetterSummary) => void;
}

export interface ReplayResult {
    readonly replayed: number;
    readonly remaining: number;
}

/**
 * Moves dead letters back to the consumer queue they came from.
 *
 * - Messages are published through the default exchange directly to
 *   that queue, so other queues bound to the same routing key do not
 *   receive a second copy.
 * - Each publication is mandatory and confirmed before the dead letter
 *   is acknowledged.
 * - At most the number of messages present at the start is replayed,
 *   so a message that fails again and returns to the dead-letter queue
 *   cannot make the replay loop forever.
 */
export async function replayDeadLetters(
    connection: amqp.ChannelModel,
    queue: QueuePair,
    options: ReplayOptions = {}
): Promise<ReplayResult> {
    const channel =
        await connection.createConfirmChannel();

    // basic.return arrives before the confirm of an unroutable message.
    let returnedCount = 0;

    channel.on(
        "return",
        () => {
            returnedCount += 1;
        }
    );

    try {
        // Fails if the consumer queue does not exist.
        await channel.checkQueue(
            queue.queueName
        );

        const {
            messageCount
        } =
            await channel.checkQueue(
                queue.deadLetterQueueName
            );

        const maximum =
            Math.min(
                options.limit ?? messageCount,
                messageCount
            );

        let replayed = 0;

        while (replayed < maximum) {
            const message =
                await channel.get(
                    queue.deadLetterQueueName,
                    {
                        noAck: false
                    }
                );

            if (message === false) {
                break;
            }

            const summary =
                summarizeDeadLetter(message);

            const returnedBefore =
                returnedCount;

            channel.publish(
                "",
                queue.queueName,
                message.content,
                {
                    mandatory: true,
                    persistent: true,
                    contentType:
                        message.properties.contentType as string | undefined,
                    messageId:
                        message.properties.messageId as string | undefined,
                    correlationId:
                        message.properties.correlationId as string | undefined,
                    type:
                        message.properties.type as string | undefined,
                    headers:
                        replayHeaders(
                            message.properties.headers,
                            queue.deadLetterQueueName,
                            summary.replayCount
                        )
                }
            );

            await channel.waitForConfirms();

            if (returnedCount !== returnedBefore) {
                // Leave the dead letter where it is.
                channel.nack(message, false, true);

                throw new Error(
                    `The broker could not route the message to ` +
                    `'${queue.queueName}'.`
                );
            }

            channel.ack(message);
            replayed += 1;
            options.onReplayed?.(summary);
        }

        const {
            messageCount: remaining
        } =
            await channel.checkQueue(
                queue.deadLetterQueueName
            );

        return {
            replayed,
            remaining
        };
    } finally {
        await channel
            .close()
            .catch(() => undefined);
    }
}

/**
 * Drops the broker's death bookkeeping (it describes the previous
 * attempt) and records where and how often the message was replayed.
 */
export function replayHeaders(
    headers: Readonly<Record<string, unknown>> | undefined,
    deadLetterQueueName: string,
    previousReplayCount: number
): Record<string, unknown> {
    const result: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(headers ?? {})) {
        if (
            key === "x-death" ||
            key.startsWith("x-first-death-") ||
            key.startsWith("x-last-death-")
        ) {
            continue;
        }

        result[key] = value;
    }

    result[REPLAYED_FROM_HEADER] =
        deadLetterQueueName;

    result[REPLAY_COUNT_HEADER] =
        previousReplayCount + 1;

    return result;
}

function formatAmqpTimestamp(
    value: unknown
): string | undefined {
    // amqplib decodes AMQP timestamps as { "!": "timestamp", value: seconds }.
    const seconds =
        typeof value === "number"
            ? value
            : typeof value === "object" &&
              value !== null &&
              "value" in value &&
              typeof value.value === "number"
                ? value.value
                : undefined;

    return seconds === undefined
        ? undefined
        : new Date(seconds * 1_000).toISOString();
}

function optionalString(
    value: unknown
): string | undefined {
    return typeof value === "string" && value !== ""
        ? value
        : undefined;
}
