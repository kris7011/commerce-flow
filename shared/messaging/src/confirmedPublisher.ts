import {
    randomUUID
} from "node:crypto";
import {
    UnroutableMessageError,
    getErrorMessage
} from "./messageErrors.js";

/*
 * Publisher confirms alone do not prove delivery: RabbitMQ positively
 * confirms a message that matched no queue and silently drops it. This
 * module publishes every message as "mandatory" and correlates the
 * broker's basic.return with the publication it belongs to.
 *
 * Protocol facts this relies on (RabbitMQ publisher confirms):
 * - for an unroutable mandatory message, basic.return is sent before
 *   the basic.ack for that message, on the same channel;
 * - frames on a channel are processed in order.
 * So by the time a publication's confirm callback runs, a return for
 * it, if any, has already been recorded.
 *
 * Returns carry no delivery tag, so each publication gets a unique
 * header. Correlating on messageId would be ambiguous when the same
 * event is published twice concurrently.
 */

export const PUBLICATION_ID_HEADER =
    "x-publication-id";

export interface ReturnedMessage {
    readonly fields: {
        readonly replyCode: number;
        readonly replyText: string;
        readonly exchange: string;
        readonly routingKey: string;
    };
    readonly properties: {
        readonly headers?: Readonly<Record<string, unknown>>;
    };
}

export interface ConfirmPublishOptions {
    readonly contentType: string;
    readonly deliveryMode: number;
    readonly messageId: string;
    readonly correlationId: string;
    readonly type: string;
    readonly mandatory: boolean;
    readonly headers: Readonly<Record<string, unknown>>;
}

/** The subset of an amqplib ConfirmChannel this module needs. */
export interface ConfirmChannelLike {
    publish(
        exchange: string,
        routingKey: string,
        content: Buffer,
        options: ConfirmPublishOptions,
        callback: (error: unknown) => void
    ): boolean;

    on(
        event: "return",
        listener: (message: ReturnedMessage) => void
    ): unknown;
}

export type PublicationOptions =
    Omit<ConfirmPublishOptions, "mandatory" | "headers">;

export class ConfirmedPublisher {
    /** Pending publications; the value is set once a return arrives. */
    private readonly pending =
        new Map<string, ReturnedMessage["fields"] | null>();

    constructor(
        private readonly channel: ConfirmChannelLike,
        private readonly generatePublicationId:
            () => string =
            randomUUID
    ) {
        channel.on(
            "return",
            message => {
                const publicationId =
                    message.properties.headers?.[
                    PUBLICATION_ID_HEADER
                    ];

                if (
                    typeof publicationId === "string" &&
                    this.pending.has(
                        publicationId
                    )
                ) {
                    this.pending.set(
                        publicationId,
                        message.fields
                    );
                }
            }
        );
    }

    /** Number of publications still awaiting their confirm. */
    get pendingCount(): number {
        return this.pending.size;
    }

    /**
     * Resolves only if the broker confirmed the message and did not
     * return it as unroutable.
     */
    publish(
        exchange: string,
        routingKey: string,
        content: Buffer,
        options: PublicationOptions
    ): Promise<void> {
        return new Promise<void>(
            (
                resolve,
                reject
            ) => {
                const publicationId =
                    this.generatePublicationId();

                this.pending.set(
                    publicationId,
                    null
                );

                const settle =
                    (error: unknown): void => {
                        const returned =
                            this.pending.get(
                                publicationId
                            );

                        this.pending.delete(
                            publicationId
                        );

                        if (
                            error !== null &&
                            error !== undefined
                        ) {
                            reject(
                                error instanceof Error
                                    ? error
                                    : new Error(
                                        getErrorMessage(
                                            error
                                        )
                                    )
                            );

                            return;
                        }

                        if (returned) {
                            reject(
                                new UnroutableMessageError(
                                    returned.exchange,
                                    returned.routingKey,
                                    returned.replyCode,
                                    returned.replyText
                                )
                            );

                            return;
                        }

                        resolve();
                    };

                try {
                    this.channel.publish(
                        exchange,
                        routingKey,
                        content,
                        {
                            ...options,
                            mandatory: true,
                            headers: {
                                [PUBLICATION_ID_HEADER]:
                                    publicationId
                            }
                        },
                        settle
                    );
                } catch (error) {
                    // Publishing on a closed channel throws
                    // synchronously; no confirm will follow.
                    this.pending.delete(
                        publicationId
                    );

                    reject(
                        error
                    );
                }
            }
        );
    }
}
