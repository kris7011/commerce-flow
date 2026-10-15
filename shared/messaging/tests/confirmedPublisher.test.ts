import assert from "node:assert/strict";
import test from "node:test";
import {
    ConfirmedPublisher,
    PUBLICATION_ID_HEADER,
    type ConfirmChannelLike,
    type ConfirmPublishOptions,
    type ReturnedMessage
} from "../src/confirmedPublisher.js";
import {
    UnroutableMessageError
} from "../src/messageErrors.js";

interface PublishedFrame {
    readonly routingKey: string;
    readonly options: ConfirmPublishOptions;
    readonly callback: (error: unknown) => void;
}

/**
 * Mimics an amqplib confirm channel. Tests drive the broker's replies
 * explicitly and in protocol order: a basic.return for a message is
 * always delivered before its basic.ack.
 */
class FakeConfirmChannel
    implements ConfirmChannelLike {
    readonly frames: PublishedFrame[] = [];

    throwOnPublish:
        Error | undefined;

    private returnListener:
        ((message: ReturnedMessage) => void) | undefined;

    publish(
        _exchange: string,
        routingKey: string,
        _content: Buffer,
        options: ConfirmPublishOptions,
        callback: (error: unknown) => void
    ): boolean {
        if (this.throwOnPublish) {
            throw this.throwOnPublish;
        }

        this.frames.push({
            routingKey,
            options,
            callback
        });

        return true;
    }

    on(
        _event: "return",
        listener: (message: ReturnedMessage) => void
    ): this {
        this.returnListener =
            listener;

        return this;
    }

    returnMessage(
        index: number
    ): void {
        const frame =
            this.frame(
                index
            );

        this.returnListener?.({
            fields: {
                replyCode: 312,
                replyText: "NO_ROUTE",
                exchange: "commerce.events",
                routingKey:
                    frame.routingKey
            },
            properties: {
                headers:
                    frame.options.headers
            }
        });
    }

    ack(
        index: number
    ): void {
        this.frame(
            index
        ).callback(
            null
        );
    }

    nack(
        index: number
    ): void {
        this.frame(
            index
        ).callback(
            new Error(
                "message nacked"
            )
        );
    }

    private frame(
        index: number
    ): PublishedFrame {
        const frame =
            this.frames[index];

        assert.ok(
            frame,
            `No publication at index ${index}.`
        );

        return frame;
    }
}

function publish(
    publisher: ConfirmedPublisher,
    routingKey: string,
    messageId = "event-001"
): Promise<void> {
    return publisher.publish(
        "commerce.events",
        routingKey,
        Buffer.from(
            "{}"
        ),
        {
            contentType: "application/json",
            deliveryMode: 2,
            messageId,
            correlationId: "correlation-001",
            type: "TestEvent"
        }
    );
}

function sequentialIds(): () => string {
    let next = 0;

    return () => {
        next += 1;

        return `publication-${next}`;
    };
}

test(
    "publishes as mandatory with a unique publication id",
    async () => {
        const channel =
            new FakeConfirmChannel();

        const publisher =
            new ConfirmedPublisher(
                channel,
                sequentialIds()
            );

        const first =
            publish(
                publisher,
                "order.created"
            );

        const second =
            publish(
                publisher,
                "order.created"
            );

        assert.equal(
            channel.frames[0]?.options.mandatory,
            true
        );

        assert.deepEqual(
            channel.frames.map(
                frame =>
                    frame.options.headers[
                    PUBLICATION_ID_HEADER
                    ]
            ),
            [
                "publication-1",
                "publication-2"
            ]
        );

        channel.ack(0);
        channel.ack(1);

        await Promise.all([
            first,
            second
        ]);

        assert.equal(
            publisher.pendingCount,
            0
        );
    }
);

test(
    "fails a returned publication even though the broker then acks it",
    async () => {
        const channel =
            new FakeConfirmChannel();

        const publisher =
            new ConfirmedPublisher(
                channel
            );

        const publication =
            publish(
                publisher,
                "nobody.listens"
            );

        channel.returnMessage(0);
        channel.ack(0);

        await assert.rejects(
            publication,
            (error: unknown) => {
                assert.ok(
                    error instanceof UnroutableMessageError
                );

                assert.equal(
                    error.routingKey,
                    "nobody.listens"
                );

                assert.equal(
                    error.replyCode,
                    312
                );

                return true;
            }
        );

        assert.equal(
            publisher.pendingCount,
            0
        );
    }
);

test(
    "attributes returns to the right publication among concurrent ones",
    async () => {
        const channel =
            new FakeConfirmChannel();

        const publisher =
            new ConfirmedPublisher(
                channel
            );

        // The same message id on purpose: correlation must not rely
        // on it, because an event can be published twice concurrently.
        const publications =
            [
                "routable.a",
                "unroutable",
                "routable.b",
                "unroutable",
                "routable.c"
            ].map(
                routingKey =>
                    publish(
                        publisher,
                        routingKey,
                        "same-event-id"
                    )
            );

        channel.returnMessage(1);
        channel.ack(0);
        channel.ack(1);
        channel.ack(2);
        channel.returnMessage(3);
        channel.ack(3);
        channel.ack(4);

        const results =
            await Promise.allSettled(
                publications
            );

        assert.deepEqual(
            results.map(
                result =>
                    result.status
            ),
            [
                "fulfilled",
                "rejected",
                "fulfilled",
                "rejected",
                "fulfilled"
            ]
        );
    }
);

test(
    "rejects a nacked publication",
    async () => {
        const channel =
            new FakeConfirmChannel();

        const publisher =
            new ConfirmedPublisher(
                channel
            );

        const publication =
            publish(
                publisher,
                "order.created"
            );

        channel.nack(0);

        await assert.rejects(
            publication,
            /message nacked/
        );

        assert.equal(
            publisher.pendingCount,
            0
        );
    }
);

test(
    "ignores returns that belong to no pending publication",
    async () => {
        const channel =
            new FakeConfirmChannel();

        const publisher =
            new ConfirmedPublisher(
                channel
            );

        const publication =
            publish(
                publisher,
                "order.created"
            );

        channel.ack(0);

        await publication;

        // A late or foreign return must neither throw nor leak state.
        channel.returnMessage(0);

        assert.equal(
            publisher.pendingCount,
            0
        );
    }
);

test(
    "rejects without leaking state when the channel is already closed",
    async () => {
        const channel =
            new FakeConfirmChannel();

        channel.throwOnPublish =
            new Error(
                "Channel closed"
            );

        const publisher =
            new ConfirmedPublisher(
                channel
            );

        await assert.rejects(
            publish(
                publisher,
                "order.created"
            ),
            /Channel closed/
        );

        assert.equal(
            publisher.pendingCount,
            0
        );
    }
);
