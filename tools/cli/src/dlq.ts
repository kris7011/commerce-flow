import * as amqp from "amqplib";
import {
    COMMERCE_TOPOLOGY
} from "@commerce-flow/contracts";
import {
    resolveEndpoints
} from "./endpoints.js";
import {
    inspectDeadLetters,
    replayDeadLetters,
    resolveQueuePair,
    type DeadLetterSummary
} from "./deadLetters.js";

/*
 *   npm run dlq                         every dead-letter queue, with samples
 *   npm run dlq -- list <queue>         up to 20 messages of one queue
 *   npm run dlq -- replay <queue> [--limit <n>]
 *
 * <queue> is a consumer queue (e.g. inventory-service.payment-authorized)
 * or its dead-letter queue. Listing never removes messages.
 */

const USAGE =
    "Usage:\n" +
    "  npm run dlq [-- list [<queue>]]\n" +
    "  npm run dlq -- replay <queue> [--limit <n>]\n\n" +
    "Queues: " +
    COMMERCE_TOPOLOGY.map(queue => queue.queueName).join(", ");

function formatSummary(
    summary: DeadLetterSummary
): string {
    const death =
        summary.death
            ? `${summary.death.reason} by ${summary.death.queue}` +
            (summary.death.time ? ` at ${summary.death.time}` : "") +
            (summary.death.count > 1 ? ` (${summary.death.count} times)` : "")
            : "no x-death header";

    return (
        `    ${summary.eventType ?? "unknown type"}  ` +
        `event ${summary.messageId ?? "?"}  ` +
        `order ${summary.orderId ?? "?"}  ` +
        `correlation ${summary.correlationId ?? "?"}\n` +
        `      ${death}` +
        (summary.replayCount > 0 ? `, replayed ${summary.replayCount} time(s) before` : "")
    );
}

async function list(
    connection: amqp.ChannelModel,
    queueName: string | undefined
): Promise<number> {
    const queues =
        queueName
            ? [resolveQueuePair(queueName)]
            : COMMERCE_TOPOLOGY.map(queue => resolveQueuePair(queue.queueName));

    const limit =
        queueName ? 20 : 3;

    let total = 0;

    for (const queue of queues) {
        const inspection =
            await inspectDeadLetters(
                connection,
                queue,
                limit
            );

        total += inspection.total;

        console.log(
            `${queue.deadLetterQueueName}: ${inspection.total} message(s)`
        );

        for (const summary of inspection.messages) {
            console.log(formatSummary(summary));
        }

        if (inspection.total > inspection.messages.length) {
            console.log(
                `    … ${inspection.total - inspection.messages.length} more`
            );
        }
    }

    if (total > 0) {
        console.log(
            "\nFix the cause first (see the services' error logs), then:\n" +
            "  npm run dlq -- replay <queue>"
        );
    }

    return 0;
}

async function replay(
    connection: amqp.ChannelModel,
    queueName: string,
    limit: number | undefined
): Promise<number> {
    const queue =
        resolveQueuePair(queueName);

    const result =
        await replayDeadLetters(
            connection,
            queue,
            {
                limit,
                onReplayed: summary => {
                    console.log(
                        `  replayed ${summary.eventType ?? "message"} ` +
                        `${summary.messageId ?? ""} ` +
                        `(order ${summary.orderId ?? "?"}) -> ${queue.queueName}`
                    );
                }
            }
        );

    console.log(
        `\nReplayed ${result.replayed} message(s) from ` +
        `${queue.deadLetterQueueName}; ${result.remaining} remaining.`
    );

    return 0;
}

function parseLimit(
    args: readonly string[]
): number | undefined {
    const index =
        args.indexOf("--limit");

    if (index < 0) {
        return undefined;
    }

    const value =
        Number(args[index + 1]);

    if (!Number.isInteger(value) || value < 1) {
        throw new Error("--limit expects a positive integer.");
    }

    return value;
}

async function main(): Promise<number> {
    const [command = "list", queueName, ...rest] =
        process.argv.slice(2);

    if (command !== "list" && command !== "replay") {
        console.error(USAGE);
        return 2;
    }

    if (command === "replay" && !queueName) {
        console.error(USAGE);
        return 2;
    }

    const connection =
        await amqp.connect(
            resolveEndpoints(process.env).rabbitMqUrl
        );

    try {
        return command === "list"
            ? await list(connection, queueName)
            : await replay(connection, queueName ?? "", parseLimit(rest));
    } finally {
        await connection
            .close()
            .catch(() => undefined);
    }
}

process.exitCode =
    await main().catch((error: unknown) => {
        console.error(
            error instanceof Error
                ? error.message
                : String(error)
        );

        return 1;
    });
