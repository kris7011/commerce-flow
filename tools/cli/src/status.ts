import {
    resolveEndpoints
} from "./endpoints.js";
import {
    countDeadLetters,
    readNotifications,
    readOutboxDiagnostics,
    readQueueDepths,
    readReadiness,
    readStock
} from "./systemProbe.js";

/*
 * One-screen view of the running system: readiness of every service,
 * stock, the outbox backlog, queue depths including dead letters, and
 * the notifications created so far. Read-only.
 *
 * Exits with status 1 if a service is not ready or a dead-letter queue
 * holds messages, so it can also be used as a quick health check.
 */

const endpoints =
    resolveEndpoints(
        process.env
    );

async function main(): Promise<number> {
    let healthy = true;

    console.log("Services");

    for (const service of await readReadiness(endpoints)) {
        healthy &&= service.ready;

        console.log(
            `  ${service.ready ? "ready    " : "NOT READY"}  ` +
            `${service.service.padEnd(22)}${service.detail}`
        );
    }

    console.log("\nStock");

    try {
        for (const [productId, quantity] of Object.entries(await readStock(endpoints))) {
            console.log(`  ${productId.padEnd(24)}${quantity}`);
        }
    } catch (error) {
        console.log(`  unavailable (${describe(error)})`);
    }

    console.log("\nOutbox");

    const outbox =
        await readOutboxDiagnostics(endpoints);

    console.log(
        outbox
            ? `  ${JSON.stringify(outbox)}`
            : "  unavailable"
    );

    console.log("\nQueues (messages waiting / consumers)");

    try {
        const queues =
            await readQueueDepths(endpoints);

        for (const queue of queues) {
            console.log(
                `  ${queue.name.padEnd(52)}` +
                (queue.missing
                    ? "missing"
                    : `${String(queue.messages).padStart(4)} / ${queue.consumers}`)
            );
        }

        const deadLetters =
            countDeadLetters(queues);

        if (deadLetters > 0) {
            healthy = false;

            console.log(
                `  ${deadLetters} dead-lettered message(s) need attention.`
            );
        }
    } catch (error) {
        healthy = false;
        console.log(`  RabbitMQ unavailable (${describe(error)})`);
    }

    console.log("\nNotifications");

    try {
        const notifications =
            await readNotifications(endpoints);

        if (notifications.length === 0) {
            console.log("  none yet");
        }

        for (const notification of notifications.slice(-10)) {
            console.log(
                `  ${notification.orderId}  ` +
                `${notification.type.padEnd(28)}${notification.message}`
            );
        }

        if (notifications.length > 10) {
            console.log(`  (latest 10 of ${notifications.length})`);
        }
    } catch (error) {
        console.log(`  unavailable (${describe(error)})`);
    }

    return healthy
        ? 0
        : 1;
}

function describe(
    error: unknown
): string {
    return error instanceof Error
        ? error.message
        : String(error);
}

process.exitCode =
    await main();
