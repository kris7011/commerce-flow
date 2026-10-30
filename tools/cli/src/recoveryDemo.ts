import {
    spawnSync
} from "node:child_process";
import * as amqp from "amqplib";
import {
    inspectDeadLetters,
    replayDeadLetters,
    resolveQueuePair
} from "./deadLetters.js";
import {
    FLOW_TIMEOUT_IN_MS,
    check,
    endpoints,
    eventsOf,
    heading,
    poll,
    printTimeline,
    readOrderTimeline,
    reportResult,
    runId,
    submitOrder,
    waitForNotification,
    waitForReadiness
} from "./demoSupport.js";
import {
    EventTap
} from "./eventTap.js";
import {
    readQueueDepths,
    readReadiness,
    readStock
} from "./systemProbe.js";

/*
 * Outage recovery through the dead-letter queue, against the
 * containerised stack (`npm run stack:up`):
 *
 * 1. Stop Payment Service and submit an order. The order is stored
 *    (Order Service needs PostgreSQL) and its OrderCreated event waits in
 *    Payment's queue.
 * 2. Stop PostgreSQL, then start Payment Service. Inventory cannot reach
 *    its database, retries PaymentAuthorized in-process and then
 *    dead-letters it. The Order Service's timeline consumer cannot record
 *    the event either and dead-letters its copy as well.
 * 3. Start PostgreSQL again and wait until Inventory is ready.
 * 4. Replay both dead-letter queues.
 * 5. The order completes, stock drops exactly once, the queues are empty
 *    and the order timeline shows every step.
 *
 * The script controls containers with `docker compose`, so it needs the
 * Compose stack, not services started with npm run dev.
 */

const INVENTORY_QUEUE =
    resolveQueuePair(
        "inventory-service.payment-authorized"
    );

const ORDER_TIMELINE_QUEUE =
    resolveQueuePair(
        "order-service.order-events"
    );

function compose(
    ...args: string[]
): boolean {
    console.log(`  $ docker compose ${args.join(" ")}`);

    const result =
        spawnSync(
            "docker",
            ["compose", ...args],
            {
                stdio: ["ignore", "ignore", "inherit"]
            }
        );

    return result.status === 0;
}

async function inventoryDatabaseStatus(): Promise<string> {
    const readiness =
        await readReadiness(endpoints);

    return readiness.find(
        service => service.service === "inventory-service"
    )?.detail ?? "";
}

async function deadLetterCount(
    deadLetterQueueName =
        INVENTORY_QUEUE.deadLetterQueueName
): Promise<number> {
    const queues =
        await readQueueDepths(endpoints);

    return queues.find(
        queue => queue.name === deadLetterQueueName
    )?.messages ?? 0;
}

interface Cleanup {
    startDatabase: boolean;
    startPayment: boolean;
}

async function run(
    tap: EventTap,
    connection: amqp.ChannelModel,
    cleanup: Cleanup
): Promise<void> {
    for (const queue of [INVENTORY_QUEUE, ORDER_TIMELINE_QUEUE]) {
        if (
            !check(
                `${queue.deadLetterQueueName} is empty before the demo ` +
                "(inspect leftovers with: npm run dlq)",
                await deadLetterCount(queue.deadLetterQueueName) === 0
            )
        ) {
            return;
        }
    }

    const stockBefore =
        await readStock(endpoints);

    const [productId, available] =
        Object.entries(stockBefore)
            .sort((left, right) => right[1] - left[1])[0] ??
        ["", 0];

    if (!check(`${productId || "a product"} has stock`, available > 0)) {
        return;
    }

    heading("2. An order waits while Payment Service is stopped");

    cleanup.startPayment = true;

    if (!check("Payment Service stopped", compose("stop", "payment-service"))) {
        return;
    }

    const order =
        await submitOrder(
            `recovery-${runId}`,
            {
                customerId: "demo-customer-recovery",
                items: [
                    {
                        productId,
                        quantity: 1,
                        unitPrice: 4999
                    }
                ]
            }
        );

    if (
        !check(
            "Order Service stores the order (201); OrderCreated waits for Payment",
            order.status === 201
        ) ||
        !order.orderId
    ) {
        return;
    }

    const placed = {
        ...order,
        orderId: order.orderId
    };

    heading("3. PostgreSQL outage while the order is processed");

    cleanup.startDatabase = true;

    if (!check("PostgreSQL stopped", compose("stop", "postgres"))) {
        return;
    }

    const status =
        await poll(
            inventoryDatabaseStatus,
            detail => detail.includes("database: NotReady"),
            30_000
        );

    check(
        `Inventory reports its database as unavailable (${status})`,
        status.includes("database: NotReady")
    );

    if (!check("Payment Service started", compose("start", "payment-service"))) {
        return;
    }

    cleanup.startPayment = false;

    console.log(
        "  Inventory retries the message in-process (5 attempts with " +
        "backoff), then dead-letters it …"
    );

    const deadLetters =
        await poll(
            deadLetterCount,
            count => count >= 1,
            60_000,
            1_000
        );

    check(
        `PaymentAuthorized was moved to ${INVENTORY_QUEUE.deadLetterQueueName}`,
        deadLetters === 1
    );

    const inspection =
        await inspectDeadLetters(
            connection,
            INVENTORY_QUEUE
        );

    for (const summary of inspection.messages) {
        console.log(
            `  dead letter: ${summary.eventType} for order ${summary.orderId}, ` +
            `${summary.death?.reason ?? "?"} by ${summary.death?.queue ?? "?"}`
        );
    }

    check(
        "no InventoryReserved was published during the outage",
        eventsOf(tap.messages, placed.orderId, "InventoryReserved").length === 0
    );

    // The timeline consumer retries for a similar time; wait for it too.
    const timelineDeadLetters =
        await poll(
            () => deadLetterCount(ORDER_TIMELINE_QUEUE.deadLetterQueueName),
            count => count >= 1,
            30_000,
            1_000
        );

    console.log(
        `  ${ORDER_TIMELINE_QUEUE.deadLetterQueueName}: ${timelineDeadLetters} message(s) ` +
        "(the timeline could not record events without its database)"
    );

    heading("4. PostgreSQL is back");

    if (!check("PostgreSQL started", compose("start", "postgres"))) {
        return;
    }

    cleanup.startDatabase = false;

    const recovered =
        await poll(
            inventoryDatabaseStatus,
            detail => detail.includes("database: Ready"),
            60_000,
            1_000
        );

    check(
        "Inventory recovers without a restart",
        recovered.includes("database: Ready")
    );

    heading("5. Replay the dead letters");

    const result =
        await replayDeadLetters(
            connection,
            INVENTORY_QUEUE,
            {
                onReplayed: summary => {
                    console.log(
                        `  replayed ${summary.eventType} ${summary.messageId} ` +
                        `-> ${INVENTORY_QUEUE.queueName}`
                    );
                }
            }
        );

    check(
        "exactly one message was replayed",
        result.replayed === 1
    );

    // After Inventory, so the replayed timeline events find the order
    // and are recorded once; any later events arrive normally.
    const timelineReplay =
        await replayDeadLetters(
            connection,
            ORDER_TIMELINE_QUEUE,
            {
                onReplayed: summary => {
                    console.log(
                        `  replayed ${summary.eventType} ${summary.messageId} ` +
                        `-> ${ORDER_TIMELINE_QUEUE.queueName}`
                    );
                }
            }
        );

    check(
        "the order timeline's dead letters were replayed",
        timelineReplay.replayed === timelineDeadLetters
    );

    const delivered =
        await tap.waitFor(
            messages =>
                eventsOf(messages, placed.orderId, "DeliveryBooked").length > 0,
            FLOW_TIMEOUT_IN_MS
        );

    printTimeline(tap, placed);

    check(
        "the order now completes with DeliveryBooked",
        delivered
    );

    check(
        "Notification Service notified the customer",
        await waitForNotification(placed.orderId, "DeliveryBooked") !== undefined
    );

    const stockAfter =
        await readStock(endpoints);

    console.log(
        `  Stock of ${productId}: ${available} -> ${stockAfter[productId]}`
    );

    check(
        `stock of ${productId} decreased by exactly 1`,
        stockAfter[productId] === available - 1
    );

    check(
        `${INVENTORY_QUEUE.deadLetterQueueName} is empty again`,
        await deadLetterCount() === 0
    );

    const timeline =
        await poll(
            () => readOrderTimeline(placed.orderId),
            view => view?.status === "DeliveryBooked",
            FLOW_TIMEOUT_IN_MS
        );

    check(
        "the order timeline shows payment, reservation and delivery",
        timeline?.status === "DeliveryBooked" &&
        ["Payment authorized", "Stock reserved", "Delivery booked"].every(title =>
            timeline.entries.some(entry => entry.title === title)
        )
    );

    check(
        `${ORDER_TIMELINE_QUEUE.deadLetterQueueName} is empty again`,
        await deadLetterCount(ORDER_TIMELINE_QUEUE.deadLetterQueueName) === 0
    );
}

async function main(): Promise<number> {
    console.log(
        `CommerceFlow recovery demo (run ${runId})`
    );

    if (!await waitForReadiness("1. System readiness")) {
        console.log(
            "\nStart the containerised stack first:\n" +
            "  npm run stack:up"
        );

        return 1;
    }

    const cleanup: Cleanup = {
        startDatabase: false,
        startPayment: false
    };

    const tap =
        await EventTap.open(
            endpoints.rabbitMqUrl
        );

    const connection =
        await amqp.connect(
            endpoints.rabbitMqUrl
        );

    try {
        await run(tap, connection, cleanup);
    } finally {
        if (cleanup.startDatabase) {
            compose("start", "postgres");
        }

        if (cleanup.startPayment) {
            compose("start", "payment-service");
        }

        await connection
            .close()
            .catch(() => undefined);

        await tap.close();
    }

    const exitCode =
        reportResult();

    console.log(
        `\n  Follow the order through all service logs with:\n` +
        `    docker compose --profile app logs --no-log-prefix | ` +
        `npm run --silent trace -- recovery-${runId}`
    );

    return exitCode;
}

process.exitCode =
    await main().catch((error: unknown) => {
        console.error(
            `\nThe recovery demo could not run: ` +
            `${error instanceof Error ? error.message : String(error)}`
        );

        return 1;
    });
