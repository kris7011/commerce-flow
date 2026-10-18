import {
    randomUUID
} from "node:crypto";
import {
    ROUTING_KEYS
} from "@commerce-flow/contracts";
import {
    FLOW_TIMEOUT_IN_MS,
    SETTLE_TIME_IN_MS,
    check,
    endpoints,
    eventsOf,
    heading,
    poll,
    printTimeline,
    reportResult,
    runId,
    sleep,
    submitOrder,
    waitForNotification,
    waitForReadiness,
    type PlacedOrder
} from "./demoSupport.js";
import {
    EventTap
} from "./eventTap.js";
import {
    countDeadLetters,
    readOutboxDiagnostics,
    readQueueDepths,
    readStock,
    type QueueDepth
} from "./systemProbe.js";

/*
 * Reproducible walk-through of the order workflow against a running
 * system (`docker compose --profile app up` or `npm run dev:*`).
 *
 * The demo submits real orders over HTTP, observes every event on the
 * exchange, injects one duplicate event and then checks the results
 * that the services expose. It exits with status 1 if any expectation
 * fails, so CI can run it as a full-stack smoke test.
 */

async function successfulOrder(
    tap: EventTap
): Promise<PlacedOrder | undefined> {
    heading("2. A successful order");

    const stockBefore =
        await readStock(endpoints);

    const [productId, available] =
        Object.entries(stockBefore)
            .sort((left, right) => right[1] - left[1])[0] ??
        ["", 0];

    if (available < 1) {
        check(
            "a product with stock is available " +
            "(reset with: docker compose --profile app down --volumes)",
            false
        );

        return undefined;
    }

    const order =
        await submitOrder(
            `demo-${runId}-success`,
            {
                customerId: "demo-customer-1",
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
        !check("Order Service answers 201 Created", order.status === 201) ||
        !order.orderId
    ) {
        return undefined;
    }

    const orderId =
        order.orderId;

    const delivered =
        await tap.waitFor(
            messages =>
                eventsOf(messages, orderId, "DeliveryBooked").length > 0,
            FLOW_TIMEOUT_IN_MS
        );

    const placed = {
        ...order,
        orderId
    };

    printTimeline(tap, placed);

    check(
        "the order passes all five services and ends with DeliveryBooked",
        delivered
    );

    const notification =
        await waitForNotification(
            orderId,
            "DeliveryBooked"
        );

    if (notification) {
        console.log(`  Notification: "${notification}"`);
    }

    check(
        "Notification Service created a DeliveryBooked notification",
        notification !== undefined
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

    return placed;
}

async function unavailableOrder(
    tap: EventTap
): Promise<void> {
    heading("3. An order that cannot be fulfilled");

    const stockBefore =
        await readStock(endpoints);

    const [productId, available] =
        Object.entries(stockBefore)
            .sort((left, right) => left[1] - right[1])[0] ??
        ["dryer-01", 0];

    const order =
        await submitOrder(
            `demo-${runId}-out-of-stock`,
            {
                customerId: "demo-customer-2",
                items: [
                    {
                        productId,
                        quantity: available + 1,
                        unitPrice: 2999
                    }
                ]
            }
        );

    if (
        !check("Order Service answers 201 Created", order.status === 201) ||
        !order.orderId
    ) {
        return;
    }

    const orderId =
        order.orderId;

    const rejected =
        await tap.waitFor(
            messages =>
                eventsOf(
                    messages,
                    orderId,
                    "InventoryReservationFailed"
                ).length > 0,
            FLOW_TIMEOUT_IN_MS
        );

    printTimeline(tap, {
        ...order,
        orderId
    });

    check(
        "Inventory rejects the reservation (InventoryReservationFailed, no DeliveryBooked)",
        rejected &&
        eventsOf(tap.messages, orderId, "DeliveryBooked").length === 0
    );

    const notification =
        await waitForNotification(
            orderId,
            "InventoryReservationFailed"
        );

    if (notification) {
        console.log(`  Notification: "${notification}"`);
    }

    check(
        "the customer is notified about the failed reservation",
        notification !== undefined
    );

    const stockAfter =
        await readStock(endpoints);

    check(
        `stock of ${productId} is unchanged (${available})`,
        stockAfter[productId] === available
    );
}

async function duplicateEvent(
    tap: EventTap,
    order: PlacedOrder
): Promise<void> {
    heading("4. A duplicate PaymentAuthorized with a new event id");

    const original =
        eventsOf(
            tap.messages,
            order.orderId,
            "PaymentAuthorized"
        )[0];

    if (
        !original ||
        typeof original.body !== "object" ||
        original.body === null
    ) {
        check("the original PaymentAuthorized event was observed", false);
        return;
    }

    // A logically identical event under a new id: the in-memory event id
    // cache cannot recognise it, so only Inventory's durable business-key
    // idempotency (one reservation per order id) protects the stock.
    const duplicate = {
        ...(original.body as Record<string, unknown>),
        eventId: randomUUID(),
        eventType: "PaymentAuthorized",
        correlationId: original.envelope.correlationId
    };

    const stockBefore =
        await readStock(endpoints);

    await tap.publish(
        ROUTING_KEYS.PaymentAuthorized,
        duplicate
    );

    console.log(
        `  Published PaymentAuthorized ${duplicate.eventId} ` +
        `for order ${order.orderId} again`
    );

    // The tap sees the duplicate once the broker has routed it; then
    // allow Inventory time to consume it and emit any (wrong) follow-up.
    await tap.waitFor(
        messages =>
            messages.some(
                message =>
                    message.envelope.eventId ===
                    duplicate.eventId
            ),
        FLOW_TIMEOUT_IN_MS
    );

    await poll(
        () => readQueueDepths(endpoints),
        queues =>
            queues.find(
                queue =>
                    queue.name === "inventory-service.payment-authorized"
            )?.messages === 0,
        FLOW_TIMEOUT_IN_MS
    );

    await sleep(SETTLE_TIME_IN_MS);

    printTimeline(tap, order);

    const reservations =
        new Set(
            eventsOf(
                tap.messages,
                order.orderId,
                "InventoryReserved"
            ).map(message => message.envelope.eventId)
        );

    check(
        "Inventory ignores the duplicate: still exactly one InventoryReserved",
        reservations.size === 1
    );

    const stockAfter =
        await readStock(endpoints);

    check(
        "stock is unchanged by the duplicate",
        JSON.stringify(stockAfter) === JSON.stringify(stockBefore)
    );
}

async function invalidOrder(
    tap: EventTap
): Promise<void> {
    heading("5. An invalid order request");

    const correlationId =
        `demo-${runId}-invalid`;

    const order =
        await submitOrder(
            correlationId,
            {
                customerId: "demo-customer-3",
                items: []
            }
        );

    check(
        "Order Service answers 400 Bad Request",
        order.status === 400
    );

    await sleep(SETTLE_TIME_IN_MS);

    check(
        "no event is published for the rejected request",
        !tap.messages.some(
            message =>
                message.envelope.correlationId === correlationId
        )
    );
}

async function summary(
    deadLettersBefore: number
): Promise<void> {
    heading("6. System state after the demo");

    const outbox =
        await poll(
            () => readOutboxDiagnostics(endpoints),
            diagnostics => diagnostics?.pendingCount === 0,
            10_000
        );

    console.log(
        `  Outbox: ${JSON.stringify(outbox ?? "not reachable")}`
    );

    check(
        "the outbox has no pending events",
        outbox?.pendingCount === 0
    );

    const queues =
        await readQueueDepths(endpoints);

    printQueues(queues);

    check(
        "no message was dead-lettered during the demo",
        countDeadLetters(queues) === deadLettersBefore
    );
}

function printQueues(
    queues: readonly QueueDepth[]
): void {
    console.log("  Queues (messages waiting / consumers):");

    for (const queue of queues) {
        console.log(
            `    ${queue.name.padEnd(52)}` +
            (queue.missing
                ? "missing"
                : `${String(queue.messages).padStart(4)} / ${queue.consumers}`)
        );
    }
}

async function main(): Promise<number> {
    console.log(
        `CommerceFlow demo (run ${runId})\n` +
        `Order Service: ${endpoints.services["order-service"]}`
    );

    if (!await waitForReadiness("1. System readiness")) {
        console.log(
            "\nStart the system first, for example:\n" +
            "  docker compose --profile app up -d --build --wait"
        );

        return 1;
    }

    const deadLettersBefore =
        countDeadLetters(
            await readQueueDepths(endpoints)
        );

    const tap =
        await EventTap.open(
            endpoints.rabbitMqUrl
        );

    try {
        const placed =
            await successfulOrder(tap);

        await unavailableOrder(tap);

        if (placed) {
            await duplicateEvent(tap, placed);
        }

        await invalidOrder(tap);
        await summary(deadLettersBefore);
    } finally {
        await tap.close();
    }

    const exitCode =
        reportResult();

    console.log(
        `\n  Follow one order through all service logs with:\n` +
        `    docker compose --profile app logs --no-log-prefix | ` +
        `npm run --silent trace -- demo-${runId}-success`
    );

    return exitCode;
}

process.exitCode =
    await main().catch((error: unknown) => {
        console.error(
            `\nThe demo could not run: ` +
            `${error instanceof Error ? error.message : String(error)}`
        );

        return 1;
    });
