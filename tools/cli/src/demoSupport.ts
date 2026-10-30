import {
    randomUUID
} from "node:crypto";
import {
    resolveEndpoints
} from "./endpoints.js";
import type {
    EventTap
} from "./eventTap.js";
import {
    readNotifications,
    readReadiness,
    requestJson
} from "./systemProbe.js";
import {
    buildOrderTimeline,
    formatTimeline,
    type ObservedMessage
} from "./timeline.js";

/*
 * Shared building blocks of the demo scripts: configuration, checks
 * with a PASS/FAIL report, polling, order submission and timelines.
 */

export const endpoints =
    resolveEndpoints(
        process.env
    );

export const READY_TIMEOUT_IN_MS =
    Number(process.env.DEMO_READY_TIMEOUT_MS ?? 60_000);

export const FLOW_TIMEOUT_IN_MS =
    Number(process.env.DEMO_FLOW_TIMEOUT_MS ?? 30_000);

/** How long the demo waits to show that a duplicate changed nothing. */
export const SETTLE_TIME_IN_MS =
    2_000;

/**
 * Part of every correlation id the demo sends, so its orders can be
 * found in the logs afterwards. DEMO_RUN_ID fixes it (used by CI).
 */
export const runId =
    process.env.DEMO_RUN_ID?.trim() ||
    new Date()
        .toISOString()
        .replace(/[-:.TZ]/g, "")
        .slice(0, 14);

// Order Service replaces correlation ids outside this pattern.
if (!/^[A-Za-z0-9._:-]{1,64}$/.test(runId)) {
    throw new Error(
        "DEMO_RUN_ID may only contain letters, digits and . _ : - " +
        "(at most 64 characters)."
    );
}

export interface Check {
    readonly description: string;
    readonly passed: boolean;
}

export const checks: Check[] = [];

export function check(
    description: string,
    passed: boolean
): boolean {
    checks.push({
        description,
        passed
    });

    console.log(
        `  ${passed ? "PASS" : "FAIL"}  ${description}`
    );

    return passed;
}

export function heading(
    title: string
): void {
    console.log(
        `\n== ${title} ${"=".repeat(Math.max(0, 66 - title.length))}`
    );
}

export async function poll<T>(
    read: () => Promise<T>,
    done: (value: T) => boolean,
    timeoutInMs: number,
    intervalInMs = 500
): Promise<T> {
    const deadline =
        Date.now() + timeoutInMs;

    for (;;) {
        const value =
            await read();

        if (
            done(value) ||
            Date.now() >= deadline
        ) {
            return value;
        }

        await new Promise(resolve =>
            setTimeout(resolve, intervalInMs)
        );
    }
}

export async function waitForReadiness(
    title: string
): Promise<boolean> {
    heading(title);

    const readiness =
        await poll(
            () => readReadiness(endpoints),
            services => services.every(service => service.ready),
            READY_TIMEOUT_IN_MS,
            1_000
        );

    for (const service of readiness) {
        console.log(
            `  ${service.ready ? "ready    " : "NOT READY"}  ` +
            `${service.service.padEnd(22)}${service.detail}`
        );
    }

    return check(
        "all five services report /ready",
        readiness.every(service => service.ready)
    );
}

export interface SubmittedOrder {
    readonly status: number;
    readonly orderId: string | undefined;
    readonly submittedAt: number;
    readonly idempotencyKey: string;
}

/**
 * POST /orders with an Idempotency-Key. Pass the key of an earlier
 * submission to retry it; a new key is generated otherwise.
 */
export async function submitOrder(
    correlationId: string,
    body: unknown,
    idempotencyKey: string = randomUUID()
): Promise<SubmittedOrder> {
    const submittedAt =
        Date.now();

    const response =
        await requestJson(
            `${endpoints.services["order-service"]}/orders`,
            {
                method: "POST",
                headers: {
                    "content-type": "application/json",
                    "x-correlation-id": correlationId,
                    "idempotency-key": idempotencyKey
                },
                body: JSON.stringify(body)
            }
        );

    const orderId =
        typeof response.body === "object" &&
        response.body !== null &&
        "orderId" in response.body &&
        typeof response.body.orderId === "string"
            ? response.body.orderId
            : undefined;

    console.log(
        `  POST /orders  x-correlation-id: ${correlationId}` +
        `  Idempotency-Key: ${idempotencyKey.slice(0, 8)}…` +
        `  ->  HTTP ${response.status}` +
        (orderId ? `  orderId ${orderId}` : "")
    );

    return {
        status: response.status,
        orderId,
        submittedAt,
        idempotencyKey
    };
}

export interface OrderTimelineSnapshot {
    readonly status: string;
    readonly outcome: string;
    readonly entries: readonly {
        readonly kind: string;
        readonly title: string;
        readonly service: string;
        readonly simulated: boolean;
    }[];
}

/** The order as the Order Service's timeline endpoint shows it. */
export async function readOrderTimeline(
    orderId: string
): Promise<OrderTimelineSnapshot | undefined> {
    const response =
        await requestJson(
            `${endpoints.services["order-service"]}/orders/${orderId}`
        );

    if (
        response.status !== 200 ||
        typeof response.body !== "object" ||
        response.body === null
    ) {
        return undefined;
    }

    const body =
        response.body as {
            order: {
                status: string;
            };
            outcome: string;
            entries: OrderTimelineSnapshot["entries"];
        };

    return {
        status:
            body.order.status,
        outcome:
            body.outcome,
        entries:
            body.entries
    };
}

export function eventsOf(
    messages: readonly ObservedMessage[],
    orderId: string,
    eventType: string
): ObservedMessage[] {
    return messages.filter(
        message =>
            message.envelope.orderId === orderId &&
            message.envelope.eventType === eventType
    );
}

export async function waitForNotification(
    orderId: string,
    type: string
): Promise<string | undefined> {
    const notifications =
        await poll(
            () => readNotifications(endpoints),
            list => list.some(
                notification =>
                    notification.orderId === orderId &&
                    notification.type === type
            ),
            10_000
        );

    return notifications.find(
        notification =>
            notification.orderId === orderId &&
            notification.type === type
    )?.message;
}

export function printTimeline(
    tap: EventTap,
    order: PlacedOrder
): void {
    const timeline =
        buildOrderTimeline(
            order.orderId,
            tap.messages,
            order.submittedAt
        );

    console.log(
        `  Events on commerce.events (outcome: ${timeline.outcome}):`
    );

    for (const line of formatTimeline(timeline)) {
        console.log(line);
    }
}

export type PlacedOrder =
    SubmittedOrder & {
        readonly orderId: string;
    };

export function sleep(
    milliseconds: number
): Promise<void> {
    return new Promise(resolve =>
        setTimeout(resolve, milliseconds)
    );
}

/** Prints the result block and returns the process exit code. */
export function reportResult(): number {
    const failed =
        checks.filter(entry => !entry.passed);

    heading("Result");

    console.log(
        failed.length === 0
            ? `  All ${checks.length} checks passed.`
            : `  ${failed.length} of ${checks.length} checks failed:\n` +
            failed
                .map(entry => `    - ${entry.description}`)
                .join("\n")
    );

    return failed.length === 0
        ? 0
        : 1;
}
