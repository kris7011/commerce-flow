/*
 * Follows one order through the structured JSON logs of all services.
 *
 * Every service writes one JSON object per line. The trace selects the
 * lines that mention the given correlation id or order id, and also the
 * lines that only carry an event id or order id that belongs to the
 * same workflow (for example the outbox relay's publication log).
 */

export interface LogEntry {
    readonly timestamp: string;
    readonly level: string;
    readonly service: string;
    readonly message: string;
    readonly context: Readonly<Record<string, unknown>>;
    readonly errorMessage: string | undefined;
}

/**
 * Parses one log line. Accepts the raw JSON written by the services as
 * well as lines prefixed by `docker compose logs` ("order-service-1  | ").
 * Returns undefined for anything that is not a structured log entry.
 */
export function parseLogLine(
    line: string
): LogEntry | undefined {
    const start =
        line.indexOf("{");

    if (start < 0) {
        return undefined;
    }

    let value: unknown;

    try {
        value = JSON.parse(
            line.slice(start)
        );
    } catch {
        return undefined;
    }

    if (
        !isRecord(value) ||
        typeof value.timestamp !== "string" ||
        typeof value.service !== "string" ||
        typeof value.message !== "string"
    ) {
        return undefined;
    }

    return {
        timestamp:
            value.timestamp,
        level:
            typeof value.level === "string"
                ? value.level
                : "info",
        service:
            value.service,
        message:
            value.message,
        context:
            isRecord(value.context)
                ? value.context
                : {},
        errorMessage:
            isRecord(value.error) &&
            typeof value.error.message === "string"
                ? value.error.message
                : undefined
    };
}

/**
 * Selects the entries of one workflow, ordered by timestamp. `id` may
 * be a correlation id or an order id.
 */
export function selectTrace(
    lines: Iterable<string>,
    id: string
): LogEntry[] {
    const entries: LogEntry[] = [];

    for (const line of lines) {
        const entry =
            parseLogLine(line);

        if (entry) {
            entries.push(entry);
        }
    }

    // First pass: entries that name the workflow directly. They tell us
    // the order ids and event ids that belong to it.
    const orderIds =
        new Set<string>([id]);

    const eventIds =
        new Set<string>();

    for (const entry of entries) {
        if (
            entry.context.correlationId === id ||
            entry.context.orderId === id
        ) {
            addString(orderIds, entry.context.orderId);
            addString(eventIds, entry.context.eventId);
        }
    }

    return entries
        .filter(entry => {
            const {
                correlationId,
                orderId,
                eventId
            } = entry.context;

            return (
                correlationId === id ||
                (typeof orderId === "string" && orderIds.has(orderId)) ||
                (typeof eventId === "string" && eventIds.has(eventId))
            );
        })
        .sort(
            (left, right) =>
                left.timestamp.localeCompare(
                    right.timestamp
                )
        );
}

/** Context keys that are shown elsewhere or add only noise. */
const HIDDEN_CONTEXT_KEYS =
    new Set([
        "correlationId",
        "component",
        "exchangeName"
    ]);

export function formatLogEntry(
    entry: LogEntry
): string {
    const time =
        entry.timestamp.slice(11, 23);

    const details =
        Object.entries(entry.context)
            .filter(
                ([key, value]) =>
                    !HIDDEN_CONTEXT_KEYS.has(key) &&
                    isScalar(value)
            )
            .map(
                ([key, value]) =>
                    `${key}=${String(value)}`
            );

    if (entry.errorMessage) {
        details.push(
            `error="${entry.errorMessage}"`
        );
    }

    return (
        `${time}  ${entry.service.padEnd(21)}` +
        `${entry.level.padEnd(6)}${entry.message}` +
        (details.length > 0
            ? `  ${details.join(" ")}`
            : "")
    );
}

function addString(
    target: Set<string>,
    value: unknown
): void {
    if (typeof value === "string") {
        target.add(value);
    }
}

function isScalar(
    value: unknown
): boolean {
    return (
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
    );
}

function isRecord(
    value: unknown
): value is Record<string, unknown> {
    return (
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
    );
}
