import assert from "node:assert/strict";
import test from "node:test";
import {
    formatLogEntry,
    parseLogLine,
    selectTrace
} from "../src/logTrace.js";

function line(
    timestamp: string,
    service: string,
    message: string,
    context: Record<string, unknown>
): string {
    return JSON.stringify({
        timestamp,
        level: "info",
        service,
        message,
        context
    });
}

test(
    "parses raw lines and lines prefixed by docker compose",
    () => {
        const raw =
            line("2026-10-18T10:00:00.000Z", "order-service", "Created order", {
                orderId: "order-1"
            });

        assert.equal(parseLogLine(raw)?.service, "order-service");

        assert.equal(
            parseLogLine(`order-service-1  | ${raw}`)?.message,
            "Created order"
        );

        for (const invalid of ["", "plain text", "{ not json", "{\"level\":\"info\"}"]) {
            assert.equal(parseLogLine(invalid), undefined);
        }
    }
);

test(
    "follows a correlation id to log lines that only carry its order or event id",
    () => {
        const lines = [
            line("2026-10-18T10:00:00.300Z", "inventory-service", "Published outbox event", {
                eventId: "reserved-1"
            }),
            line("2026-10-18T10:00:00.100Z", "order-service", "Created order", {
                correlationId: "demo-1",
                orderId: "order-1",
                eventId: "created-1"
            }),
            line("2026-10-18T10:00:00.200Z", "inventory-service", "Reserved inventory", {
                correlationId: "demo-1",
                orderId: "order-1",
                eventId: "reserved-1"
            }),
            line("2026-10-18T10:00:00.250Z", "payment-service", "Unrelated order", {
                correlationId: "demo-2",
                orderId: "order-2"
            }),
            line("2026-10-18T10:00:00.400Z", "notification-service", "Lookup", {
                orderId: "order-1"
            }),
            "not a log line"
        ];

        const trace =
            selectTrace(lines, "demo-1");

        assert.deepEqual(
            trace.map(entry => entry.message),
            [
                "Created order",
                "Reserved inventory",
                "Published outbox event",
                "Lookup"
            ]
        );

        assert.deepEqual(
            selectTrace(lines, "order-2").map(entry => entry.message),
            ["Unrelated order"]
        );
    }
);

test(
    "formats an entry with its scalar context and error message",
    () => {
        const entry =
            parseLogLine(
                JSON.stringify({
                    timestamp: "2026-10-18T10:00:00.123Z",
                    level: "error",
                    service: "payment-service",
                    message: "Failed to process message",
                    context: {
                        correlationId: "demo-1",
                        component: "messaging",
                        attempt: 5,
                        action: "dead-letter",
                        nested: { hidden: true }
                    },
                    error: {
                        name: "Error",
                        message: "boom"
                    }
                })
            );

        assert.ok(entry);

        assert.equal(
            formatLogEntry(entry),
            "10:00:00.123  payment-service      error Failed to process message" +
            "  attempt=5 action=dead-letter error=\"boom\""
        );
    }
);
