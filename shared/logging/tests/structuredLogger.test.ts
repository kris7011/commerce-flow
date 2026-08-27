import assert from "node:assert/strict";
import test from "node:test";
import {
    createStructuredLogger,
    type LogWriter,
    type StructuredLogEntry
} from "../src/index.js";

const fixedTime =
    "2026-08-27T07:00:00.000Z";

test(
    "writes a structured info log",
    () => {
        const writer =
            new RecordingLogWriter();

        const logger =
            createStructuredLogger(
                "order-service",
                {
                    getCurrentTime:
                        () => fixedTime,
                    writer
                }
            );

        logger.info(
            "Created order",
            {
                orderId:
                    "order-001",
                correlationId:
                    "correlation-001"
            }
        );

        assert.equal(
            writer.logs.length,
            1
        );

        assert.deepEqual(
            parseEntry(
                writer.logs[0]
            ),
            {
                timestamp:
                    fixedTime,
                level:
                    "info",
                service:
                    "order-service",
                message:
                    "Created order",
                context: {
                    orderId:
                        "order-001",
                    correlationId:
                        "correlation-001"
                }
            }
        );
    }
);

test(
    "writes warning logs to the warning writer",
    () => {
        const writer =
            new RecordingLogWriter();

        const logger =
            createStructuredLogger(
                "messaging",
                {
                    getCurrentTime:
                        () => fixedTime,
                    writer
                }
            );

        logger.warn(
            "Duplicate event",
            {
                eventId:
                    "event-001"
            }
        );

        assert.equal(
            writer.warnings.length,
            1
        );

        assert.equal(
            writer.logs.length,
            0
        );

        assert.equal(
            parseEntry(
                writer.warnings[0]
            ).level,
            "warn"
        );
    }
);

test(
    "serializes Error instances",
    () => {
        const writer =
            new RecordingLogWriter();

        const logger =
            createStructuredLogger(
                "payment-service",
                {
                    getCurrentTime:
                        () => fixedTime,
                    writer
                }
            );

        logger.error(
            "Payment processing failed",
            new Error(
                "Provider unavailable"
            ),
            {
                orderId:
                    "order-001"
            }
        );

        assert.equal(
            writer.errors.length,
            1
        );

        const entry =
            parseEntry(
                writer.errors[0]
            );

        assert.equal(
            entry.level,
            "error"
        );

        assert.equal(
            entry.error?.name,
            "Error"
        );

        assert.equal(
            entry.error?.message,
            "Provider unavailable"
        );

        assert.equal(
            entry.context?.orderId,
            "order-001"
        );
    }
);

test(
    "child loggers include inherited context",
    () => {
        const writer =
            new RecordingLogWriter();

        const logger =
            createStructuredLogger(
                "inventory-service",
                {
                    getCurrentTime:
                        () => fixedTime,
                    writer
                }
            );

        const orderLogger =
            logger.child({
                correlationId:
                    "correlation-001",
                orderId:
                    "order-001"
            });

        orderLogger.info(
            "Reserved inventory",
            {
                reservationId:
                    "reservation-001"
            }
        );

        assert.deepEqual(
            parseEntry(
                writer.logs[0]
            ).context,
            {
                correlationId:
                    "correlation-001",
                orderId:
                    "order-001",
                reservationId:
                    "reservation-001"
            }
        );
    }
);

test(
    "per-call context overrides inherited context",
    () => {
        const writer =
            new RecordingLogWriter();

        const logger =
            createStructuredLogger(
                "delivery-service",
                {
                    getCurrentTime:
                        () => fixedTime,
                    writer
                }
            );

        const childLogger =
            logger.child({
                orderId:
                    "order-001"
            });

        childLogger.info(
            "Test log",
            {
                orderId:
                    "order-002"
            }
        );

        assert.equal(
            parseEntry(
                writer.logs[0]
            ).context?.orderId,
            "order-002"
        );
    }
);

class RecordingLogWriter
    implements LogWriter {
    readonly logs:
        string[] = [];

    readonly warnings:
        string[] = [];

    readonly errors:
        string[] = [];

    log(message: string): void {
        this.logs.push(message);
    }

    warn(message: string): void {
        this.warnings.push(message);
    }

    error(message: string): void {
        this.errors.push(message);
    }
}

function parseEntry(
    serializedEntry:
        string | undefined
): StructuredLogEntry {
    assert.ok(
        serializedEntry
    );

    return JSON.parse(
        serializedEntry
    ) as StructuredLogEntry;
}