export type LogLevel =
    | "info"
    | "warn"
    | "error";

export type LogContext =
    Readonly<
        Record<string, unknown>
    >;

export interface StructuredError {
    readonly name: string;
    readonly message: string;
    readonly stack?: string;
}

export interface StructuredLogEntry {
    readonly timestamp: string;
    readonly level: LogLevel;
    readonly service: string;
    readonly message: string;
    readonly context?:
    Record<string, unknown>;
    readonly error?:
    StructuredError;
}

export interface LogWriter {
    log(message: string): void;

    warn(message: string): void;

    error(message: string): void;
}

export interface AppLogger {
    info(
        message: string,
        context?: LogContext
    ): void;

    warn(
        message: string,
        context?: LogContext
    ): void;

    error(
        message: string,
        error?: unknown,
        context?: LogContext
    ): void;

    child(
        context: LogContext
    ): AppLogger;
}

export interface StructuredLoggerOptions {
    readonly getCurrentTime?:
    () => string;

    readonly writer?:
    LogWriter;

    readonly baseContext?:
    LogContext;
}

export function createStructuredLogger(
    service: string,
    options:
        StructuredLoggerOptions = {}
): AppLogger {
    return new StructuredLogger(
        service,
        options.getCurrentTime ??
        (() =>
            new Date()
                .toISOString()),
        options.writer ??
        console,
        options.baseContext ?? {}
    );
}

class StructuredLogger
    implements AppLogger {
    constructor(
        private readonly service:
            string,

        private readonly getCurrentTime:
            () => string,

        private readonly writer:
            LogWriter,

        private readonly baseContext:
            LogContext
    ) {
    }

    info(
        message: string,
        context?: LogContext
    ): void {
        this.write(
            "info",
            message,
            undefined,
            context
        );
    }

    warn(
        message: string,
        context?: LogContext
    ): void {
        this.write(
            "warn",
            message,
            undefined,
            context
        );
    }

    error(
        message: string,
        error?: unknown,
        context?: LogContext
    ): void {
        this.write(
            "error",
            message,
            error,
            context
        );
    }

    child(
        context: LogContext
    ): AppLogger {
        return new StructuredLogger(
            this.service,
            this.getCurrentTime,
            this.writer,
            {
                ...this.baseContext,
                ...context
            }
        );
    }

    private write(
        level: LogLevel,
        message: string,
        error: unknown,
        context?: LogContext
    ): void {
        const mergedContext = {
            ...this.baseContext,
            ...context
        };

        const entry:
            StructuredLogEntry = {
            timestamp:
                this.getCurrentTime(),
            level,
            service:
                this.service,
            message,
            ...(
                Object.keys(
                    mergedContext
                ).length > 0
                    ? {
                        context:
                            mergedContext
                    }
                    : {}
            ),
            ...(
                error !== undefined
                    ? {
                        error:
                            serializeError(
                                error
                            )
                    }
                    : {}
            )
        };

        const serializedEntry =
            JSON.stringify(entry);

        switch (level) {
            case "info":
                this.writer.log(
                    serializedEntry
                );
                return;

            case "warn":
                this.writer.warn(
                    serializedEntry
                );
                return;

            case "error":
                this.writer.error(
                    serializedEntry
                );
                return;

            default:
                assertNever(level);
        }
    }
}

function serializeError(
    error: unknown
): StructuredError {
    if (error instanceof Error) {
        return {
            name: error.name,
            message: error.message,
            ...(
                error.stack
                    ? {
                        stack:
                            error.stack
                    }
                    : {}
            )
        };
    }

    return {
        name:
            "UnknownError",
        message:
            getUnknownErrorMessage(
                error
            )
    };
}

function getUnknownErrorMessage(
    error: unknown
): string {
    if (typeof error === "string") {
        return error;
    }

    try {
        const serialized =
            JSON.stringify(error);

        if (serialized) {
            return serialized;
        }
    } catch {
        // Fall back to String below.
    }

    return String(error);
}

function assertNever(
    value: never
): never {
    throw new Error(
        `Unsupported log level: ${value}`
    );
}