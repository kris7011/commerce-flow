/**
 * Signals that a message can never be processed successfully,
 * for example because it is malformed or violates the event
 * contract. Such messages are dead-lettered immediately instead
 * of being retried.
 */
export class PermanentMessageError extends Error {
    constructor(
        message: string,
        options?: ErrorOptions
    ) {
        super(
            message,
            options
        );

        this.name =
            "PermanentMessageError";
    }
}

/**
 * Raised when the broker does not confirm a published message
 * within the configured timeout. The message may or may not have
 * reached the broker, so callers must treat the publication as
 * failed and retry (at-least-once delivery).
 */
export class PublishTimeoutError extends Error {
    constructor(
        message: string
    ) {
        super(
            message
        );

        this.name =
            "PublishTimeoutError";
    }
}

export function getErrorMessage(
    error: unknown
): string {
    if (
        error instanceof Error &&
        error.message.length > 0
    ) {
        return error.message;
    }

    if (
        typeof error ===
        "object" &&
        error !== null
    ) {
        try {
            return JSON.stringify(
                error
            );
        } catch {
            // Fall back to String below.
        }
    }

    const message =
        String(
            error
        );

    if (
        message.length > 0
    ) {
        return message;
    }

    return "Unknown error";
}
