/*
 * The Idempotency-Key request header (IETF draft
 * "The Idempotency-Key HTTP Header Field"). A client generates one
 * unique key per order it intends to place, typically a UUID, and sends
 * the same key again when it retries that order. The Order Service
 * stores the key with the order, so a retry returns the original order
 * instead of creating a second one.
 */

export const IDEMPOTENCY_KEY_HEADER =
    "idempotency-key";

export const IDEMPOTENCY_KEY_MAX_LENGTH =
    255;

/** Visible ASCII only: no spaces, control characters or non-ASCII. */
const IDEMPOTENCY_KEY_PATTERN =
    /^[\x21-\x7E]+$/;

export type IdempotencyKeyResult =
    | {
        readonly success: true;
        readonly key: string;
    }
    | {
        readonly success: false;
        readonly error: string;
    };

export function parseIdempotencyKey(
    headerValue: string | undefined
): IdempotencyKeyResult {
    if (
        headerValue === undefined ||
        headerValue.length === 0
    ) {
        return {
            success: false,
            error:
                "The Idempotency-Key header is required. Send a new unique " +
                "value (for example a UUID) per order, and the same value " +
                "when you retry that order."
        };
    }

    if (
        headerValue.length > IDEMPOTENCY_KEY_MAX_LENGTH ||
        !IDEMPOTENCY_KEY_PATTERN.test(
            headerValue
        )
    ) {
        return {
            success: false,
            error:
                `The Idempotency-Key header must be 1 to ` +
                `${IDEMPOTENCY_KEY_MAX_LENGTH} visible ASCII characters.`
        };
    }

    return {
        success: true,
        key:
            headerValue
    };
}
