import {
    createHash
} from "node:crypto";

/**
 * Derives a stable, UUID-formatted identifier from its inputs
 * (RFC 9562 version 5 layout, SHA-1 based).
 *
 * Stateless services use this to derive the ids of the events they
 * emit from the ids of the events they consume. Processing the same
 * message twice then yields an identical event, which downstream
 * consumers can recognise as a duplicate.
 */
export function deriveDeterministicId(
    namespace: string,
    ...parts: readonly string[]
): string {
    const hash =
        createHash(
            "sha1"
        )
            .update(
                JSON.stringify([
                    namespace,
                    ...parts
                ])
            )
            .digest();

    const bytes =
        hash.subarray(
            0,
            16
        );

    // Version 5 and RFC 4122 variant bits.
    bytes[6] =
        ((bytes[6] ?? 0) & 0x0f) |
        0x50;

    bytes[8] =
        ((bytes[8] ?? 0) & 0x3f) |
        0x80;

    const hex =
        bytes.toString(
            "hex"
        );

    return [
        hex.slice(0, 8),
        hex.slice(8, 12),
        hex.slice(12, 16),
        hex.slice(16, 20),
        hex.slice(20, 32)
    ].join(
        "-"
    );
}
