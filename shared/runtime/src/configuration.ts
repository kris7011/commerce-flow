/*
 * Small, explicit environment readers. Invalid values fail at startup
 * with a message naming the variable, instead of surfacing later as
 * NaN ports or silently ignored settings. Values are never echoed for
 * variables that may contain credentials.
 */

export type Environment =
    Readonly<Record<string, string | undefined>>;

export class ConfigurationError extends Error {
    constructor(
        message: string
    ) {
        super(
            message
        );

        this.name =
            "ConfigurationError";
    }
}

export function readString(
    environment: Environment,
    name: string,
    defaultValue: string
): string {
    const value =
        environment[name]?.trim();

    if (
        value === undefined ||
        value === ""
    ) {
        return defaultValue;
    }

    return value;
}

export function readInteger(
    environment: Environment,
    name: string,
    defaultValue: number,
    bounds: {
        readonly min: number;
        readonly max: number;
    }
): number {
    const rawValue =
        environment[name]?.trim();

    if (
        rawValue === undefined ||
        rawValue === ""
    ) {
        return defaultValue;
    }

    if (!/^-?\d+$/.test(rawValue)) {
        throw new ConfigurationError(
            `${name} must be an integer, received '${rawValue}'.`
        );
    }

    const value =
        Number(
            rawValue
        );

    if (
        value < bounds.min ||
        value > bounds.max
    ) {
        throw new ConfigurationError(
            `${name} must be between ${bounds.min} and ` +
            `${bounds.max}, received ${value}.`
        );
    }

    return value;
}

export function readPort(
    environment: Environment,
    name: string,
    defaultValue: number
): number {
    return readInteger(
        environment,
        name,
        defaultValue,
        {
            min: 0,
            max: 65_535
        }
    );
}

/**
 * Reads a connection URL and validates its protocol. Error messages
 * never include the value, which typically contains a password.
 */
export function readUrl(
    environment: Environment,
    name: string,
    defaultValue: string,
    allowedProtocols: readonly string[]
): string {
    const value =
        readString(
            environment,
            name,
            defaultValue
        );

    let protocol: string;

    try {
        protocol =
            new URL(
                value
            ).protocol;
    } catch {
        throw new ConfigurationError(
            `${name} is not a valid URL.`
        );
    }

    if (
        !allowedProtocols.includes(
            protocol
        )
    ) {
        throw new ConfigurationError(
            `${name} must use one of: ` +
            `${allowedProtocols.join(", ")}.`
        );
    }

    return value;
}
