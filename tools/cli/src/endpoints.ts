/*
 * Where the developer tools find the running system. The defaults match
 * both `npm run dev:*` and `docker compose --profile app up`, which
 * publish the same ports on localhost.
 */

export const SERVICE_NAMES = [
    "order-service",
    "payment-service",
    "inventory-service",
    "delivery-service",
    "notification-service"
] as const;

export type ServiceName =
    (typeof SERVICE_NAMES)[number];

const DEFAULT_PORTS:
    Readonly<Record<ServiceName, number>> = {
        "order-service": 3001,
        "payment-service": 3002,
        "inventory-service": 3003,
        "delivery-service": 3004,
        "notification-service": 3005
    };

const URL_VARIABLES:
    Readonly<Record<ServiceName, string>> = {
        "order-service": "ORDER_SERVICE_URL",
        "payment-service": "PAYMENT_SERVICE_URL",
        "inventory-service": "INVENTORY_SERVICE_URL",
        "delivery-service": "DELIVERY_SERVICE_URL",
        "notification-service": "NOTIFICATION_SERVICE_URL"
    };

export interface Endpoints {
    readonly services:
    Readonly<Record<ServiceName, string>>;

    readonly rabbitMqUrl: string;
}

export function resolveEndpoints(
    environment: Readonly<Record<string, string | undefined>>
): Endpoints {
    const services =
        Object.fromEntries(
            SERVICE_NAMES.map(name => [
                name,
                readUrl(
                    environment,
                    URL_VARIABLES[name],
                    `http://127.0.0.1:${DEFAULT_PORTS[name]}`,
                    ["http:", "https:"]
                )
            ])
        ) as Record<ServiceName, string>;

    return {
        services,
        rabbitMqUrl:
            readUrl(
                environment,
                "RABBITMQ_URL",
                "amqp://guest:guest@127.0.0.1:5672",
                ["amqp:", "amqps:"]
            )
    };
}

function readUrl(
    environment: Readonly<Record<string, string | undefined>>,
    name: string,
    defaultValue: string,
    protocols: readonly string[]
): string {
    const value =
        environment[name]?.trim() ||
        defaultValue;

    let url: URL;

    try {
        url = new URL(value);
    } catch {
        // Never echo the value: it may contain credentials.
        throw new Error(
            `${name} is not a valid URL.`
        );
    }

    if (!protocols.includes(url.protocol)) {
        throw new Error(
            `${name} must use one of: ${protocols.join(", ")}.`
        );
    }

    return value.replace(/\/+$/, "");
}
