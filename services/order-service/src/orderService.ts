import {
    createHash,
    randomUUID
} from "node:crypto";
import type {
    OrderCreatedEvent,
    OrderItem
} from "@commerce-flow/contracts";

export interface CreateOrderRequest {
    customerId: string;
    items: OrderItem[];
}

export interface CreatedOrderResponse {
    orderId: string;
    status: "Created";
    totalAmount: number;
    correlationId: string;
}

export interface CreateOrderResult {
    event: OrderCreatedEvent;
    response: CreatedOrderResponse;

    /**
     * Identifies the request content, so a reused Idempotency-Key can
     * be told apart from a genuine retry of the same request.
     */
    requestFingerprint: string;
}

const CORRELATION_ID_PATTERN =
    /^[A-Za-z0-9._:-]{1,128}$/;

export interface OrderServiceDependencies {
    generateId?: () => string;
    getCurrentTime?: () => string;
}

export class OrderService {
    private readonly generateId: () => string;
    private readonly getCurrentTime: () => string;

    constructor(
        dependencies: OrderServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId ?? (() => randomUUID());

        this.getCurrentTime =
            dependencies.getCurrentTime ??
            (() => new Date().toISOString());
    }

    createOrder(
        request: CreateOrderRequest,
        suppliedCorrelationId?: string
    ): CreateOrderResult {
        const correlationId =
            this.resolveCorrelationId(suppliedCorrelationId);

        const orderId = this.generateId();
        const eventId = this.generateId();
        const totalAmount =
            calculateTotalAmount(request.items);

        const event: OrderCreatedEvent = {
            eventId,
            eventType: "OrderCreated",
            occurredAt: this.getCurrentTime(),
            correlationId,
            data: {
                orderId,
                customerId: request.customerId,
                items: request.items,
                totalAmount
            }
        };

        return {
            event,
            response: {
                orderId,
                status: "Created",
                totalAmount,
                correlationId
            },
            requestFingerprint:
                fingerprintOrderRequest(
                    request
                )
        };
    }

    /**
     * Accepts a caller-supplied correlation id only if it is short and
     * uses a safe character set; anything else (including attempts to
     * inject newlines or markup into logs) is replaced by a new id.
     */
    private resolveCorrelationId(
        suppliedCorrelationId?: string
    ): string {
        const normalizedCorrelationId =
            suppliedCorrelationId?.trim();

        if (
            normalizedCorrelationId &&
            CORRELATION_ID_PATTERN.test(
                normalizedCorrelationId
            )
        ) {
            return normalizedCorrelationId;
        }

        return this.generateId();
    }
}

function calculateTotalAmount(
    items: readonly OrderItem[]
): number {
    const total = items.reduce((sum, item) => {
        return sum + item.quantity * item.unitPrice;
    }, 0);

    return Number(total.toFixed(2));
}

/**
 * SHA-256 of the request in a canonical form: fixed key order, item
 * order preserved (it is part of the order), unknown fields excluded.
 * Two requests with the same fingerprint describe the same order.
 */
export function fingerprintOrderRequest(
    request: CreateOrderRequest
): string {
    const canonical =
        JSON.stringify({
            customerId:
                request.customerId,
            items:
                request.items.map(item => ({
                    productId:
                        item.productId,
                    quantity:
                        item.quantity,
                    unitPrice:
                        item.unitPrice
                }))
        });

    return createHash(
        "sha256"
    )
        .update(
            canonical
        )
        .digest(
            "hex"
        );
}
