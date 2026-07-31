import { randomUUID } from "node:crypto";
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
}

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
            }
        };
    }

    private resolveCorrelationId(
        suppliedCorrelationId?: string
    ): string {
        const normalizedCorrelationId =
            suppliedCorrelationId?.trim();

        if (normalizedCorrelationId) {
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
