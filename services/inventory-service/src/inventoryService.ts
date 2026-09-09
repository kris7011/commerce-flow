import { randomUUID } from "node:crypto";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type { InventoryRepository } from "./inventoryRepository.js";

export type InventoryResultEvent =
    | InventoryReservedEvent
    | InventoryReservationFailedEvent;

type UnavailableItem =
    InventoryReservationFailedEvent["data"]["unavailableItems"][number];

export interface InventoryServiceDependencies {
    generateId?: () => string;
    getCurrentTime?: () => string;
}

const INVENTORY_RESERVATION_FAILURE_REASON =
    "One or more products are not available in the requested quantity.";

export class InventoryService {
    private readonly generateId: () => string;
    private readonly getCurrentTime: () => string;

    constructor(
        private readonly repository: InventoryRepository,
        dependencies: InventoryServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId ?? (() => randomUUID());

        this.getCurrentTime =
            dependencies.getCurrentTime ?? (() => new Date().toISOString());
    }

    async processPaymentAuthorized(
        event: PaymentAuthorizedEvent
    ): Promise<InventoryResultEvent> {
        const unavailableItems =
            await this.findUnavailableItems(
                event.data.items
            );

        if (unavailableItems.length > 0) {
            return this.createReservationFailedEvent(
                event,
                unavailableItems
            );
        }

        await this.repository.reserve(
            event.data.items
        );

        return this.createInventoryReservedEvent(
            event
        );
    }

    private async findUnavailableItems(
        items: readonly OrderItem[]
    ): Promise<UnavailableItem[]> {
        const requestedQuantityByProductId =
            aggregateQuantities(items);

        const unavailableItems:
            UnavailableItem[] = [];

        for (
            const [
                productId,
                requestedQuantity
            ] of
            requestedQuantityByProductId
        ) {
            const availableQuantity =
                await this.repository
                    .getAvailableQuantity(
                        productId
                    );

            if (
                availableQuantity <
                requestedQuantity
            ) {
                unavailableItems.push({
                    productId,
                    requestedQuantity,
                    availableQuantity
                });
            }
        }

        return unavailableItems;
    }

    private createInventoryReservedEvent(
        sourceEvent: PaymentAuthorizedEvent
    ): InventoryReservedEvent {
        return {
            eventId: this.generateId(),
            eventType: "InventoryReserved",
            occurredAt: this.getCurrentTime(),
            correlationId: sourceEvent.correlationId,
            data: {
                orderId: sourceEvent.data.orderId,
                reservationId: this.generateId(),
                items: sourceEvent.data.items
            }
        };
    }

    private createReservationFailedEvent(
        sourceEvent: PaymentAuthorizedEvent,
        unavailableItems: UnavailableItem[]
    ): InventoryReservationFailedEvent {
        return {
            eventId: this.generateId(),
            eventType: "InventoryReservationFailed",
            occurredAt: this.getCurrentTime(),
            correlationId: sourceEvent.correlationId,
            data: {
                orderId: sourceEvent.data.orderId,
                reason: INVENTORY_RESERVATION_FAILURE_REASON,
                unavailableItems
            }
        };
    }
}

function aggregateQuantities(
    items: readonly OrderItem[]
): Map<string, number> {
    const quantityByProductId = new Map<string, number>();

    for (const item of items) {
        const currentQuantity =
            quantityByProductId.get(item.productId) ?? 0;

        quantityByProductId.set(
            item.productId,
            currentQuantity + item.quantity
        );
    }

    return quantityByProductId;
}