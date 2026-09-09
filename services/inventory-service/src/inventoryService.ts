import { randomUUID } from "node:crypto";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    InventoryRepository
} from "./inventoryRepository.js";

export type InventoryResultEvent =
    | InventoryReservedEvent
    | InventoryReservationFailedEvent;

type UnavailableItem =
    InventoryReservationFailedEvent[
    "data"
    ][
    "unavailableItems"
    ][number];

export interface InventoryServiceDependencies {
    generateId?: () => string;
    getCurrentTime?: () => string;
}

const INVENTORY_RESERVATION_FAILURE_REASON =
    "One or more products are not available in the requested quantity.";

export class InventoryService {
    private readonly generateId:
        () => string;

    private readonly getCurrentTime:
        () => string;

    constructor(
        private readonly repository:
            InventoryRepository,
        dependencies:
            InventoryServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId ??
            (() => randomUUID());

        this.getCurrentTime =
            dependencies.getCurrentTime ??
            (() => new Date().toISOString());
    }

    async processPaymentAuthorized(
        event: PaymentAuthorizedEvent
    ): Promise<InventoryResultEvent> {
        const reservationResult =
            await this.repository
                .tryReserve(
                    event.data.items
                );

        if (
            reservationResult.status ===
            "InsufficientStock"
        ) {
            return this
                .createReservationFailedEvent(
                    event,
                    [
                        ...reservationResult
                            .unavailableItems
                    ]
                );
        }

        return this
            .createInventoryReservedEvent(
                event
            );
    }

    private createInventoryReservedEvent(
        sourceEvent:
            PaymentAuthorizedEvent
    ): InventoryReservedEvent {
        return {
            eventId:
                this.generateId(),
            eventType:
                "InventoryReserved",
            occurredAt:
                this.getCurrentTime(),
            correlationId:
                sourceEvent
                    .correlationId,
            data: {
                orderId:
                    sourceEvent
                        .data
                        .orderId,
                reservationId:
                    this.generateId(),
                items:
                    sourceEvent
                        .data
                        .items
            }
        };
    }

    private createReservationFailedEvent(
        sourceEvent:
            PaymentAuthorizedEvent,
        unavailableItems:
            UnavailableItem[]
    ): InventoryReservationFailedEvent {
        return {
            eventId:
                this.generateId(),
            eventType:
                "InventoryReservationFailed",
            occurredAt:
                this.getCurrentTime(),
            correlationId:
                sourceEvent
                    .correlationId,
            data: {
                orderId:
                    sourceEvent
                        .data
                        .orderId,
                reason:
                    INVENTORY_RESERVATION_FAILURE_REASON,
                unavailableItems
            }
        };
    }
}
