import { randomUUID } from "node:crypto";
import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderItem,
    PaymentAuthorizedEvent
} from "@commerce-flow/contracts";
import type {
    InventoryResultEvent,
    InventoryUnitOfWork,
    ReservationRecord
} from "./inventoryStore.js";

export type {
    InventoryResultEvent
} from "./inventoryStore.js";

type UnavailableItem =
    InventoryReservationFailedEvent[
    "data"
    ][
    "unavailableItems"
    ][number];

export type ReservationOutcome =
    | {
        /** The reservation decision was made and committed now. */
        readonly status: "Processed";
        readonly event: InventoryResultEvent;
    }
    | {
        /**
         * The order had already been processed (by this or another
         * message). Nothing was changed; the original outcome stands.
         */
        readonly status: "Duplicate";
        readonly existing: ReservationRecord;
    };

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
        private readonly unitOfWork:
            InventoryUnitOfWork,
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

    /**
     * Reserves stock for a paid order exactly once per order id.
     *
     * The stock change (if any), the idempotency record and the
     * resulting event are committed in one transaction. The event is
     * published later by the outbox relay, so a broker outage can
     * never leave a committed reservation without its event.
     */
    async reserveForPayment(
        sourceEvent: PaymentAuthorizedEvent
    ): Promise<ReservationOutcome> {
        const {
            orderId,
            items
        } = sourceEvent.data;

        return await this.unitOfWork.inTransaction(
            async transaction => {
                const existing =
                    await transaction.lockOrder(
                        orderId
                    );

                if (existing) {
                    return {
                        status:
                            "Duplicate",
                        existing
                    };
                }

                const requestedQuantities =
                    aggregateQuantities(
                        items
                    );

                const availableQuantities =
                    await transaction.lockStock([
                        ...requestedQuantities.keys()
                    ]);

                const unavailableItems =
                    findUnavailableItems(
                        requestedQuantities,
                        availableQuantities
                    );

                if (
                    unavailableItems.length >
                    0
                ) {
                    const event =
                        this.createReservationFailedEvent(
                            sourceEvent,
                            unavailableItems
                        );

                    await transaction.recordReservation({
                        orderId,
                        status:
                            "Rejected",
                        reservationId:
                            null,
                        sourceEventId:
                            sourceEvent.eventId,
                        resultEventId:
                            event.eventId,
                        items
                    });

                    await transaction.enqueueEvent(
                        event
                    );

                    return {
                        status:
                            "Processed",
                        event
                    };
                }

                await transaction.decrementStock(
                    requestedQuantities
                );

                const event =
                    this.createInventoryReservedEvent(
                        sourceEvent
                    );

                await transaction.recordReservation({
                    orderId,
                    status:
                        "Reserved",
                    reservationId:
                        event.data.reservationId,
                    sourceEventId:
                        sourceEvent.eventId,
                    resultEventId:
                        event.eventId,
                    items
                });

                await transaction.enqueueEvent(
                    event
                );

                return {
                    status:
                        "Processed",
                    event
                };
            }
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

export function aggregateQuantities(
    items: readonly OrderItem[]
): Map<string, number> {
    const quantityByProductId =
        new Map<string, number>();

    for (const item of items) {
        quantityByProductId.set(
            item.productId,
            (
                quantityByProductId
                    .get(item.productId) ??
                0
            ) +
            item.quantity
        );
    }

    return quantityByProductId;
}

function findUnavailableItems(
    requestedQuantities:
        ReadonlyMap<string, number>,
    availableQuantities:
        ReadonlyMap<string, number>
): UnavailableItem[] {
    const unavailableItems:
        UnavailableItem[] = [];

    for (
        const [
            productId,
            requestedQuantity
        ] of requestedQuantities
    ) {
        const availableQuantity =
            availableQuantities
                .get(productId) ??
            0;

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
