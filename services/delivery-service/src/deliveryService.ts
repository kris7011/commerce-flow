import {
    deriveDeterministicId,
    type DeliveryBookedEvent,
    type InventoryReservedEvent
} from "@commerce-flow/contracts";

export interface DeliveryServiceDependencies {
    /**
     * Overrides id generation (used by tests). By default the event id
     * is derived from the source event and the delivery id from the
     * order.
     */
    generateId?: () => string;
}

const DEFAULT_CARRIER = "DefaultCarrier";
const DELIVERY_TIME_IN_DAYS = 3;

export class DeliveryService {
    private readonly generateId: (() => string) | undefined;

    constructor(
        dependencies: DeliveryServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId;
    }

    /**
     * The resulting event is a pure function of the source event, so a
     * redelivered InventoryReserved message re-emits a byte-identical
     * DeliveryBooked event. In particular the estimated delivery date
     * is based on the reservation date, not on the processing date: a
     * redelivery after midnight must not change the promised date
     * under the same event id.
     */
    bookDelivery(
        sourceEvent: InventoryReservedEvent
    ): DeliveryBookedEvent {
        const bookingDate =
            new Date(
                sourceEvent.occurredAt
            );

        return {
            eventId: this.createId(
                "delivery-booked-event",
                sourceEvent.eventId
            ),
            eventType: "DeliveryBooked",
            occurredAt: sourceEvent.occurredAt,
            correlationId: sourceEvent.correlationId,
            data: {
                orderId: sourceEvent.data.orderId,
                deliveryId: this.createId(
                    "delivery",
                    sourceEvent.data.orderId
                ),
                carrier: DEFAULT_CARRIER,
                estimatedDeliveryDate:
                    calculateEstimatedDeliveryDate(
                        bookingDate,
                        DELIVERY_TIME_IN_DAYS
                    )
            }
        };
    }

    private createId(
        namespace: string,
        key: string
    ): string {
        return (
            this.generateId?.() ??
            deriveDeterministicId(
                `delivery-service.${namespace}`,
                key
            )
        );
    }
}

function calculateEstimatedDeliveryDate(
    bookingDate: Date,
    deliveryTimeInDays: number
): string {
    const estimatedDate =
        new Date(bookingDate.getTime());

    estimatedDate.setUTCDate(
        estimatedDate.getUTCDate() + deliveryTimeInDays
    );

    return estimatedDate
        .toISOString()
        .slice(0, 10);
}
