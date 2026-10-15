import {
    deriveDeterministicId,
    type DeliveryBookedEvent,
    type InventoryReservedEvent
} from "@commerce-flow/contracts";

export interface DeliveryServiceDependencies {
    /**
     * Overrides id generation (used by tests). By default the event id
     * is derived from the source event and the delivery id from the
     * order, so a redelivered InventoryReserved message re-emits an
     * identical, recognisable DeliveryBooked event.
     */
    generateId?: () => string;
    getCurrentDate?: () => Date;
}

const DEFAULT_CARRIER = "DefaultCarrier";
const DELIVERY_TIME_IN_DAYS = 3;

export class DeliveryService {
    private readonly generateId: (() => string) | undefined;
    private readonly getCurrentDate: () => Date;

    constructor(
        dependencies: DeliveryServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId;

        this.getCurrentDate =
            dependencies.getCurrentDate ?? (() => new Date());
    }

    bookDelivery(
        sourceEvent: InventoryReservedEvent
    ): DeliveryBookedEvent {
        const currentDate = this.getCurrentDate();

        return {
            eventId: this.createId(
                "delivery-booked-event",
                sourceEvent.eventId
            ),
            eventType: "DeliveryBooked",
            occurredAt: currentDate.toISOString(),
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
                        currentDate,
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
