import { randomUUID } from "node:crypto";
import type {
    DeliveryBookedEvent,
    InventoryReservedEvent
} from "@commerce-flow/contracts";

export interface DeliveryServiceDependencies {
    generateId?: () => string;
    getCurrentDate?: () => Date;
}

const DEFAULT_CARRIER = "DefaultCarrier";
const DELIVERY_TIME_IN_DAYS = 3;

export class DeliveryService {
    private readonly generateId: () => string;
    private readonly getCurrentDate: () => Date;

    constructor(
        dependencies: DeliveryServiceDependencies = {}
    ) {
        this.generateId =
            dependencies.generateId ?? (() => randomUUID());

        this.getCurrentDate =
            dependencies.getCurrentDate ?? (() => new Date());
    }

    bookDelivery(
        sourceEvent: InventoryReservedEvent
    ): DeliveryBookedEvent {
        const currentDate = this.getCurrentDate();

        return {
            eventId: this.generateId(),
            eventType: "DeliveryBooked",
            occurredAt: currentDate.toISOString(),
            correlationId: sourceEvent.correlationId,
            data: {
                orderId: sourceEvent.data.orderId,
                deliveryId: this.generateId(),
                carrier: DEFAULT_CARRIER,
                estimatedDeliveryDate:
                    calculateEstimatedDeliveryDate(
                        currentDate,
                        DELIVERY_TIME_IN_DAYS
                    )
            }
        };
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
