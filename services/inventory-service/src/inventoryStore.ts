import type {
    InventoryReservationFailedEvent,
    InventoryReservedEvent,
    OrderItem
} from "@commerce-flow/contracts";

/*
 * Persistence ports used by the inventory application layer.
 *
 * The application service owns the business rules; an adapter only
 * supplies an atomic transaction boundary. Everything done through a
 * single InventoryTransaction commits together or not at all, which is
 * what makes the stock change, the idempotency record and the outbox
 * event consistent with each other.
 */

export type InventoryResultEvent =
    | InventoryReservedEvent
    | InventoryReservationFailedEvent;

export type ReservationStatus =
    | "Reserved"
    | "Rejected";

export interface ReservationRecord {
    readonly orderId: string;
    readonly status: ReservationStatus;
    readonly reservationId: string | null;
    readonly sourceEventId: string;
    readonly resultEventId: string;
    readonly items: readonly OrderItem[];
}

export interface InventoryTransaction {
    /**
     * Serialises all transactions for the same order and returns the
     * already recorded outcome, if any. Must be called before any
     * other operation for that order.
     */
    lockOrder(
        orderId: string
    ): Promise<ReservationRecord | null>;

    /**
     * Locks the stock rows for the given products until the
     * transaction ends and returns their quantities. Unknown products
     * are omitted.
     */
    lockStock(
        productIds: readonly string[]
    ): Promise<ReadonlyMap<string, number>>;

    decrementStock(
        quantityByProductId:
            ReadonlyMap<string, number>
    ): Promise<void>;

    recordReservation(
        record: ReservationRecord
    ): Promise<void>;

    /** Stores the event in the transactional outbox. */
    enqueueEvent(
        event: InventoryResultEvent
    ): Promise<void>;
}

export interface InventoryUnitOfWork {
    inTransaction<T>(
        work: (
            transaction: InventoryTransaction
        ) => Promise<T>
    ): Promise<T>;
}

export interface InventoryStockReader {
    getAllStock(): Promise<
        Readonly<Record<string, number>>
    >;
}
