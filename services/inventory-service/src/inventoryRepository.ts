import type {
    OrderItem
} from "@commerce-flow/contracts";

export interface UnavailableInventoryItem {
    readonly productId: string;
    readonly requestedQuantity: number;
    readonly availableQuantity: number;
}

export type InventoryReservationResult =
    | {
        readonly status: "Reserved";
    }
    | {
        readonly status: "InsufficientStock";
        readonly unavailableItems:
        readonly UnavailableInventoryItem[];
    };

export interface InventoryRepository {
    tryReserve(
        items: readonly OrderItem[]
    ): Promise<InventoryReservationResult>;

    getAllStock(): Promise<
        Readonly<Record<string, number>>
    >;
}
