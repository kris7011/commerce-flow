import type {
    OrderItem
} from "@commerce-flow/contracts";

export interface InventoryRepository {
    getAvailableQuantity(
        productId: string
    ): Promise<number>;

    reserve(
        items: readonly OrderItem[]
    ): Promise<void>;

    getAllStock(): Promise<
        Readonly<Record<string, number>>
    >;
}
