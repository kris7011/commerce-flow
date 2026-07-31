import type { OrderItem } from "@commerce-flow/contracts";

export interface InventoryRepository {
    getAvailableQuantity(productId: string): number;

    reserve(items: readonly OrderItem[]): void;

    getAllStock(): Readonly<Record<string, number>>;
}