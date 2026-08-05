import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";

export interface InventoryStockReader {
    getAllStock():
        Readonly<Record<string, number>>;
}

export interface InventoryAppDependencies {
    readonly stockReader:
    InventoryStockReader;
}

export function createInventoryApp(
    dependencies:
        InventoryAppDependencies
): Express {
    const {
        stockReader
    } = dependencies;

    const app = express();

    app.get(
        "/health",
        (
            _request: Request,
            response: Response
        ) => {
            response.json({
                status: "Healthy",
                service: "inventory-service"
            });
        }
    );

    app.get(
        "/stock",
        (
            _request: Request,
            response: Response
        ) => {
            response.json({
                stock:
                    stockReader.getAllStock()
            });
        }
    );

    return app;
}