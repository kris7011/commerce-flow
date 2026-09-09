import express from "express";
import type {
    Express,
    NextFunction,
    Request,
    Response
} from "express";

export interface InventoryStockReader {
    getAllStock(): Promise<
        Readonly<Record<string, number>>
    >;
}

export interface ReadinessProbe {
    isReady(): boolean;
}

export interface InventoryAppDependencies {
    readonly stockReader:
    InventoryStockReader;

    readonly readinessProbe:
    ReadinessProbe;
}

export function createInventoryApp(
    dependencies:
        InventoryAppDependencies
): Express {
    const {
        stockReader,
        readinessProbe
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
        "/ready",
        (
            _request: Request,
            response: Response
        ) => {
            const rabbitMqReady =
                readinessProbe.isReady();

            const status =
                rabbitMqReady
                    ? "Ready"
                    : "NotReady";

            return response
                .status(
                    rabbitMqReady
                        ? 200
                        : 503
                )
                .json({
                    status,
                    service:
                        "inventory-service",
                    dependencies: {
                        rabbitMq:
                            status
                    }
                });
        }
    );

    app.get(
        "/stock",
        async (
            _request: Request,
            response: Response,
            next: NextFunction
        ) => {
            try {
                const stock =
                    await stockReader
                        .getAllStock();

                response.json({
                    stock
                });
            } catch (error) {
                next(error);
            }
        }
    );

    return app;
}