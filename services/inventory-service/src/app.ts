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
    isReady():
        boolean |
        Promise<boolean>;
}

export interface InventoryAppDependencies {
    readonly stockReader:
    InventoryStockReader;

    readonly rabbitMqReadinessProbe:
    ReadinessProbe;

    readonly databaseReadinessProbe:
    ReadinessProbe;
}

export function createInventoryApp(
    dependencies:
        InventoryAppDependencies
): Express {
    const {
        stockReader,
        rabbitMqReadinessProbe,
        databaseReadinessProbe
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
        async (
            _request: Request,
            response: Response
        ) => {
            const [
                rabbitMqReady,
                databaseReady
            ] =
                await Promise.all([
                    getProbeReadiness(
                        rabbitMqReadinessProbe
                    ),
                    getProbeReadiness(
                        databaseReadinessProbe
                    )
                ]);

            const ready =
                rabbitMqReady &&
                databaseReady;

            return response
                .status(
                    ready
                        ? 200
                        : 503
                )
                .json({
                    status:
                        ready
                            ? "Ready"
                            : "NotReady",
                    service:
                        "inventory-service",
                    dependencies: {
                        rabbitMq:
                            toReadinessStatus(
                                rabbitMqReady
                            ),
                        database:
                            toReadinessStatus(
                                databaseReady
                            )
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

async function getProbeReadiness(
    probe: ReadinessProbe
): Promise<boolean> {
    try {
        return await probe.isReady();
    } catch {
        return false;
    }
}

function toReadinessStatus(
    ready: boolean
): "Ready" | "NotReady" {
    return ready
        ? "Ready"
        : "NotReady";
}
