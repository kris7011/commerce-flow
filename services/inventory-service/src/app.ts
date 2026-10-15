import express from "express";
import type {
    AppLogger
} from "@commerce-flow/logging";
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

export interface OutboxDiagnosticsReader {
    getOutboxDiagnostics():
        Promise<Readonly<Record<string, unknown>>>;
}

export interface InventoryAppDependencies {
    readonly stockReader:
    InventoryStockReader;

    readonly rabbitMqReadinessProbe:
    ReadinessProbe;

    readonly databaseReadinessProbe:
    ReadinessProbe;

    readonly outboxDiagnosticsReader?:
    OutboxDiagnosticsReader;

    readonly logger?:
    AppLogger;
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

    const {
        outboxDiagnosticsReader
    } = dependencies;

    if (outboxDiagnosticsReader) {
        // Operational view of the outbox backlog. Contains counts and
        // timestamps only, never event payloads.
        app.get(
            "/diagnostics/outbox",
            async (
                _request: Request,
                response: Response,
                next: NextFunction
            ) => {
                try {
                    response.json(
                        await outboxDiagnosticsReader
                            .getOutboxDiagnostics()
                    );
                } catch (error) {
                    next(error);
                }
            }
        );
    }

    app.use(
        (
            error: unknown,
            request: Request,
            response: Response,
            // Express identifies error middleware by its arity.
            _next: NextFunction
        ) => {
            dependencies.logger?.error(
                "Request failed",
                error,
                {
                    method:
                        request.method,
                    path:
                        request.path
                }
            );

            // Details stay in the server logs; clients get a stable,
            // non-sensitive body.
            response
                .status(503)
                .json({
                    error:
                        "The inventory data is temporarily unavailable."
                });
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
