import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";

export interface ReadinessProbe {
    isReady(): boolean;
}

export interface DeliveryAppDependencies {
    readonly readinessProbe:
    ReadinessProbe;
}

export function createDeliveryApp(
    dependencies:
        DeliveryAppDependencies
): Express {
    const {
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
                service: "delivery-service"
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
                        "delivery-service",
                    dependencies: {
                        rabbitMq:
                            status
                    }
                });
        }
    );

    return app;
}