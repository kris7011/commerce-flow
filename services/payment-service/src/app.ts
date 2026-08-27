import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";

export interface ReadinessProbe {
    isReady(): boolean;
}

export interface PaymentAppDependencies {
    readonly readinessProbe:
    ReadinessProbe;
}

export function createPaymentApp(
    dependencies:
        PaymentAppDependencies
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
                service: "payment-service"
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
                        "payment-service",
                    dependencies: {
                        rabbitMq:
                            status
                    }
                });
        }
    );

    return app;
}