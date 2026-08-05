import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";

export function createPaymentApp(): Express {
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

    return app;
}
