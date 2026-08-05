import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";

export function createDeliveryApp(): Express {
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

    return app;
}