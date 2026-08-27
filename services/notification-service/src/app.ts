import express from "express";
import type {
    Express,
    Request,
    Response
} from "express";
import type {
    CustomerNotification
} from "./notificationService.js";

export interface NotificationReader {
    getNotifications():
        readonly CustomerNotification[];
}

export interface ReadinessProbe {
    isReady(): boolean;
}

export interface NotificationAppDependencies {
    readonly notificationReader:
    NotificationReader;

    readonly readinessProbe:
    ReadinessProbe;
}

export function createNotificationApp(
    dependencies:
        NotificationAppDependencies
): Express {
    const {
        notificationReader,
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
                service:
                    "notification-service"
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
                        "notification-service",
                    dependencies: {
                        rabbitMq:
                            status
                    }
                });
        }
    );

    app.get(
        "/notifications",
        (
            _request: Request,
            response: Response
        ) => {
            response.json({
                notifications:
                    notificationReader
                        .getNotifications()
            });
        }
    );

    return app;
}