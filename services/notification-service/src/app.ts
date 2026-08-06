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

export interface NotificationAppDependencies {
    readonly notificationReader:
    NotificationReader;
}

export function createNotificationApp(
    dependencies:
        NotificationAppDependencies
): Express {
    const {
        notificationReader
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