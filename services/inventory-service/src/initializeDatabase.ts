import {
    createStructuredLogger
} from "@commerce-flow/logging";
import {
    createInventoryDatabasePool,
    initializeInventoryDatabase
} from "./database.js";

const logger =
    createStructuredLogger(
        "inventory-database"
    );

const pool =
    createInventoryDatabasePool();

try {
    await initializeInventoryDatabase(
        pool
    );

    logger.info(
        "Inventory database initialized"
    );
} catch (error) {
    logger.error(
        "Failed to initialize inventory database",
        error
    );

    process.exitCode =
        1;
} finally {
    await pool.end();
}
