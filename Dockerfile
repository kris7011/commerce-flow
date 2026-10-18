# syntax=docker/dockerfile:1

# One image for all five services. Docker Compose selects the entry
# point per service (see docker-compose.yml, profile "app").
#
# The image runs the compiled JavaScript, not the TypeScript sources:
# the shared packages export their build output under the "production"
# condition, so `node --conditions=production` needs neither tsx nor
# any dev dependency at runtime.

ARG NODE_IMAGE=node:24-alpine

FROM ${NODE_IMAGE} AS build

WORKDIR /app

# Manifests first, so the dependency layer is cached until they change.
COPY package.json package-lock.json tsconfig.base.json ./
COPY shared/contracts/package.json shared/contracts/
COPY shared/messaging/package.json shared/messaging/
COPY shared/logging/package.json shared/logging/
COPY shared/testing/package.json shared/testing/
COPY shared/runtime/package.json shared/runtime/
COPY services/order-service/package.json services/order-service/
COPY services/payment-service/package.json services/payment-service/
COPY services/inventory-service/package.json services/inventory-service/
COPY services/delivery-service/package.json services/delivery-service/
COPY services/notification-service/package.json services/notification-service/
COPY tools/cli/package.json tools/cli/

RUN npm ci --ignore-scripts --no-audit --no-fund

COPY shared shared
COPY services services

RUN npm run build

FROM ${NODE_IMAGE} AS runtime

ENV NODE_ENV=production

WORKDIR /app

COPY --from=build /app/package.json /app/package-lock.json ./
COPY --from=build /app/shared/contracts/package.json shared/contracts/
COPY --from=build /app/shared/messaging/package.json shared/messaging/
COPY --from=build /app/shared/logging/package.json shared/logging/
COPY --from=build /app/shared/testing/package.json shared/testing/
COPY --from=build /app/shared/runtime/package.json shared/runtime/
COPY --from=build /app/services/order-service/package.json services/order-service/
COPY --from=build /app/services/payment-service/package.json services/payment-service/
COPY --from=build /app/services/inventory-service/package.json services/inventory-service/
COPY --from=build /app/services/delivery-service/package.json services/delivery-service/
COPY --from=build /app/services/notification-service/package.json services/notification-service/
COPY --from=build /app/tools/cli/package.json tools/cli/

RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund \
    && npm cache clean --force

COPY --from=build /app/shared/contracts/dist shared/contracts/dist
COPY --from=build /app/shared/messaging/dist shared/messaging/dist
COPY --from=build /app/shared/logging/dist shared/logging/dist
COPY --from=build /app/shared/runtime/dist shared/runtime/dist
COPY --from=build /app/services/order-service/dist services/order-service/dist
COPY --from=build /app/services/payment-service/dist services/payment-service/dist
COPY --from=build /app/services/inventory-service/dist services/inventory-service/dist
COPY --from=build /app/services/delivery-service/dist services/delivery-service/dist
COPY --from=build /app/services/notification-service/dist services/notification-service/dist

USER node

# Overridden per service in docker-compose.yml.
CMD ["node", "--conditions=production", "services/order-service/dist/index.js"]
