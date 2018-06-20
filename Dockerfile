FROM node:8-alpine as builder
WORKDIR /app


RUN apk add --update build-base python bash
COPY package.json /app/package.json
RUN npm install

FROM node:8-alpine

WORKDIR /app

ENV SERVER_HOST=localhost \
  SERVER_PORT=80 \
  SERVER_AUTH_CALLBACK_ENDPOINT=http://localhost/callback \
  SESSION_SECRET=consent-management-backend \
  HTTP_MAX_SOCKETS=10 \
  RETHINKDB_HOST=localhost \
  RETHINKDB_PORT=28015 \
  RETHINKDB_TIMEOUT=60 \
  KAFKA_BROKER_LIST=localhost:9092 \
  KAFKA_TIMEOUT=60000 \
  KAFKA_VERSION_REQUEST=false \
  KAFKA_CHANGE_LOGS_TOPIC=policies-audit \
  KAFKA_FULL_POLICIES_TOPIC=full-policies \
  AUTH_CLIENT_ID=special-platform \
  AUTH_CLIENT_SECRET=special-platform-secret \
  AUTH_LOGIN_ENDPOINT=http://localhost:8080/auth/realms/master/protocol/openid-connect/auth \
  AUTH_TOKEN_ENDPOINT=http://localhost:8080/auth/realms/master/protocol/openid-connect/token \
  AUTH_USERINFO_ENDPOINT=http://localhost:8080/auth/realms/master/protocol/openid-connect/userinfo


EXPOSE 80
ARG NODE_ENV=development
CMD ["node", "server.js"]

COPY --from=builder /app/package.json /app/package.json
COPY --from=builder /app/node_modules /app/node_modules

COPY server.js /app/server.js
COPY app.js /app/app.js
COPY utils /app/utils
COPY lib /app/lib
COPY test /app/test

RUN if [ ${NODE_ENV} == "production" ]; then rm -rf /app/test; fi
