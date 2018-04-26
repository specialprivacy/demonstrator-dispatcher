FROM node:8 as builder
WORKDIR /app

COPY package.json /app/package.json
RUN npm install

FROM node:8-alpine

WORKDIR /app
EXPOSE 80
ARG NODE_ENV=development
CMD ["node", "server.js"]

COPY --from=builder /app/node_modules /app/node_modules

COPY utils /app/utils
COPY server.js /app/server.js
COPY lib /app/lib
COPY test /app/test

RUN if [ ${NODE_ENV} == "production" ]; then rm -rf /app/test; fi
