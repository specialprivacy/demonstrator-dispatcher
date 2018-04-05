FROM tenforce/caddyserver:latest

EXPOSE 80
EXPOSE 443

ENV FRONTEND=http://overview-ui
ENV SSE_PROXY=http://sse-proxy
ENV KEYCLOAK_PROXY=http://keycloak:8080

COPY Caddyfile /config/Caddyfile

