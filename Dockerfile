FROM tenforce/caddyserver:latest

EXPOSE 80
EXPOSE 443

ENV FRONTEND=http://overview-ui
ENV SSE_PROXY=http://sse-proxy
ENV KEYCLOAK=http://keycloak:8080
ENV CONSENT_MANAGEMENT_BACKEND=http://consent-management-backend:8081

COPY Caddyfile /config/Caddyfile

