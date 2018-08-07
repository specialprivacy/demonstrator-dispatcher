FROM tenforce/caddyserver:latest

EXPOSE 80
EXPOSE 443

ENV DOMAIN=:80
ENV EMAIL=off

ENV TRANSPARENCY_UI=http://overview-ui
ENV CONSENT_UI=http://consent-management-frontend
ENV POLICY_CRUD_UI=http://data-controller-policy-management-frontend
ENV TRANSPARENCY_BACKEND=http://transparency-backend
ENV KEYCLOAK=http://keycloak:8080
ENV CONSENT_MANAGEMENT_BACKEND=http://consent-management-backend
ENV MOBILE_FRONTEND=http://mobile-frontend

RUN apk --update add ca-certificates

COPY landingpage /landingpage
COPY Caddyfile /config/Caddyfile
