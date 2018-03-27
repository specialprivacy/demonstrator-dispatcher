FROM tenforce/caddyserver:latest

EXPOSE 80
EXPOSE 443

ENV FRONTEND=http://overview-ui
ENV SSE_PROXY=http://sse-proxy

COPY Caddyfile /config/Caddyfile

