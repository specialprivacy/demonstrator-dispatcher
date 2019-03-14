FROM tenforce/caddyserver:latest

EXPOSE 80
EXPOSE 443

ENV DOMAIN=:80
ENV EMAIL=off

ENV PROXY_TO: http://kong:8000

RUN apk --update add ca-certificates

COPY Caddyfile /config/Caddyfile
