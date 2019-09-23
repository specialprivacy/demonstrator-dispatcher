FROM tenforce/caddyserver:latest

EXPOSE 80
EXPOSE 443

ENV DOMAIN=:80
ENV EMAIL=off

RUN apk --update add ca-certificates

COPY Caddyfile /config/Caddyfile
