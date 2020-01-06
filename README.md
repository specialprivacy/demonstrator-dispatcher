# Special Platform Dispatcher

## Description
This service is the primary entry point for the special platform.
It contains the routing definitions for all the services used by the various frontends.

Configuration is managed by the Caddyfile, which can be mounted as a volume for local development

It also handles TLS termination when deployed onto a publicly accessible environment

## Configuration
* DOMAIN: The domain this server should listen on, by default ":80".
* EMAIL: The email used to get a certificate from TLS, by default "off", which prevents the application from trying to get one.