# Special Platform Dispatcher
This service is the primary entry point for the special platform.
It contains the routing definitions for all the services used by the various frontends.

Configuration is managed by the Caddyfile, which can be mounted as a volume for local development

It also handles TLS termination when deployed onto a publicly accessible environment
