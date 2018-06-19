# Consent Management Backend
This is the backend component for the consent management service in the SPECIAL architecture.
It is responsible for CRUD operations of both data subject and data controller policies.

It takes care of validating the inputs before writing them into rethinkdb.

It also emits 2 streams of data for downstream consumption:
* Policy Change Feed for audit purposes
* Full Policy Feed for replication purposes

The CI configuration takes care of testing the code and automatically building properly tagged docker images for deployment.

## Options
All options are specified as environment variables for the nodejs process
* **LOGGING_LEVEL**: The verbosity of the logs. oneOf: [`trace`, `debug`, `info`, `warn`, `error`, `fatal`] (_default_: `info`)
* **SERVER_HOST**: The hostname of the server (_default_: `localhost`)
* **SERVER_PORT**: The port of the server (_default_: `80`)
* **SERVER_AUTH_CALLBACK_ENDPOINT**: The callback URL where the authentication server should redirect the user after authentication (_default_: `http://localhost/callback`)
* **SESSION_SECRET**: The secret used to sign the session id cookie (_default_: `consent-management-backend`)
* **HTTP_MAX_SOCKETS**: The maximum number of HTTP sockets that can be opened (_default_: `10`)
* **RETHINKDB_HOST**: The hostname of the rethinkdb server (_default_: `localhost`)
* **RETHINKDB_PORT**: The port on which the rethinkdb server is listening (_default_: `28015`)
* **RETHINKDB_TIMEOUT**: The timeout in seconds when connecting to RethinkDB (_default_: `60`)
* **KAFKA_BROKER_LIST**: A list of kafka endpoints to try when connecting to the kafka cluster (_default_: `localhost:9092`)
* **KAFKA_TIMEOUT**: The timeout in milliseconds when connecting to Kafka (_default_: `localhost:60000`)
* **KAFKA_VERSION_REQUEST**: Whether the Kafka client should try to obtain the Kafka version, was to set to false to avoid a known bug (_default_: `false`)
* **KAFKA_CHANGE_LOGS_TOPIC**: The name of the topic where the audit log gets written (_default_: `policies-audit`)
* **KAFKA_FULL_POLICIES_TOPIC**: The name of the topic where the the replication log gets written (_default_: `full-policies`)
* **AUTH_CLIENT_ID**: The ID of your OAUTH client (_default_: `special-platform`)
* **AUTH_CLIENT_SECRET**: The secret of your OAUTH client (_default_: `special-platform-secret`)
* **AUTH_LOGIN_ENDPOINT**: The authentication URL of your OAUTH server (_default_: `http://localhost:8080/auth/realms/master/protocol/openid-connect/auth`)
* **AUTH_TOKEN_ENDPOINT**: The token URL of your OAUTH server (_default_: `http://localhost:8080/auth/realms/master/protocol/openid-connect/token`)
* **AUTH_USERINFO_ENDPOINT**: The token URL of your OAUTH server (_default_: `http://localhost:8080/auth/realms/master/protocol/openid-connect/userinfo`)

## Build
TODO: Add build instructions for local and docker based development

## TODO
1. Flesh out README
1. Clean up the code (too much stuff in server.js)
    * Move triggers to module
    * Move oauth middleware to lib
1. Add support for kafka SSL
1. Add support for kafka access management
1. Restructure API
1. Reuse database connections (typically expensive to create, current behaviour will cause problems under load)
    * Base lib doesn't have connection pool, check other possibilities
1. Rename git repo
1. Use something else than Memory Store for sessions
1. Use "secure" in express-session (need HTTPS though)
1. List all possible ENV
1. Add logout (frontend should redirect to logout url, authentication service should then redirect to here on /logout, where the session would be cleared)
1. Access token should be checked on every call to ensure it's still valid. If it's not, try to use refresh token, if outdated too, clear session then login again.
