# Consent Management Backend
This is the backend component for the consent management service in the SPECIAL architecture.
It is responsible for CRUD operations of both data subject and data controller policies.

It takes care of validating the inputs before writing them into rethinkdb.

It also emits 2 streams of data for downstream consumption:
* Policy Change Feed for audit purposes
* Full Policy Feed for replication purposes

The CI configuration takes care of testing the code and automatically building properly taggen docker images for deployment.

## Options
All options are specified as environment variables for the nodejs process
* **RETHINKDB_HOST**: The hostname of the rethinkdb server (_default_: `localhost`)
* **RETHINKDB_PORT**: The portnumber on which the rethinkdb server is listening (_default_: `28015`)
* **KAFKA_BROKER_LIST**: A list of kafka endpoints to try when connecting to the kafka cluster (_default_: `localhost:9092`)
* **CHANGE_LOGS_TOPIC**: The name of the topic where the audit log gets written (_default_: `policies-audit`)
* **FULL_POLICIES_TOPIC**: The name of the topic where the the replication log gets written (_default_: `full-policies`)

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
1. Set default ENV variables in Dockerfile
1. Rename git repo
1. Use something else than Memory Store for sessions
1. Use "secure" in express-session (need HTTPS though)
1. List all possible ENV
1. Add logout (frontend should redirect to logout url, authentication service should then redirect to here on /logout, where the session would be cleared)
1. Access token should be checked on every call to ensure it's still valid. If it's not, try to use refresh token, if outdated too, clear session then login again.
