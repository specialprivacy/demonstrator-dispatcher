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
* Flesh out README
* Make port number configurable
* Clean up the code (too much stuff in server.js)
* Add structured logging
* Add support for kafka SSL
* Add support for kafka access management
* Restructure API
