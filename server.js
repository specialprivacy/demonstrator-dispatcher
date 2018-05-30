// TODO: Add tests and documentation

const app = require("express")()
const session = require("express-session")
const crypto = require("crypto")
const bodyParser = require("body-parser")
const Kafka = require("node-rdkafka")
const querystring = require("querystring")

const http = require("http")
http.globalAgent.maxSockets = process.env["HTTP_MAX_SOCKETS"] || 10

const applications = require("./lib/applications")
const dataSubjects = require("./lib/data-subjects")
const policies = require("./lib/policies")
const childlogger = require("./lib/middleware/child-logger")

const producer = new Kafka.Producer({
  "metadata.broker.list": process.env["KAFKA_BROKER_LIST"] || "localhost:9092, localhost:9094",
  "api.version.request": process.env["KAFKA_VERSION_REQUEST"] || false,
  "dr_cb": true
})

const changeLogsTopic = process.env["CHANGE_LOGS_TOPIC"] || "policies-audit"
const fullPolicyTopic = process.env["FULL_POLICIES_TOPIC"] || "full-policies"

const rethink = require("./utils/rethinkdb_config")
const {
  dbHost,
  dbPort,
  dbTimeout,
  r,
  applicationsTable,
  dataControllerPoliciesTable,
  dataSubjectsTable
} = rethink

const dataGenerator = require("./utils/data_generator")

app.disable("x-powered-by")

var request = require("request-promise")
// require("request-debug")(request) // Uncomment to log HTTP requests

let redirectUri = null
const baseAuthURL = process.env["AUTH_LOGIN_URL"] || "http://localhost:8080/auth/realms/master/protocol/openid-connect/auth"
let server = null
async function init () {
  await dataGenerator.generate()
  producer.connect({"timeout": process.env["KAFKA_TIMEOUT"] || 30000})
  producer.on("connection.failure", function (error) {
    console.error("Could not connect to Kafka, exiting: %s", error)
    console.error(error)
    process.exit(-1)
  })
  producer.on("event.error", function (error) {
    console.error("Error from kafka producer: %s", error)
    console.error(error)
  })
  producer.on("delivery-report", function (error, report) {
    if (error) {
      console.error("Error in kafka delivery report")
      console.error(error)
    } else {
      console.log(`Kafka delivery report: ${JSON.stringify(report)}`)
    }
  })
  producer.setPollInterval(100)
  producer.on("ready", async () => {
    // Here we start the triggers on data subjects & policies changes
    watchDataSubjects()
    watchPolicies()

    server = app.listen(process.env["SERVER_PORT"] || 80, () => {
      let { address, port } = server.address()
      address = address === "::" ? "0.0.0.0" : address
      redirectUri = process.env["SERVER_AUTH_CALLBACK"] || "/callback"
      console.debug("App listening at http://%s:%s", address, port)
    })
  })
}
init()

app.use(childlogger)

app.use(session({
  secret: process.env["SESSION_SECRET"] || crypto.randomBytes(20).toString("hex") || "super secret"
}))

const clientId = process.env["AUTH_CLIENT_ID"] || "special-platform"
const clientSecret = process.env["AUTH_CLIENT_SECRET"] || "special-platform-secret"

app.use("/callback", (req, res, next) => {
  const state = JSON.parse(Buffer.from(req.query.state, "base64"))
  if (state.nonce !== req.session.nonce) return next({"status": "403", "message": "Unknown state"})

  const authCode = req.query.code

  var clientServerOptions = {
    "headers": {
      // Using "auth" does not work with POST on request library for now, see: https://github.com/request/request/issues/2777
      "Authorization": "Basic " + Buffer.from(clientId + ":" + clientSecret).toString("base64")
    },
    "uri": process.env["AUTH_TOKEN_ENDPOINT"] || "http://localhost:8080/auth/realms/master/protocol/openid-connect/token",
    // // See above, using "auth" does not work with request library for now
    // "auth": {
    //  "user": "special-platform",
    //  "pass": "760b7a62-058d-4095-b090-ccf07d1d1b8f",
    //  "sendImmediately": false
    // },
    "form": {
      "grant_type": "authorization_code",
      "redirect_uri": redirectUri,
      "code": authCode
    },
    "json": true,
    "method": "POST",
    "content-type": "application/x-www-form-urlencoded"
  }

  let conn = null
  return r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout})
    .then(dbconn => {
      conn = dbconn
      return conn
    })
    .then(() => {
      return request(clientServerOptions)
    })
    .then(response => {
      let accessToken = response["access_token"]
      var clientServerOptions = {
        "uri": process.env["AUTH_USERINFO_ENDPOINT"] || "http://localhost:8080/auth/realms/master/protocol/openid-connect/userinfo",
        "form": {
          "access_token": accessToken
        },
        "json": true,
        "method": "POST",
        "content-type": "application/x-www-form-urlencoded"
      }

      return request(clientServerOptions)
    })
    .then(response => {
      response["id"] = response["sub"]
      delete response["sub"]
      return response
    })
    .then(user => {
      return dataSubjectsTable.insert(Object.assign({}, user, {"policies": []}), {
        "conflict": function (id, oldDoc, newDoc) { return newDoc.merge({"policies": oldDoc("policies")}) }
      }).run(conn).then(updateResult => {
        console.debug("User [%s] updated: %s", user["id"], JSON.stringify(updateResult))
        return user
      })
    })
    .then(user => {
      req.session.user = user
      req.session.authenticated = true
      res.redirect(state.referer)
      return user
    })
    .catch(error => {
      console.error("Error when authorizing client: %s", error)
      console.error(error)
      return next({"code": 403, "message": "Not authorized"})
    })
    .finally(() => {
      conn.close()
    })
})

app.use((req, res, next) => {
  if (req.session.authenticated) {
    // Check authorization status (expired or not)
    return next()
  }

  if (!req.session.nonce) {
    req.session.nonce = crypto.randomBytes(20).toString("hex")
  }
  const state = {
    "nonce": req.session.nonce,
    "referer": req.header("Referer")
  }
  const options = {
    "scope": "all openid",
    "response_type": "code",
    "client_id": clientId,
    "redirect_uri": redirectUri,
    "state": Buffer.from(JSON.stringify(state)).toString("base64")
  }
  let authRedirect = `${baseAuthURL}?${querystring.stringify(options)}`
  return res.status(401).location(authRedirect).end()
})

app.use(bodyParser.json())
app.use(createConnection)

app.use(applications)
app.use(dataSubjects)
app.use(policies)

app.use(closeConnection)
app.use(errorHandler)

// Handle SIGTERM gracefully
process.on("SIGTERM", gracefulShutdown)
process.on("SIGINT", gracefulShutdown)
process.on("SIGHUP", gracefulShutdown)
function gracefulShutdown () {
  // Serve existing requests, but refuse new ones
  console.warn("Received signal to terminate: wrapping up existing requests")
  server.close(() => {
    // Exit once all existing requests have been served
    console.warn("Received signal to terminate: done serving existing requests. Exiting")
    process.exit(0)
  })
}

let deletedPolicies = {}
async function watchPolicies () {
  console.debug("Starting to watch policies changes...")
  let conn = await r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout})
  let cursor = await dataControllerPoliciesTable.changes({"include_types": true}).run(conn)
  return cursor.each(async (error, row) => {
    if (error) {
      console.error("Error occurred when watching policies changes: %s", error)
      console.error(error)
    } else if (row["type"] === "remove") {
      let policyId = row["old_val"]["id"]
      // In order to populate that change logs topic, we need to keep in memory the deleted policies for some time
      deletedPolicies[policyId] = row["old_val"]

      applicationsTable
        .filter(application => { return r.expr(application("policies")).coerceTo("array").contains(policyId) })
        .update({"policies": r.row("policies").difference([policyId])})
        .run(conn)
        .then(updateResult => {
          console.debug("Applications updated to remove policy [%s]: %s", policyId, JSON.stringify(updateResult))
        })
        .catch(error => {
          console.error("Could not update Applications to remove policy [%s]: %s", policyId, error)
          console.error(error)
        })

      dataSubjectsTable
        .filter(dataSubject => { return r.expr(dataSubject("policies")).coerceTo("array").contains(policyId) })
        .update({"policies": r.row("policies").difference([policyId])})
        .run(conn)
        .then(updateResult => {
          console.debug("Data subjects updated to remove policy [%s]: %s", policyId, JSON.stringify(updateResult))
          // // By now, the kafka topic has probably been updated, we can safely remove the policy from the memory.
          // delete deletedPolicies[policyId] // Let's keep the deleted policies in memory
        })
        .catch(error => {
          console.error("Could not update Applications to remove policy [%s]: %s", policyId, error)
          console.error(error)
        })
    }
  }, () => {
    return conn.close()
  })
}

async function watchDataSubjects () {
  console.debug("Starting to watch data subject changes...")
  let conn = await r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout})
  let cursor = await dataSubjectsTable.changes({"includeInitial": true}).run(conn)
  return cursor.each(async (error, row) => {
    if (error) {
      console.error("Error occurred on data subject modification: %s", error)
      console.error(error)
      return
    }

    let policyIds = []
    if (row["old_val"]) {
      policyIds = policyIds.concat(row["old_val"]["policies"])
    }
    if (row["new_val"]) {
      policyIds = policyIds.concat(row["new_val"]["policies"])
    }

    let policies = {}
    try {
      let cursor = await dataControllerPoliciesTable.getAll(r.args(r.expr(policyIds).distinct())).run(conn)
      let policiesArr = await cursor.toArray()
      policiesArr.forEach(policy => {
        policies[policy["id"]] = policy
        delete policy["id"]
      })
    } catch (error) {
      console.error("Couldn't fetch policies: %s", error)
      console.error(error)
    }

    let withdrawn = []
    let added = []
    let newPolicies = null
    let dataSubjectId = null
    if (!row["new_val"]) {
      dataSubjectId = row["old_val"]["id"]
      // Remove policies from topic
      console.debug("User [%s] removed. ", dataSubjectId)

      withdrawn = row["old_val"]["policies"]
      newPolicies = null
    } else {
      dataSubjectId = row["new_val"]["id"]

      if (row["old_val"]) {
        // Check and propagate withdrawals of consent to history kafka topic
        withdrawn = row["old_val"]["policies"].filter(item => { return !row["new_val"]["policies"].includes(item) })
        added = row["new_val"]["policies"].filter(item => { return !row["old_val"]["policies"].includes(item) })
      } else {
        // New data subject
        added = row["new_val"]["policies"]
      }

      // Create new list of policies for data subject
      console.debug("Data subject policies modified, generating new set of policies.")

      newPolicies = {
        "simplePolicies": row["new_val"]["policies"].map(policy => {
          let simplePolicy = Object.assign({}, policies[policy])
          delete simplePolicy["explanation"]
          return simplePolicy
        })
      }
    }

    let messages = []
    for (let consent of withdrawn) {
      console.debug("Removing data subject [%s] consent for policy [%s].", dataSubjectId, consent)
      let message = policies[consent]
      if (!message) {
        // Policy no longer exists in DB, checking deleted policies.
        console.debug("Policy [%s] deleted, checking recently deleted policies.", consent)
        message = deletedPolicies[consent] || {}
        message["deleted-policy"] = true
      }
      message["given"] = false
      message["data-subject"] = dataSubjectId
      delete message["id"]

      messages.push(message)
    }

    for (let consent of added) {
      console.debug("Adding data subject [%s] consent for policy [%s].", dataSubjectId, consent)
      let message = Object.assign({}, policies[consent])
      message["given"] = true
      message["data-subject"] = dataSubjectId

      messages.push(message)
    }

    for (let message of messages) {
      try {
        console.debug("\nProducing on topic [%s] : %s\n", changeLogsTopic, JSON.stringify(message))
        producer.produce(
          changeLogsTopic, // Topic
          null, // Partition, null uses default
          Buffer.from(JSON.stringify(message)), // Message
          dataSubjectId,
          Date.now()
        )
        producer.flush()
      } catch (error) {
        console.error("An error occurred when trying to send message to Kafka topic [%s]: %s", changeLogsTopic, error)
        console.error(error)
      }
    }

    try {
      console.debug("\nProducing on topic [%s] : %s\n", fullPolicyTopic, JSON.stringify(newPolicies))
      producer.produce(
        fullPolicyTopic, // Topic
        null, // Partition, null uses default
        newPolicies ? Buffer.from(JSON.stringify(newPolicies)) : null, // Either null in case of removal or the new set of policies
        dataSubjectId, // To ensure we only keep the latest set of policies
        Date.now()
      )
      producer.flush()
    } catch (error) {
      console.error("An error occurred when trying to send message to Kafka topic [%s]: %s", fullPolicyTopic, error)
      console.error(error)
    }
  }, () => {
    return conn.close()
  })
}

function createConnection (req, res, next) {
  return r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout}).then(function (conn) {
    req._rdbConn = conn
    console.debug("Creating connection to database for request %s...", req.url)
    next()
  }).catch(error => { next(error) })
}

function closeConnection (req, res, next) {
  console.debug("Closing connection to database for request %s...", req.url)
  req._rdbConn.close()
  next()
}

function errorHandler (error, req, res, next) {
  console.error("Error occurred in /consent-manager: %s", JSON.stringify(error))
  console.error(error)
  if (req._rdbConn) req._rdbConn.close()
  res.status(error.status || 500).json({"error": error.message || error})
  next()
}
