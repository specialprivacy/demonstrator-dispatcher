// TODO: Add tests and documentation

const app = require("express")()
const bodyParser = require("body-parser")
const Kafka = require("node-rdkafka")

const http = require("http")
http.globalAgent.maxSockets = 10

const applications = require("./lib/applications")
const dataSubjects = require("./lib/data-subjects")
const policies = require("./lib/policies")

const producer = new Kafka.Producer({
  "metadata.broker.list": process.env["KAFKA_BROKER_LIST"] || "localhost:9092, localhost:9094",
  "api.version.request": false
})



const changeLogsTopic = process.env["CHANGE_LOGS_TOPIC"] || "policies-audit"
const fullPolicyTopic = process.env["FULL_POLICIES_TOPIC"] || "full-policies"

const rethink = require("./utils/rethinkdb_config")
const {
  dbHost,
  dbPort,
  r,
  dbName,
  db,
  applicationsTable,
  dataControllerPoliciesTable,
  dataSubjectsTable,
  dbTables
  } = rethink

app.disable("x-powered-by")

var request = require('request-promise');
require('request-debug')(request);

let authorized = {}
let authorizing = {}

app.use(async (req, res, next) => {
  let authCode = req.header("Authorization-Code")
  let redirectUri = req.header("Redirect-Uri")

  if(authCode && authorizing[authCode]){
    let sleep = function (millis) {
      return new Promise(resolve => setTimeout(resolve, millis));
    }
    while(authorizing[authCode]){
      console.debug("User is being authorized, waiting: %s", JSON.stringify(authorizing))
      await sleep(500)
    }
  }
  if(authCode && authorized[authCode]) {
    req._user = authorized[authCode]
    next()
  }
  else {
    authorizing[authCode] = true
    let clientId = "special-platform"
    let secret = "760b7a62-058d-4095-b090-ccf07d1d1b8f"
    var clientServerOptions = {
      "headers": {
        // Using "auth" does not work with POST on request library for now, see: https://github.com/request/request/issues/2777
        "Authorization": "Basic " + new Buffer(clientId + ':' + secret).toString('base64')
      },
      "uri": "http://localhost:8082/auth/realms/master/protocol/openid-connect/token",
      // // See above, using "auth" does not work with request library for now
      //"auth": {
      //  "user": "special-platform",
      //  "pass": "760b7a62-058d-4095-b090-ccf07d1d1b8f",
      //  "sendImmediately": false
      //},
      "form": {
        "grant_type": "authorization_code",
        "redirect_uri": redirectUri,
        "code": authCode
      },
      "json": true,
      "method": "POST",
      "content-type": "application/x-www-form-urlencoded"
    }

    request(clientServerOptions).then(response => {
      let accessToken = response["access_token"]
      var clientServerOptions = {
        "uri": "http://localhost:8082/auth/realms/master/protocol/openid-connect/userinfo",
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
      authorized[authCode] = response
      req._user = authorized[authCode]
      next()
    })
    .catch(error => {
      console.error("Error when authorizing client: %s", error)
      res.status(403).json(error)
    })
    .finally(() => {
      delete authorizing[authCode]
    })
  }
})

// TODO: check if still needed
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*")
  res.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, Content-Length, X-Requested-With, Application-Id")

  // intercepts OPTIONS method
  if (req.method === "OPTIONS") {
    // respond with 200
    res.status(200).send()
  }
  next()
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
  let conn = await r.connect({"host": dbHost, "port": dbPort})
  let cursor = await dataControllerPoliciesTable.changes({"include_types": true}).run(conn)
  return cursor.each(async (err, row) => {
    if (err) {
      console.error("Error occured when watching policies changes: %s", err)
    } else if (row["type"] === "remove") {
      // TODO: Remove that ID from applications and data subjects
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
        })

      dataSubjectsTable
        .filter(dataSubject => { return r.expr(dataSubject("policies")).coerceTo("array").contains(policyId) })
        .update({"policies": r.row("policies").difference([policyId])})
        .run(conn)
        .then(updateResult => {
          console.debug("Data subjects updated to remove policy [%s]: %s", policyId, JSON.stringify(updateResult))
          // By now, the kafka topic has probably been updated, we can safely remove the policy from the memory.
          delete deletedPolicies[policyId]
        })
        .catch(error => {
          console.error("Could not update Applications to remove policy [%s]: %s", policyId, error)
        })
    }
  }, () => {
    return conn.close()
  })
}

async function watchDataSubjects () {
  console.debug("Starting to watch data subject changes...")
  let conn = await r.connect({"host": dbHost, "port": dbPort})
  let cursor = await dataSubjectsTable.changes({"includeInitial": true}).run(conn)
  return cursor.each(async (err, row) => {
    if (err) { console.error("Error occurred on data subject modification: %s", err) }

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
          let simplePolicy = Object.assign({}, policies[policy]);
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
        message = deletedPolicies[consent]
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
      } catch (error) {
        console.error("An error occurred when trying to send message to Kafka topic [%s]: %s", changeLogsTopic, error)
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
    } catch (error) {
      console.error("An error occurred when trying to send message to Kafka topic [%s]: %s", fullPolicyTopic, error)
    }
  }, () => {
    return conn.close()
  })
}

async function generateData () {
  let conn = await r.connect({"host": dbHost, "port": dbPort})
  console.debug("Creating database...")
  try {
    await r.dbCreate(dbName).run(conn, function (err, result) {
      if (!err) { console.debug("Database created: %s", result) }
    })
  } catch (error) { console.debug("Database already exists.") }

  console.debug("Creating tables...")
  try {
    await r.expr(dbTables).forEach(db.tableCreate(r.row)).run(conn, function (err, result) {
      if (!err) { console.debug("Tables created: %s", result) }
    })
  } catch (error) { console.debug("Tables already exist.") }

  let promises = []

  console.debug("Inserting base data")

  promises.push(dataControllerPoliciesTable.insert([
    {
      "id": "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymized",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EU",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Collect",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Account",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Delivery",
      "explanation": "I consent to the collection of my anonymized data in Europe for the purpose of accounting."
    },
    {
      "id": "54ff9c00-1b47-4389-8390-870b2ee9a03c",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Derived",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EULike",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Copy",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Same",
      "explanation": "I consent to the copying of my derived data in Europe-like countries for the purpose of administration."
    },
    {
      "id": "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Computer",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ThirdParty",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Move",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Browsing",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Public",
      "explanation": "I consent to the moving of my computer data on third-party servers for the purpose of browsing."
    },
    {
      "id": "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Activity",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ControllerServers",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Aggregate",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Account",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Delivery",
      "explanation": "I consent to the aggregation of my activity data on your servers for the purpose of accounting."
    },
    {
      "id": "be155566-7b56-4265-92fe-cb474aa0ed42",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymized",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EU",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Analyze",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Ours",
      "explanation": "I consent to the analysis of my anonymized data in Europe for the purpose of administration."
    },
    {
      "id": "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#AudiovisualActivity",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EULike",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymize",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Public",
      "explanation": "I consent to the anonymization of my activity data in Europe-like countries for the purpose of administration."
    },
    {
      "id": "2f274ae6-6c2e-4350-9109-6c15e50ba670",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Computer",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ThirdCountries",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Copy",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Arts",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Same",
      "explanation": "I consent to the copying of my computer data in third countries for the purpose of artistic usage."
    },
    {
      "id": "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Content",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#OurServers",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Derive",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#AuxPurpose",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Unrelated",
      "explanation": "I consent to the derivation of my content data on your servers for auxiliary purposes."
    },
    {
      "id": "86371d81-30ff-49c4-897f-5e6dbc721e85",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Demographic",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ProcessorServers",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Move",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Browsing",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Delivery",
      "explanation": "I consent to the moving of my demographic data on processor servers for the purpose of browsing."
    },
    {
      "id": "4d675233-279f-4b5e-8695-b0b66be4f0f9",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Derived",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ThirdParty",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Aggregate",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Charity",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#OtherRecipient",
      "explanation": "I consent to the aggregation of my derived data on third-party servers for the purpose of charity."
    }
  ], {conflict: "replace"}).run(conn))

  promises.push(applicationsTable.insert([
    {
      "id": "d5aca7a6-ed5f-411c-b927-6f19c36b93c3",
      "name": "Super application",
      "policies":
        [
          "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
          "54ff9c00-1b47-4389-8390-870b2ee9a03c",
          "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
          "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
          "be155566-7b56-4265-92fe-cb474aa0ed42",
          "8a7cf1f6-4c34-497f-8a65-4c985eb47a35"
        ]
    },
    {
      "id": "c52dcc17-89f7-4a56-8836-bad27fd15bb3",
      "name": "Super duper application",
      "policies":
        [
          "be155566-7b56-4265-92fe-cb474aa0ed42",
          "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
          "2f274ae6-6c2e-4350-9109-6c15e50ba670",
          "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
          "86371d81-30ff-49c4-897f-5e6dbc721e85",
          "4d675233-279f-4b5e-8695-b0b66be4f0f9"
        ]
    }
  ], {conflict: "replace"}).run(conn))

  promises.push(dataSubjectsTable.insert([
    {
      "id": "ff4523f7-852e-4758-b7c6-a553c84487e1",
      "name": "Bernard Antoine",
      "policies": [
        "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998"
      ]
    },
    {
      "id": "14f97114-bb25-43d2-85f9-b42c10538c09",
      "name": "Roger Frederick",
      "policies": [
        "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c"
      ]
    }
  ], {conflict: "replace"}).run(conn))

  return Promise.all(promises).then(resolved => {
    console.debug("Data inserted")

    return conn.close()
  })
}

let server = null
async function init () {
  await generateData()
  producer.connect({"timeout": 30000})
  producer.on("connection.failure", function (err) {
    console.error("Could not connect to Kafka, exiting: %s", err)
    process.exit(-1)
  })
  producer.on("event.error", function (err) {
    console.error("Error from kafka producer: %s", err)
  })
  producer.on("ready", async () => {
    // Here we start the triggers on data subjects & policies changes
    watchDataSubjects()
    watchPolicies()

    server = app.listen(8086, () => {
      const { address } = server.address()
      const { port } = server.address()
      console.debug("App listening at http://%s:%s", address, port)
    })
  })
}

init()

function createConnection (req, res, next) {
  return r.connect({"host": dbHost, "port": dbPort}).then(function (conn) {
    req._rdbConn = conn
    console.debug("Creating connection to database for request %s...", req.url)
    next()
  }).catch(error => {next(error)})
}

function closeConnection (req, res, next) {
  console.debug("Closing connection to database for request %s...", req.url)
  req._rdbConn.close()
  next()
}

function errorHandler (err, req, res, next) {
  console.error("Error occurred in /consent-manager: %s", JSON.stringify(err))
  res.status(500).json({"error": err.message})
  next()
}