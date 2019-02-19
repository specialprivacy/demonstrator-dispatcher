const Kafka = require("node-rdkafka")
const log = require("./log")
const producer = new Kafka.Producer({
  "metadata.broker.list": process.env["KAFKA_BROKER_LIST"] || "localhost:9092, localhost:9094",
  "api.version.request": process.env["KAFKA_VERSION_REQUEST"] || false,
  "dr_cb": true
})

const changeLogsTopic = process.env["KAFKA_CHANGE_LOGS_TOPIC"] || "policies-audit"
const fullPolicyTopic = process.env["KAFKA_FULL_POLICIES_TOPIC"] || "full-policies"

const {
  dbHost,
  dbPort,
  dbTimeout,
  r,
  applicationsTable,
  dataControllerPoliciesTable,
  dataSubjectsTable
} = require("./rethinkdb_config")

const MAX_RETRIES = 10
let retryCount = 0

function startWatching () {
  const connectOptions = { "timeout": process.env["KAFKA_TIMEOUT"] || 5000 }
  producer.connect(connectOptions)
  producer.on("connection.failure", function (error) {
    if (retryCount >= MAX_RETRIES) {
      log.error({ err: error }, "Could not connect to Kafka, exiting")
      process.exit(1)
    }
    retryCount++
    const timeout = (Math.pow(2, retryCount) + Math.random()) * 1000
    log.warn({ err: error, timeout, retryCount }, `Failed to connect to kafka, retrying in ${timeout} ms`)
    setTimeout(producer.connect.bind(producer), timeout, connectOptions)
  })
  producer.on("event.error", function (error) {
    log.error({ err: error }, "Error from kafka producer")
  })
  producer.on("delivery-report", function (error, report) {
    if (error) {
      log.error({ err: error }, "Error in kafka delivery report")
    } else {
      log.info({ report }, "Kafka delivery report")
    }
  })
  producer.setPollInterval(100)

  producer.on("ready", async () => {
    // Here we start the triggers on data subjects & policies changes
    watchDataSubjects()
    watchPolicies()
  })
}

let deletedPolicies = {}
async function watchPolicies (retryCount = 0) {
  log.debug("Starting to watch policies changes...")
  let conn
  try {
    conn = await r.connect({ "host": dbHost, "port": dbPort, "timeout": dbTimeout })
  } catch (err) {
    if (retryCount >= MAX_RETRIES) {
      log.error({ err }, "Failed to connect to rethinkdb and out of retries. Exiting.")
      process.exit(1)
    }
    const timeout = (Math.pow(2, retryCount + 1) + Math.random()) * 1000
    log.warn({ err, timeout, retryCount }, `Failed to connect to rethinkdb retrying in ${timeout} ms`)
    return setTimeout(watchPolicies, timeout, retryCount + 1)
  }
  // Watch changes to the Data Controller Policy table in order to propagate deletions.
  let cursor = await dataControllerPoliciesTable.changes({ "include_types": true }).run(conn)
  return cursor.each(async (error, row) => {
    if (error) {
      log.error({ err: error }, "Error occurred when watching policies changes")
    } else if (row["type"] === "remove") {
      let policyId = row["old_val"]["id"]
      // In order to populate that change logs topic, we need to keep in memory the deleted policies for some time
      deletedPolicies[policyId] = row["old_val"]

      applicationsTable
      // Get applications which have this policy
        .filter(application => { return r.expr(application("policies")).coerceTo("array").contains(policyId) })
        // Update those applications to remove the deleted policy
        .update({ "policies": r.row("policies").difference([policyId]) })
        .run(conn)
        .then(updateResult => {
          log.debug({ policyId, updateResult }, "Applications updated to remove policy")
        })
        .catch(error => {
          log.error({ policyId, err: error }, "Could not update Applications to remove policy")
        })

      dataSubjectsTable
      // Get data subjects who have this policy
        .filter(dataSubject => { return r.expr(dataSubject("policies")).coerceTo("array").contains(policyId) })
        // Update those data subjects to remove the deleted policy
        .update({ "policies": r.row("policies").difference([policyId]) })
        .run(conn)
        .then(updateResult => {
          log.debug({ policyId, updateResult }, "Data subjects updated to remove policy")
          // // By now, the kafka topic has probably been updated, we can safely remove the policy from the memory.
          // delete deletedPolicies[policyId] // Let's keep the deleted policies in memory
        })
        .catch(error => {
          log.error({ policyId, err: error }, "Could not update Applications to remove policy")
        })
    }
  }, () => {
    return conn.close()
  })
}

async function watchDataSubjects () {
  log.debug("Starting to watch data subject changes...")
  let conn
  try {
    conn = await r.connect({ "host": dbHost, "port": dbPort, "timeout": dbTimeout })
  } catch (err) {
    if (retryCount >= MAX_RETRIES) {
      log.error({ err }, "Failed to connect to rethinkdb and out of retries. Exiting.")
      process.exit(1)
    }
    const timeout = (Math.pow(2, retryCount + 1) + Math.random()) * 1000
    log.warn({ err, timeout, retryCount }, `Failed to connect to rethinkdb retrying in ${timeout} ms`)
    return setTimeout(watchDataSubjects, timeout, retryCount + 1)
  }
  // Watch every change to the data subject table, including the ones that already exist
  let cursor = await dataSubjectsTable.changes({ "includeInitial": true }).run(conn)
  return cursor.each(async (error, row) => {
    if (error) {
      log.error({ err: error }, "Error occurred on data subject modification: %s", error)
      return
    }

    // A data subject has been modified. We now need to generate a new data subject profile and find the changes
    let policyIds = []
    if (row["old_val"]) {
      // Get policy ids before update
      policyIds = policyIds.concat(row["old_val"]["policies"])
    }
    if (row["new_val"]) {
      // Get policy ids after update
      policyIds = policyIds.concat(row["new_val"]["policies"])
    }

    let policies = {}
    try {
      // Fetch data for the policies found above
      let cursor = await dataControllerPoliciesTable.getAll(r.args(r.expr(policyIds).distinct())).run(conn)
      let policiesArr = await cursor.toArray()
      // We'll create a hash where the key is the ID of a policy and the value is the policy itself.
      policiesArr.forEach(policy => {
        policies[policy["id"]] = policy
        delete policy["id"]
      })
    } catch (error) {
      log.error({ err: error }, "Couldn't fetch policies, can't update data subject profile")
      return
    }

    let withdrawn = []
    let added = []
    let newPolicies = null
    let dataSubjectId = null
    if (!row["new_val"]) {
      // The data subject was deleted
      dataSubjectId = row["old_val"]["id"]
      log.debug({ userId: dataSubjectId }, "User removed")

      withdrawn = row["old_val"]["policies"]
      newPolicies = null
    } else {
      // The data subject was inserted or updated
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
      log.debug("Data subject policies modified, generating new set of policies.")
      newPolicies = {
        "timestamp": new Date().getTime(),
        "userID": dataSubjectId,
        "simplePolicies": row["new_val"]["policies"].map(policy => {
          let simplePolicy = Object.assign({}, policies[policy])
          delete simplePolicy["explanation"]
          return simplePolicy
        })
      }
    }

    let messages = []
    for (let consent of withdrawn) {
      log.debug({ userId: dataSubjectId, policyId: consent }, "Removing data subject consent for policy")
      let message = policies[consent]
      if (!message) {
        // Policy no longer exists in DB, checking deleted policies.
        log.debug({ policyId: consent }, "Policy deleted, checking recently deleted policies")
        message = deletedPolicies[consent] || {}
        message["deleted-policy"] = true
      }
      message["given"] = false
      message["data-subject"] = dataSubjectId
      delete message["id"]

      messages.push(message)
    }

    for (let consent of added) {
      log.debug({ userId: dataSubjectId, policyId: consent }, "Adding data subject consent for policy")
      let message = Object.assign({}, policies[consent])
      message["given"] = true
      message["data-subject"] = dataSubjectId

      messages.push(message)
    }

    for (let message of messages) {
      try {
        log.debug({ topic: changeLogsTopic, message }, "Producing on topic")
        producer.produce(
          changeLogsTopic, // Topic
          null, // Partition, null uses default
          Buffer.from(JSON.stringify(message)), // Message
          dataSubjectId,
          Date.now()
        )
      } catch (error) {
        log.error({ topic: changeLogsTopic, err: error }, "An error occurred when trying to send message to Kafka topic")
      }
    }

    try {
      log.debug({ topic: fullPolicyTopic, message: newPolicies }, "Producing on topic")
      producer.produce(
        fullPolicyTopic, // Topic
        null, // Partition, null uses default
        newPolicies ? Buffer.from(JSON.stringify(newPolicies)) : null, // Either null in case of removal or the new set of policies
        dataSubjectId, // To ensure we only keep the latest set of policies
        Date.now()
      )
    } catch (error) {
      log.error({ topic: fullPolicyTopic, err: error }, "An error occurred when trying to send message to Kafka topic")
    }
    producer.flush()
  }, () => {
    return conn.close()
  })
}

module.exports = {
  producer,
  startWatching
}
