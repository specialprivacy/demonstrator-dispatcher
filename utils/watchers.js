const Kafka = require("node-rdkafka")
const producer = new Kafka.Producer({
  "metadata.broker.list": process.env["KAFKA_BROKER_LIST"] || "localhost:9092, localhost:9094",
  "api.version.request": process.env["KAFKA_VERSION_REQUEST"] || false,
  "dr_cb": true
})

const changeLogsTopic = process.env["KAFKA_CHANGE_LOGS_TOPIC"] || "policies-audit"
const fullPolicyTopic = process.env["KAFKA_FULL_POLICIES_TOPIC"] || "full-policies"

const rethink = require("./rethinkdb_config")
const {
  dbHost,
  dbPort,
  dbTimeout,
  r,
  applicationsTable,
  dataControllerPoliciesTable,
  dataSubjectsTable
} = rethink

function startWatching () {
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
  })
}

let deletedPolicies = {}
async function watchPolicies () {
  console.debug("Starting to watch policies changes...")
  let conn = await r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout})
  // Watch changes to the Data Controller Policy table in order to propagate deletions.
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
      // Get applications which have this policy
        .filter(application => { return r.expr(application("policies")).coerceTo("array").contains(policyId) })
        // Update those applications to remove the deleted policy
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
      // Get data subjects who have this policy
        .filter(dataSubject => { return r.expr(dataSubject("policies")).coerceTo("array").contains(policyId) })
        // Update those data subjects to remove the deleted policy
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
  // Watch every change to the data subject table, including the ones that already exist
  let cursor = await dataSubjectsTable.changes({"includeInitial": true}).run(conn)
  return cursor.each(async (error, row) => {
    if (error) {
      console.error("Error occurred on data subject modification: %s", error)
      console.error(error)
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
      console.error("Couldn't fetch policies, can't update data subject profile: %s", error)
      console.error(error)
      return
    }

    let withdrawn = []
    let added = []
    let newPolicies = null
    let dataSubjectId = null
    if (!row["new_val"]) {
      // The data subject was deleted
      dataSubjectId = row["old_val"]["id"]
      console.debug("User [%s] removed. ", dataSubjectId)

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

module.exports = {
  producer,
  startWatching
}
