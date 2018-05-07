var express = require("express")
var app = module.exports = express()
app.disable("x-powered-by")

const rethink = require("../../utils/rethinkdb_config")
const {
  r,
  applicationsTable,
  dataControllerPoliciesTable
} = rethink

app.get("/policies/:id", (req, res, next) => {
  let policyId = req.params.id
  console.debug("Received request to get policy: %s", policyId)

  if (!policyId) {
    console.error("No id specified, can't GET.")
    res.status(400).send()
    next()
  }

  dataControllerPoliciesTable.get(policyId).default({}).run(req._rdbConn)
    .then(policy => {
      if (!policy["id"]) return next({"status": 404, "message": "Policy does not exist / You're not authorized"})
      return res.status(200).json({"policies": [policy]})
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

app.put("/policies/:id", (req, res, next) => {
  let policyId = req.params.id
  if (!policyId) {
    console.error("No id specified, can't update.")
    res.status(400).send()
    next()
  }
  console.debug("Received request to update policy: %s", policyId)

  let policy = req.body.policy
  policy["id"] = policyId

  dataControllerPoliciesTable.get(policyId).update(policy).run(req._rdbConn)
    .then(updateResult => {
      if (updateResult["skipped"] > 0 &&
        updateResult["replaced"] === 0 &&
        updateResult["unchanged"] === 0) {
        return next({"status": 404, "message": "Policy does not exist / You are not authorized"})
      }
      console.debug("Policy [%s] updated: %s", policyId, JSON.stringify(updateResult))
      return res.status(200).json(policy)
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

app.delete("/policies/:id", (req, res, next) => {
  let policyId = req.params.id
  if (!policyId) {
    console.error("No id specified, can't delete.")
    res.status(400).send()
    next()
  }
  console.debug("Received request to delete policy: %s", policyId)

  dataControllerPoliciesTable.get(policyId).delete().run(req._rdbConn)
    .then(updateResult => {
      if (updateResult["skipped"] > 0 &&
        updateResult["replaced"] === 0 &&
        updateResult["unchanged"] === 0) {
        return next({"status": 404, "message": "Policy does not exist / You are not authorized"})
      }
      console.debug("Policy [%s] deleted: %s", policyId, JSON.stringify(updateResult))
      return res.status(204).send()
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

// Get policies for an application
app.get("/policies", (req, res, next) => {
  console.debug("Received request to get policies")
  let appId = req.header("Application-Id")

  let query = dataControllerPoliciesTable

  // If an Application-Id is specified, we get the policies for that application only.
  if (appId) {
    query = query.filter(policy => { return r.expr(applicationsTable.get(appId)("policies")).contains(policy("id")) })
  } else {
    // TODO: ADMIN, otherwise error
  }
  query
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(policies => {
      return res.status(200).json({"policies": policies})
    })
    .catch(error => { next(error) })
    .finally(() => { next() })
})

app.post("/policies", (req, res, next) => {
  console.debug("Received request to POST new policy")

  let policy = req.body.policy

  dataControllerPoliciesTable.insert(policy, {"return_changes": true}).run(req._rdbConn)
    .then(updateResult => {
      console.debug("Policy inserted: %s", JSON.stringify(updateResult))
      return res.status(200).json({"policy": updateResult.changes[0]["new_val"]})
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

function errorHandler (err, req, res, next) {
  console.error("Error occurred in /policies: %s", JSON.stringify(err))
  next(err)
}

app.use(errorHandler)
