var express = require("express")
var app = module.exports = express()
app.disable("x-powered-by")

const rethink = require("../../utils/rethinkdb_config")
const {
  r,
  applicationsTable,
  dataControllerPoliciesTable
} = rethink

function handleError (res) {
  return function (error) {
    console.error("Error occurred in /policies: %s", error)
    res.status(500).send({error: error.message})
  }
}

app.get("/policies/:id", (req, res, next) => {
  let policyId = req.params.id
  console.debug("Received request to get policy: %s", policyId)

  if (!policyId) {
    console.error("No id specified, can't GET.")
    res.status(400).send()
    next()
  }

  dataControllerPoliciesTable.get(policyId).run(req._rdbConn)
    .then(policy => {
      return res.status(200).json({"policies": [policy]})
    })
    .finally(() => {
      next()
    })
    .error(handleError(res))
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
      console.debug("Policy [%s] updated: %s", policyId, JSON.stringify(updateResult))
      return res.status(200).json(policy)
    })
    .finally(() => {
      next()
    })
    .error(handleError(res))
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
      console.debug("Policy [%s] deleted: %s", policyId, JSON.stringify(updateResult))
      return res.status(204).send()
    })
    .finally(() => {
      next()
    })
    .error(handleError(res))
})

// Get policies for an application
app.get("/policies", (req, res, next) => {
  console.debug("Received request to get policies")
  let appId = req.header("APP_KEY")

  let query = dataControllerPoliciesTable

  // If an APP_KEY is specified, we get the policies for that application only.
  if (appId) {
    query = query.filter(policy => { return r.expr(applicationsTable.get(appId)("policies")).contains(policy("id")) })
  } else {
    // TODO: ADMIN, otherwise error
  }
  query
    .map(row => { return row.merge({"type": "policy"}) })
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(policies => {
      return res.status(200).json({"policies": policies})
    })
    .finally(() => { next() })
    .error(handleError(res))
})

app.post("/policies", (req, res, next) => {
  console.debug("Received request to POST new policy")

  let policy = req.body.policy

  dataControllerPoliciesTable.insert(policy, {"return_changes": true}).run(req._rdbConn)
    .then(updateResult => {
      console.debug("Policy inserted: %s", JSON.stringify(updateResult))
      return res.status(200).json({"policy": updateResult.changes[0]["new_val"]})
    })
    .finally(() => {
      next()
    })
    .error(handleError(res))
})