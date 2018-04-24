var express = require("express")
var app = module.exports = express()
app.disable("x-powered-by")

const rethink = require("../../utils/rethinkdb_config")
const {
  r,
  applicationsTable,
  dataControllerPoliciesTable,
  dataSubjectsTable
} = rethink

function handleError (res) {
  return function (error) {
    console.error("Error occurred in /data-subjects: %s", error)
    res.status(500).send({error: error.message})
  }
}

// Get policies linked to specified data subject
app.get("/users/:id/policies", (req, res, next) => {
  let dataSubjectId = req.params.id
  if (!dataSubjectId) {
    console.error("No dataSubjectId specified, can't update.")
    res.status(400).send()
    next()
  }
  console.debug("Received request to get policies for data subject: %s", dataSubjectId)

  let appId = req.header("APP_KEY")

  let query = dataControllerPoliciesTable.getAll(r.args(dataSubjectsTable.get(dataSubjectId)("policies").coerceTo("array")))

  // If an APP_KEY is specified, we get the policies for that application only.
  if (appId) {
    query = query.filter(policy => { return r.expr(applicationsTable.get(appId)("policies")).contains(policy("id")) })
  }
  query
    .map(row => { return row.merge({"dataSubjectId": dataSubjectId, "type": "policy"}) })
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(policies => {
      res.status(200).json({"policies": policies})
    })
    .finally(() => { next() })
    .error(handleError(res))
})

app.get("/users/:id", (req, res, next) => {
  let dataSubjectId = req.params.id
  if (!dataSubjectId) {
    console.error("No dataSubjectId specified, can't update.")
    res.status(400).send()
    next()
  }
  console.debug("Received request to get data subject: %s", dataSubjectId)

  dataSubjectsTable.get(dataSubjectId).without({"policies": true}).run(req._rdbConn)
    .then(dataSubject => {
      dataSubject["links"] = {
        "policies": "/users/" + dataSubject["id"] + "/policies"
      }
      res.status(200).json({"users": [dataSubject]})
    })
    .error(handleError(res))
    .finally(() => {
      next()
    })
})

app.put("/users/:id", (req, res, next) => {
  let dataSubjectId = req.params.id
  if (!dataSubjectId) {
    console.error("No dataSubjectId specified, can't update.")
    res.status(400).send()
    next()
  }
  console.debug("Received request to update data subject: %s", dataSubjectId)

  let appId = req.header("APP_KEY")
  let dataSubject = req.body.user

  let query = applicationsTable
  let dbUser = null

  // If an APP_KEY is specified, we get the policies for that application only.
  if (appId) {
    query = query.get(appId)("policies")
  }
  else {
    query = query("policies").coerceTo("array").concatMap(item => {return item})
  }
  query
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(appPolicies => {
      return dataSubjectsTable.get(dataSubjectId).run(req._rdbConn).then(dbUser => {
        return {
          appPolicies, dbUser
        }
      })
    })
    .then(hash => {
      dbUser = hash["dbUser"]

      let dbPolicies = dbUser["policies"]
      let appPolicies = hash["appPolicies"]
      let newPolicies = dataSubject["policies"]

      for (let appPolicy of appPolicies) {
        if (dbPolicies.includes(appPolicy)) {
          dbPolicies.splice(dbPolicies.indexOf(appPolicy))
        }
      }
      for (let newPolicy of newPolicies) {
        if (appPolicies.includes(newPolicy)) {
          dbPolicies.push(newPolicy)
        }
      }

      return dataSubjectsTable.get(dataSubjectId).update(dbUser).run(req._rdbConn).then(updateResult => {
        console.debug("User [%s] updated: %s", dataSubjectId, JSON.stringify(updateResult))
        dbUser["policies"] = newPolicies
        return dbUser
      })
    })
    .then(newUser => {
      res.status(200).send({"user": newUser})
    })
    .error(handleError(res))
    .finally(() => {
      next()
    })
})
