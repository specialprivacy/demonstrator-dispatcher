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

// Get policies linked to specified data subject
app.get("/users/:id/policies", (req, res, next) => {
  try {
    checkUserAccess(req)
  }
  catch(error){
    return next(error)
  }
  let reqId = req.params.id
  let userId = req.session.user.id

  console.debug("Received request to get policies for data subject: %s", userId)

  let appId = req.header("Application-Id")

  let query = dataControllerPoliciesTable.getAll(r.args(dataSubjectsTable.get(userId)("policies").default([]).coerceTo("array")))

  // If an Application-Id is specified, we get the policies for that application only.
  if (appId) {
    query = query.filter(policy => { return r.expr(applicationsTable.get(appId)("policies").default([])).contains(policy("id")) })
  }
  query
    .default([])
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(policies => {
      res.status(200).json({"policies": policies})
    })
    .catch(error => {next(error)})
    .finally(() => { next() })
})

app.get("/users/:id", (req, res, next) => {
  try {
    checkUserAccess(req)
  }
  catch(error){
    return next(error)
  }
  let reqId = req.params.id
  let userId = req.session.user.id

  console.debug("Received request to get data subject: %s", userId)

  dataSubjectsTable.get(userId).default({}).without({"policies": true}).run(req._rdbConn)
    .then(dataSubject => {
      if(!dataSubject["id"]) throw {"status": 404, "message": "User does not exist / You are not authorized"}
      dataSubject["links"] = {
        "policies": "/users/" + reqId + "/policies"
      }
      dataSubject["id"] = reqId
      res.status(200).json({"users": [dataSubject]})
    })
    .catch(error => {next(error)})
    .finally(() => {
      next()
    })
})

app.put("/users/:id", (req, res, next) => {
  try {
    checkUserAccess(req)
  }
  catch(error){
    return next(error)
  }
  let reqId = req.params.id
  let userId = req.session.user.id
  console.debug("Received request to update data subject: %s", userId)

  let appId = req.header("Application-Id")
  let dataSubject = req.body.user

  let query = applicationsTable
  let dbUser = null

  // If an Application-Id is specified, we get the policies for that application only.
  if (appId) {
    query = query.get(appId)("policies").default([])
  }
  else {
    query = query("policies").default([]).coerceTo("array").concatMap(item => {return item})
  }
  query
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(appPolicies => {
      return dataSubjectsTable.get(userId).run(req._rdbConn).then(dbUser => {
        if(!dbUser["id"]) throw {"status": 404, "message": "User does not exist / You are not authorized"}
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

      return dataSubjectsTable.get(userId).update(dbUser).run(req._rdbConn).then(updateResult => {
        if(updateResult["skipped"] > 0 &&
          updateResult["replaced"] === 0 &&
          updateResult["unchanged"] === 0)
        {
          throw {"status": 404, "message": "User does not exist / You are not authorized"}
        }
        console.debug("User [%s] updated: %s", userId, JSON.stringify(updateResult))
        dbUser["policies"] = newPolicies
        return dbUser
      })
    })
    .then(newUser => {
      // Need to set it back to whatever the frontend sent
      newUser["id"] = reqId
      res.status(200).send({"user": newUser})
    })
    .catch(error => {next(error)})
    .finally(() => {
      next()
    })
})

function errorHandler (err, req, res, next) {
  console.error("Error occurred in /data-subjects: %s", JSON.stringify(err))
  next(err)
}

app.use(errorHandler)

function checkUserAccess (req) {
  try {
    let reqId = req.params.id
    let userId = req.session.user.id
    if(reqId === "current") return userId
    if(reqId === userId) return userId
  }
  catch(error) {
    next({"error": {"code": 401, "message": "Not authorized"}})
  }

  // Not current and not the same, check
  // TODO: Check if admin
  next({"error": {"code": 403, "message": "Not admin"}})
}
