let router = require("express").Router()

const rethink = require("../../utils/rethinkdb_config")
const {
  r,
  applicationsTable,
  dataControllerPoliciesTable,
  dataSubjectsTable
} = rethink

// *******************************
// * Process:         => returns the policies for a data subject
// * Input:
//  - id              => data subject id
//  - application-id  => the ID of the application making the call, obtained through a header
// * Output:
//  - 200             => array of policies filtered by application-id if existing
//  - (4|5)xx         => error
// *******************************
router.get("/:id/policies", (req, res, next) => {
  try {
    checkUserAccess(req)
  } catch (error) {
    return next({status: 401, message: error.message})
  }
  const reqId = req.params.id
  const userId = req.session.user.id
  const id = reqId === "current" ? userId : reqId

  console.debug("Received request to get policies for data subject: %s", id)

  let appId = req.header("Application-Id")

  let query = dataControllerPoliciesTable.getAll(r.args(dataSubjectsTable.get(id)("policies").default([]).coerceTo("array")))

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
    .catch(error => { next(error) })
    .finally(() => { next() })
})

// *******************************
// * Process:         => returns a data subject
// * Input:
//  - id              => data subject id
// * Output:
//  - 200             => a single data subject
//  - (4|5)xx         => error
// *******************************
router.get("/:id", (req, res, next) => {
  try {
    checkUserAccess(req)
  } catch (error) {
    return next({status: 401, message: error.message})
  }
  let reqId = req.params.id
  let userId = req.session.user.id

  console.debug("Received request to get data subject: %s", userId)

  dataSubjectsTable.get(userId).default({}).without({"policies": true}).run(req._rdbConn)
    .then(dataSubject => {
      if (!dataSubject["id"]) return next({"status": 404, "message": "User does not exist / You are not authorized"})
      dataSubject["links"] = {
        "policies": "/users/" + reqId + "/policies"
      }
      dataSubject["id"] = reqId
      res.status(200).json({"users": [dataSubject]})
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

// *******************************
// * Process:         => update a data subject
// * Input:
//  - id              => data subject id
//  - dataSubject     => new content for data subject
//  - application-id  => the ID of the application making the call, obtained through a header
// * Output:
//  - 200             => updated data subject
//  - (4|5)xx         => error
// *******************************
router.put("/:id", (req, res, next) => {
  try {
    checkUserAccess(req)
  } catch (error) {
    return next({status: 401, message: error.message})
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
  } else {
    query = query("policies").default([]).coerceTo("array").concatMap(item => { return item })
  }
  query
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(appPolicies => {
      return dataSubjectsTable.get(userId).run(req._rdbConn).then(dbUser => {
        if (!dbUser["id"]) return next({"status": 404, "message": "User does not exist / You are not authorized"})
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
        if (updateResult["skipped"] > 0 &&
          updateResult["replaced"] === 0 &&
          updateResult["unchanged"] === 0) {
          return next({"status": 404, "message": "User does not exist / You are not authorized"})
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
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

function checkUserAccess (req) {
  try {
    let reqId = req.params.id
    let userId = req.session.user.id
    if (reqId === "current") return userId
    if (reqId === userId) return userId

    // Not current and not the same, check
    // TODO: Check if admin
    throw new Error("Not Authorized")
  } catch (error) {
    throw new Error("Current user is not authorized to do this")
  }
}

module.exports = router
