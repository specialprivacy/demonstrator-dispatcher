let router = require("express").Router()

const APIError = require("../../utils/api-error.js")
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
    return next(error)
  }

  const userObject = getUserFromRequest(req)

  const reqId = req.params.id
  const userId = userObject.id
  const id = reqId === "current" ? userId : reqId

  req.log.debug({ userId: id }, "Received request to get policies for data subject")

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
      res.status(200).json({ "policies": policies })
    })
    .catch(error => next(error))
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
  // add logic to make sure we upsert the user in the users table
  // logic can come from the old oauth middleware

  try {
    checkUserAccess(req)
  } catch (error) {
    return next(error)
  }

  const userObject = getUserFromRequest(req)

  if (req.params.id === "current") {
    upsertUserInDatabase(userObject, req)
  }

  let reqId = req.params.id
  let userId = userObject.id

  req.log.debug({ userId }, "Received request to get data subject")

  dataSubjectsTable.get(userId).default({}).without({ "policies": true }).run(req._rdbConn)
    .then(dataSubject => {
      if (!dataSubject["id"]) return next(new APIError({ statusCode: 404, detail: "User does not exist / You are not authorized" }))
      dataSubject["links"] = {
        "policies": "/users/" + reqId + "/policies"
      }
      dataSubject["id"] = reqId
      res.status(200).json({ "users": [dataSubject] })
    })
    .catch(error => next(error))
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
    return next(error)
  }

  const userObject = getUserFromRequest(req)

  let reqId = req.params.id
  let userId = userObject.id

  req.log.debug({ userId }, "Received request to update data subject")

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
        if (!dbUser["id"]) return next(new APIError({ statusCode: 404, detail: "User does not exist / You are not authorized" }))
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
          return next(new APIError({ statusCode: 404, detail: "User does not exist / You are not authorized" }))
        }
        req.log.debug({ userId, updateResult }, "User updated")
        dbUser["policies"] = newPolicies
        return dbUser
      })
    })
    .then(newUser => {
      // Need to set it back to whatever the frontend sent
      newUser["id"] = reqId
      res.status(200).send({ "user": newUser })
    })
    .catch(error => next(error))
})

function checkUserAccess (request) {
  try {
    if (request.headers["x-userinfo"]) {
      const userObject = getUserFromRequest(request)
      const requestUserId = request.params.id

      if (userObject && requestUserId === "current") {
        return
      }

      if (userObject && requestUserId === userObject.id) {
        return
      }
    }
    throw new APIError({ statusCode: 401, detail: "Not Authorized" })
  } catch (error) {
    throw new APIError({ statusCode: 401, detail: "Current user is not authorized to do this" })
  }
}

function getUserFromRequest (request) {
  // extract user from x-userinfo header, added by Kong OIDC
  const encodedUserString = request.headers["x-userinfo"]
  const decodedUserString = Buffer.from(encodedUserString, "base64").toString("ascii")
  const userObject = JSON.parse(decodedUserString)

  // put id where we want it
  userObject["id"] = userObject["sub"]
  delete userObject["sub"]

  return userObject
}

function upsertUserInDatabase (user, request) {
  // We update the users table with the logged in user
  // if he didn't exist yet, we give him an empty set of policies
  // if he existed, we just updated his other information
  return dataSubjectsTable.insert(Object.assign({}, user, { "policies": [] }), {
    "conflict": function (id, oldDoc, newDoc) { return newDoc.merge({ "policies": oldDoc("policies") }) }
  }).run(request._rdbConn).then(updateResult => {
    request.log.debug({ userId: user["id"], updateResult }, "User updated")
    return user
  })
}

module.exports = router
