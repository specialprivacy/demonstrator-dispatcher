var express = require("express")
var app = module.exports = express()
app.disable("x-powered-by")

const rethink = require("../../utils/rethinkdb_config")
const {
  r,
  applicationsTable,
  dataControllerPoliciesTable
} = rethink

// TODO: ADMIN
// *******************************
// * Process:   => returns the policies for an application
// * Input:
//  - id        => application id
// * Output:
//  - 200       => array of policies
//  - (4|5)xx   => error
// *******************************
app.get("/applications/:id/policies", (req, res, next) => {
  let appId = req.params.id
  console.debug("Received request to get policies for application %s", appId)

  if (!appId) {
    console.error("No appId specified, can't GET.")
    return next({status: 400, message: "No application ID specified"})
  }

  dataControllerPoliciesTable.getAll(
    r.args(applicationsTable.get(appId)("policies").default([]).coerceTo("array"))
  ).run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    }).then(policies => {
      return res.status(200).json({policies})
    })
    .catch(error => { next(error) })
    .finally(() => { next() })
})

// TODO: ADMIN
// *******************************
// * Process:   => returns an application
// * Input:
//  - id        => application id
// * Output:
//  - 200       => array containing a single application
//  - (4|5)xx   => error
// *******************************
app.get("/applications/:id", (req, res, next) => {
  let appId = req.params.id
  console.debug("Received request to get application %s", appId)

  if (!appId) {
    console.error("No appId specified, can't GET.")
    return next({status: 400, message: "No application ID specified"})
  }

  // Get application with $appId but replace its policies by a link call
  applicationsTable.get(appId).default({}).without({"policies": true}).run(req._rdbConn)
    .then(application => {
      if (!application["id"]) return next({"status": 404, "message": "Application does not exist / You're not authorized"})
      application["links"] = {
        "policies": "/applications/" + appId + "/policies"
      }
      return res.status(200).json({"applications": [application]})
    })
    .catch(error => { next(error) })
    .finally(() => { next() })
})

// *******************************
// * Process:     => update an application
// * Input:
//  - id          => application id
//  - application => updated application content
// * Output:
//  - 200         => updated application
//  - (4|5)xx     => error
// *******************************
app.put("/applications/:id", (req, res, next) => {
  let appId = req.params.id
  if (!appId) {
    console.error("No id specified, can't update.")
    return next({status: 400, message: "No ID specified, can't update"})
  }
  console.debug("Received request to update application: %s", appId)

  let application = req.body.application
  application["id"] = appId

  applicationsTable.get(appId).update(application).run(req._rdbConn)
    .then(updateResult => {
      if (updateResult["skipped"] > 0 &&
        updateResult["replaced"] === 0 &&
        updateResult["unchanged"] === 0) {
        return next({"status": 404, "message": "Application does not exist / You are not authorized"})
      }
      console.debug("Application [%s] updated: %s", appId, JSON.stringify(updateResult))
      delete application["policies"]
      application["links"] = {
        "policies": "/applications/" + appId + "/policies"
      }
      return res.status(200).json(application)
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

// *******************************
// * Process:     => delete an application
// * Input:
//  - id          => application id
// * Output:
//  - 204         => success, empty response
//  - (4|5)xx     => error
// *******************************
app.delete("/applications/:id", (req, res, next) => {
  let appId = req.params.id
  if (!appId) {
    console.error("No id specified, can't delete.")
    return next({status: 400, message: "No ID specified, can't delete"})
  }
  console.debug("Received request to delete application: %s", appId)

  applicationsTable.get(appId).delete().run(req._rdbConn)
    .then(updateResult => {
      if (updateResult["skipped"] > 0 &&
        updateResult["replaced"] === 0 &&
        updateResult["unchanged"] === 0) {
        return next({"status": 404, "message": "Application does not exist / You are not authorized"})
      }
      console.debug("Application [%s] deleted: %s", appId, JSON.stringify(updateResult))
      return res.status(204).send()
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

// TODO: ADMIN
// *******************************
// * Process:     => get all applications
// * Input:       => /
// * Output:
//  - 200         => array of applications
//  - (4|5)xx     => error
// *******************************
app.get("/applications", (req, res, next) => {
  console.debug("Received request to get applications")

  applicationsTable.without({"policies": true}).run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(applications => {
      applications = applications.map(application => {
        application["links"] = {
          "policies": "/applications/" + application["id"] + "/policies"
        }
        return application
      })
      res.status(200).send({applications})
    })
    .catch(error => { next(error) })
    .finally(() => { next() })
})

// *******************************
// * Process:     => create new application
// * Input:
//  - application => new application content
// * Output:
//  - 200         => created application
//  - (4|5)xx     => error
// *******************************
app.post("/applications", (req, res, next) => {
  console.debug("Received request to POST new application")

  let application = req.body.application

  applicationsTable.insert(application, {"return_changes": true}).run(req._rdbConn)
    .then(updateResult => {
      console.debug("Application inserted: %s", JSON.stringify(updateResult))
      let application = updateResult.changes[0]["new_val"]
      delete application["policies"]
      application["links"] = {
        "policies": "/applications/" + application["id"] + "/policies"
      }
      return res.status(200).json({"application": application})
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

function errorHandler (err, req, res, next) {
  console.error("Error occurred in /applications: %s", JSON.stringify(err))
  next(err)
}

app.use(errorHandler)
