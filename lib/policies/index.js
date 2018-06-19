let router = require("express").Router()

const rethink = require("../../utils/rethinkdb_config")
const {
  r,
  applicationsTable,
  dataControllerPoliciesTable
} = rethink

// *******************************
// * Process:         => returns a policy
// * Input:
//  - id              => policy id
// * Output:
//  - 200             => array of policies
//  - (4|5)xx         => error
// *******************************
router.get("/:id", (req, res, next) => {
  let policyId = req.params.id
  req.log.debug({policyId}, "Received request to get policy")

  if (!policyId) {
    req.log.info("No id specified, can't GET.")
    return next({status: 400, message: "No policy ID specified"})
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

// *******************************
// * Process:         => update a policy
// * Input:
//  - id              => policy id
//  - policy          => new content for policy
// * Output:
//  - 200             => updated policy
//  - (4|5)xx         => error
// *******************************
router.put("/:id", (req, res, next) => {
  let policyId = req.params.id
  if (!policyId) {
    req.log.info("No id specified, can't update.")
    return next({status: 400, message: "No ID specified, can't update"})
  }
  req.log.debug({policyId}, "Received request to update policy")

  let policy = req.body.policy
  policy["id"] = policyId

  dataControllerPoliciesTable.get(policyId).update(policy).run(req._rdbConn)
    .then(updateResult => {
      if (updateResult["skipped"] > 0 &&
        updateResult["replaced"] === 0 &&
        updateResult["unchanged"] === 0) {
        return next({"status": 404, "message": "Policy does not exist / You are not authorized"})
      }
      req.log.debug({policyId, updateResult}, "Policy updated")
      return res.status(200).json(policy)
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

// *******************************
// * Process:         => delete a policy
// * Input:
//  - id              => policy id
// * Output:
//  - 204             => success, empty response
//  - (4|5)xx         => error
// *******************************
router.delete("/:id", (req, res, next) => {
  let policyId = req.params.id
  if (!policyId) {
    req.log.info("No id specified, can't delete.")
    return next({status: 400, message: "No ID specified, can't delete"})
  }
  req.log.debug({policyId}, "Received request to delete policy")

  dataControllerPoliciesTable.get(policyId).delete().run(req._rdbConn)
    .then(updateResult => {
      if (updateResult["skipped"] > 0 &&
        updateResult["replaced"] === 0 &&
        updateResult["unchanged"] === 0) {
        return next({"status": 404, "message": "Policy does not exist / You are not authorized"})
      }
      req.log.debug({policyId, updateResult}, "Policy deleted")
      return res.status(204).send()
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

// *******************************
// * Process:         => get all policies
// * Input:
//  - application-id  => the ID of the application making the call, obtained through a header
// * Output:
//  - 200             => array of policies, filtered by application-id if existing
//  - (4|5)xx         => error
// *******************************
router.get("/policies", (req, res, next) => {
  req.log.debug("Received request to get policies")
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

// *******************************
// * Process:         => create new policy
// * Input:
//  - policy          => content of new policy
// * Output:
//  - 200             => created policy
//  - (4|5)xx         => error
// *******************************
router.post("/policies", (req, res, next) => {
  req.log.debug("Received request to POST new policy")

  let policy = req.body.policy

  dataControllerPoliciesTable.insert(policy, {"return_changes": true}).run(req._rdbConn)
    .then(updateResult => {
      req.log.debug({updateResult}, "Policy inserted")
      return res.status(200).json({"policy": updateResult.changes[0]["new_val"]})
    })
    .catch(error => { next(error) })
    .finally(() => {
      next()
    })
})

module.exports = router
