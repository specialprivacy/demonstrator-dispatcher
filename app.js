"use strict"
const app = require("express")()
const session = require("express-session")
const crypto = require("crypto")
const bodyParser = require("body-parser")

const childLogger = require("./lib/middleware/child-logger")
const errorHandler = require("./lib/middleware/error-handler")
const notFoundHandler = require("./lib/middleware/not-found-handler")
const applications = require("./lib/applications")
const dataSubjects = require("./lib/data-subjects")
const policies = require("./lib/policies")
const rethink = require("./utils/rethinkdb_config")

app.disable("x-powered-by")

app.use(childLogger)
app.use(session({
  secret: process.env["SESSION_SECRET"] || crypto.randomBytes(20).toString("hex")
}))
app.use(bodyParser.json())
// TODO: Change CORS origin once domain have been decided
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*") // for development purposes, can be later changed accordingly
  res.header("Access-Control-Allow-Credentials", true)
  res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
  res.header("Access-Control-Expose-Headers", "location")
  next()
})

app.use("/applications", rethink.createConnection, applications)
app.use("/policies", rethink.createConnection, policies)
app.use("/users", rethink.createConnection, dataSubjects)

app.use(notFoundHandler)
app.use(errorHandler)

module.exports = app
