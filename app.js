"use strict"
const app = require("express")()
const session = require("express-session")
const crypto = require("crypto")
const bodyParser = require("body-parser")
const {oauthCallback, authenticate} = require("./lib/middleware/oauth")
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
app.use("/callback", oauthCallback)
app.use(authenticate)
app.use(bodyParser.json())
app.use(rethink.createConnection)
app.use("/applications", applications)
app.use("/users", dataSubjects)
app.use("/policies", policies)
app.use(notFoundHandler)
app.use(errorHandler)

module.exports = app
