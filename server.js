const app = require("express")()
app.disable("x-powered-by")
const session = require("express-session")
const crypto = require("crypto")
const bodyParser = require("body-parser")
const http = require("http")
http.globalAgent.maxSockets = process.env["HTTP_MAX_SOCKETS"] || 10
const oauth = require("./lib/middleware/oauth")
const childLogger = require("./lib/middleware/child-logger")
const applications = require("./lib/applications")
const dataSubjects = require("./lib/data-subjects")
const policies = require("./lib/policies")
const rethink = require("./utils/rethinkdb_config")
const dataGenerator = require("./utils/data_generator")
const watchers = require("./utils/watchers")

let server = null
async function init () {
  await dataGenerator.generate()
  const { producer } = watchers
  watchers.startWatching()
  producer.on("ready", async () => {
    server = app.listen(process.env["SERVER_PORT"] || 80, () => {
      let { address, port } = server.address()
      address = address === "::" ? "0.0.0.0" : address
      console.debug("App listening at http://%s:%s", address, port)
    })
  })
}
init()

app.use(childLogger)
app.use(session({
  secret: process.env["SESSION_SECRET"] || crypto.randomBytes(20).toString("hex")
}))
app.use(oauth)
app.use(bodyParser.json())
app.use(rethink.createConnection)
app.use(applications)
app.use(dataSubjects)
app.use(policies)
app.use(rethink.closeConnection)
app.use(errorHandler)

// Handle SIGTERM gracefully
process.on("SIGTERM", gracefulShutdown)
process.on("SIGINT", gracefulShutdown)
process.on("SIGHUP", gracefulShutdown)
function gracefulShutdown () {
  // Serve existing requests, but refuse new ones
  console.warn("Received signal to terminate: wrapping up existing requests")
  server.close(() => {
    // Exit once all existing requests have been served
    console.warn("Received signal to terminate: done serving existing requests. Exiting")
    process.exit(0)
  })
}

function errorHandler (error, req, res, next) {
  console.error("Error occurred in /consent-manager: %s", JSON.stringify(error))
  console.error(error)
  if (req._rdbConn) req._rdbConn.close()
  res.status(error.status || 500).json({"error": error.message || error})
  next()
}
