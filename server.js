const app = require("./app")
const http = require("http")
const log = require("./utils/log")
const dataGenerator = require("./utils/data_generator")
const watchers = require("./utils/watchers")

let server = null
http.globalAgent.maxSockets = process.env["HTTP_MAX_SOCKETS"] || 10

async function init () {
  try {
    await dataGenerator.generate()
  } catch (err) {
    log.fatal({err}, "Failed to insert data")
    process.exit(1)
  }
  const { producer } = watchers
  watchers.startWatching()
  producer.on("ready", async () => {
    server = app.listen(process.env["SERVER_PORT"] || 80, () => {
      let { address, port } = server.address()
      address = address === "::" ? "0.0.0.0" : address
      log.info(`App listening at http://${address}:${port}`)
    })
  })
}
init()

// Handle SIGTERM gracefully
process.on("SIGTERM", gracefulShutdown)
process.on("SIGINT", gracefulShutdown)
process.on("SIGHUP", gracefulShutdown)
function gracefulShutdown () {
  // Serve existing requests, but refuse new ones
  if (server == null) {
    log.warn("Received signal to terminate before initialization was complete. Exiting")
    process.exit(0)
  }
  log.warn("Received signal to terminate: wrapping up existing requests")
  server.close(() => {
    // Exit once all existing requests have been served
    log.warn("Received signal to terminate: done serving existing requests. Exiting")
    process.exit(0)
  })
}
