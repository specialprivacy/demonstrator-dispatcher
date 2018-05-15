"use strict"
let uuid = require("uuid")
let bunyan = require("bunyan")
let fs = require("fs")
let os = require("os")
let path = require("path")
let pjson = require(path.resolve(process.cwd(), "./package.json"))

const levels = ["debug", "info", "warn", "error", "fatal"]

const hostname = fs.existsSync("/etc/host_hostname")
  ? fs.readFileSync("/etc/host_hostname")
  : os.hostname()

let log = bunyan.createLogger({
  src: process.env.NODE_ENV !== "production",
  name: pjson.name,
  serverVersion: pjson.version,
  hostname,
  level: levels.includes(process.env.LOGGING_LEVEL) ? process.env.LOGGING_LEVEL : "info",
  serializers: bunyan.stdSerializers
})

module.exports = log
function addChildLogger (req, res, next) {
  req.requestId = req.headers["x-request-id"] ? req.headers["x-request-id"] : uuid.v4()
  req.log = log.child({requestId: req.requestId})
  req.log.info({req}, "API Request")
  res.set("x-request-id", req.requestId)
  res.on("finish", () => req.log.debug({res}, "API Response"))
  next()
}

module.exports = addChildLogger
