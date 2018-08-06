"use strict"
let log = require("../../utils/log")
let uuid = require("uuid")

function addChildLogger (req, res, next) {
  req.requestId = req.headers["x-request-id"] ? req.headers["x-request-id"] : uuid.v4()
  req.log = log.child({requestId: req.requestId})
  req.log.info({req}, "API Request")
  res.set("x-request-id", req.requestId)
  res.on("close", () => req.log.debug({res}, "API Response"))
  res.on("finish", () => req.log.debug({res}, "API Response"))
  next()
}

module.exports = addChildLogger
