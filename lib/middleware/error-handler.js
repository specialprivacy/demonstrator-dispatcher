"use strict"
const APIError = require("../../utils/api-error")
const { INTERNAL_SERVER_ERROR } = require("http-status-codes")

function errorHandler (error, req, res, next) {
  if (!(error instanceof APIError)) error = new APIError(error)

  let sc = error.statusCode || INTERNAL_SERVER_ERROR
  if (sc >= INTERNAL_SERVER_ERROR) req.log.error({ err: error })
  else req.log.debug({ err: error })
  res.status(sc)

  if (req.accepts(["html", "json"]) === "json") {
    res.json(error)
  } else {
    // TODO: HTML requested here, we should render an actual web page
    res.json(error)
  }
}

module.exports = errorHandler
