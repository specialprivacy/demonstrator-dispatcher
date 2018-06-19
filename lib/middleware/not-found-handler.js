"use strict"
let APIError = require("../../utils/api-error")
const {NOT_FOUND} = require("http-status-codes")

function notFoundHandler (req, res, next) {
  next(new APIError({
    statusCode: NOT_FOUND,
    title: "Resource not found",
    detail: "No resource exists at this path. Check your URI"
  }))
}

module.exports = notFoundHandler
