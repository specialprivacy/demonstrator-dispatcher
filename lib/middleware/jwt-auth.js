"use strict"
/**
 * This implements a middleware which just checks the Authorization header
 * for a bearer token and validates this at the OIDC /userinfo endpoint
 * If authentication fails we just call next('route') to let the app default
 * behaviour handle it
 *
 * It's a quick hack to support service accounts
 */
const request = require("superagent")
const { UNAUTHORIZED } = require("http-status-codes")
const endpoint = process.env.AUTH_USERINFO_ENDPOINT || "http://localhost:8080/auth/realms/master/protocol/openid-connect/userinfo"

function jwtAuth (req, res, next) {
  // TODO: to be spec compliant a token can also be passed into the body or
  // as a query parameter, but we're only supporting the Authorization header
  // at the moment (it is a hack remember)
  // see: https://github.com/jaredhanson/passport-http-bearer/blob/master/lib/strategy.js
  if (!req.headers || !req.headers.authorization) return next("route")
  const parts = req.headers.authorization.split(" ")
  if (parts.length !== 2) return next("route")
  const [scheme, token] = parts
  if (!/^Bearer$/i.test(scheme)) return next("route")
  req.log.debug({ token }, "Parsed access_token")

  // At this stage we have a token, let's verify it
  request
    .get(endpoint)
    .set("Authorization", `Bearer ${token}`)
    .set("Accept", "application/json")
    .end((err) => {
      if (err) {
        req.log.warn({ error: err }, "Failed to authorize access token")
        return res.status(err.status || UNAUTHORIZED).send()
      }
      return next()
    })
}

module.exports = jwtAuth
