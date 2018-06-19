const crypto = require("crypto")
const querystring = require("querystring")
const request = require("request-promise")
if (process.env.LOGGING_LEVEL === "trace") {
  require("request-debug")(request)
}

const rethink = require("../../utils/rethinkdb_config")
const {
  dbHost,
  dbPort,
  dbTimeout,
  r,
  dataSubjectsTable
} = rethink

const redirectUri = process.env["SERVER_AUTH_CALLBACK_ENDPOINT"] || "/callback"
const clientId = process.env["AUTH_CLIENT_ID"] || "special-platform"
const clientSecret = process.env["AUTH_CLIENT_SECRET"] || "special-platform-secret"
const baseAuthURL = process.env["AUTH_LOGIN_ENDPOINT"] || "http://localhost:8080/auth/realms/master/protocol/openid-connect/auth"

function oauthCallback (req, res, next) {
  const state = JSON.parse(Buffer.from(req.query.state, "base64"))
  // To mitigate CSRF attacks, we ensure that the extra parameter we created when redirecting is the same as the one we received
  if (state.nonce !== req.session.nonce) return next({"status": "403", "message": "Unknown state"})

  const authCode = req.query.code

  // Let's exchange the authorization code for an access token
  var clientServerOptions = {
    "headers": {
      // Using "auth" does not work with POST on request library for now, see: https://github.com/request/request/issues/2777
      "Authorization": "Basic " + Buffer.from(clientId + ":" + clientSecret).toString("base64")
    },
    "uri": process.env["AUTH_TOKEN_ENDPOINT"] || "http://localhost:8080/auth/realms/master/protocol/openid-connect/token",
    // // See above, using "auth" does not work with request library for now
    // "auth": {
    //  "user": "special-platform",
    //  "pass": "760b7a62-058d-4095-b090-ccf07d1d1b8f",
    //  "sendImmediately": false
    // },
    "form": {
      "grant_type": "authorization_code",
      "redirect_uri": redirectUri,
      "code": authCode
    },
    "json": true,
    "method": "POST",
    "content-type": "application/x-www-form-urlencoded"
  }

  let conn = null
  // Obtain the DB connection we will be using
  return r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout})
    .then(dbconn => {
      conn = dbconn
      return conn
    })
    .then(() => {
      return request(clientServerOptions)
    })
    .then(response => {
      let accessToken = response["access_token"]
      // We've obtained our access token, we now need to use it to get the user information
      var clientServerOptions = {
        "uri": process.env["AUTH_USERINFO_ENDPOINT"] || "http://localhost:8080/auth/realms/master/protocol/openid-connect/userinfo",
        "form": {
          "access_token": accessToken
        },
        "json": true,
        "method": "POST",
        "content-type": "application/x-www-form-urlencoded"
      }

      return request(clientServerOptions)
    })
    .then(response => {
      response["id"] = response["sub"]
      delete response["sub"]
      return response
    })
    .then(user => {
      // We update the users table with the logged in user, if he didn't exist yet, we give him an empty set of policies, if he existed, we just updated his other information
      return dataSubjectsTable.insert(Object.assign({}, user, {"policies": []}), {
        "conflict": function (id, oldDoc, newDoc) { return newDoc.merge({"policies": oldDoc("policies")}) }
      }).run(conn).then(updateResult => {
        console.debug("User [%s] updated: %s", user["id"], JSON.stringify(updateResult))
        return user
      })
    })
    .then(user => {
      req.session.user = user
      req.session.authenticated = true
      res.redirect(state.referer)
      return user
    })
    .catch(error => {
      console.error("Error when authorizing client: %s", error)
      console.error(error)
      return next({"code": 403, "message": "Not authorized"})
    })
    .finally(() => {
      conn.close()
    })
}

function authenticate (req, res, next) {
  if (req.session.authenticated) {
    // TODO: Check authorization status (expired or not)
    return next()
  }

  if (!req.session.nonce) {
    req.session.nonce = crypto.randomBytes(20).toString("hex")
  }
  const state = {
    "nonce": req.session.nonce,
    // The original request path, we will redirect to it after the authentication
    "referer": req.header("Referer")
  }
  const options = {
    "scope": "all openid",
    "response_type": "code",
    "client_id": clientId,
    "redirect_uri": redirectUri,
    "state": Buffer.from(JSON.stringify(state)).toString("base64")
  }
  let authRedirect = `${baseAuthURL}?${querystring.stringify(options)}`
  return res.status(401).location(authRedirect).send()
}

module.exports = {
  oauthCallback,
  authenticate
}
