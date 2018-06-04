const rethinkDB = require("rethinkdb")

const dbHost = process.env["RETHINKDB_HOST"] || "localhost"
const dbPort = process.env["RETHINKDB_PORT"] || 28015
const dbTimeout = process.env["RETHINKDB_TIMEOUT"] || 30

const r = rethinkDB
const dbName = "changeLogsProducer"
const db = r.db(dbName)
const applicationsTableName = "applications"
const applicationsTable = db.table(applicationsTableName)
const dataControllerPoliciesTableName = "dataControllerPolicies"
const dataControllerPoliciesTable = db.table(dataControllerPoliciesTableName)
const dataSubjectsTableName = "dataSubjects"
const dataSubjectsTable = db.table(dataSubjectsTableName)
const dbTables = [applicationsTableName, dataControllerPoliciesTableName, dataSubjectsTableName]

function createConnection (req, res, next) {
  return r.connect({"host": dbHost, "port": dbPort, "timeout": dbTimeout}).then(function (conn) {
    req._rdbConn = conn
    console.debug("Creating connection to database for request %s...", req.url)
    next()
  }).catch(error => { next(error) })
}

function closeConnection (req, res, next) {
  console.debug("Closing connection to database for request %s...", req.url)
  req._rdbConn.close()
  next()
}

module.exports = {
  createConnection,
  closeConnection,
  dbHost,
  dbPort,
  dbTimeout,
  r,
  dbName,
  db,
  applicationsTableName,
  applicationsTable,
  dataControllerPoliciesTableName,
  dataControllerPoliciesTable,
  dataSubjectsTableName,
  dataSubjectsTable,
  dbTables
}
