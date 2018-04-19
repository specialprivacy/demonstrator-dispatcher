const rethinkDB = require("rethinkdb")

const dbHost = process.env["RETHINKDB_HOST"] || "localhost"
const dbPort = process.env["RETHINKDB_PORT"] || 28015

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

module.exports = {
  dbHost,
  dbPort,
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
