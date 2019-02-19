const log = require("./log")
const rethink = require("./rethinkdb_config")
const {
  dbHost,
  dbPort,
  dbTimeout,
  r,
  dbName,
  db,
  applicationsTable,
  dataControllerPoliciesTable,
  dbTables
} = rethink

module.exports = {
  generate: generateData
}

const MAX_RETRIES = 10

async function generateData (retryCount = 0) {
  let conn
  try {
    conn = await r.connect({ "host": dbHost, "port": dbPort, "timeout": dbTimeout })
  } catch (err) {
    if (retryCount >= MAX_RETRIES) {
      log.error({ err }, "Failed to connect to rethinkdb and out of retries. Exiting.")
      process.exit(1)
    }
    const timeout = (Math.pow(2, retryCount + 1) + Math.random()) * 1000
    log.warn({ err, timeout, retryCount }, `Failed to connect to rethinkdb retrying in ${timeout} ms`)
    return setTimeout(generateData, timeout, retryCount + 1)
  }
  log.debug("Creating database...")
  try {
    await r.dbCreate(dbName).run(conn, function (error, result) {
      if (!error) { log.debug({ result }, "Database created") }
    })
  } catch (error) { log.debug("Database already exists.") }

  log.debug("Creating tables...")
  try {
    await r.expr(dbTables).forEach(db.tableCreate(r.row)).run(conn, function (error, result) {
      if (!error) { log.debug({ result }, "Tables created") }
    })
  } catch (error) { log.debug("Tables already exist.") }

  let promises = []

  log.debug("Inserting base data")

  promises.push(dataControllerPoliciesTable.insert([
    {
      "id": "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymized",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#EU",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Collect",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Account",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsDelivery",
      "explanation": "I consent to the collection of my anonymized data in Europe for the purpose of accounting."
    },
    {
      "id": "54ff9c00-1b47-4389-8390-870b2ee9a03c",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Derived",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#EULike",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Copy",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsSame",
      "explanation": "I consent to the copying of my derived data in Europe-like countries for the purpose of administration."
    },
    {
      "id": "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Computer",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#ThirdParty",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Move",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Browsing",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsPublic",
      "explanation": "I consent to the moving of my computer data on third-party servers for the purpose of browsing."
    },
    {
      "id": "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Activity",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#ControllerServers",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Aggregate",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Account",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsDelivery",
      "explanation": "I consent to the aggregation of my activity data on your servers for the purpose of accounting."
    },
    {
      "id": "be155566-7b56-4265-92fe-cb474aa0ed42",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymized",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#EU",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Analyze",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsOurs",
      "explanation": "I consent to the analysis of my anonymized data in Europe for the purpose of administration."
    },
    {
      "id": "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#AudiovisualActivity",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#EULike",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Anonymize",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsPublic",
      "explanation": "I consent to the anonymization of my activity data in Europe-like countries for the purpose of administration."
    },
    {
      "id": "2f274ae6-6c2e-4350-9109-6c15e50ba670",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Computer",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#ThirdCountries",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Copy",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Arts",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsSame",
      "explanation": "I consent to the copying of my computer data in third countries for the purpose of artistic usage."
    },
    {
      "id": "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Content",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#OurServers",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Derive",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#AuxPurpose",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsUnrelated",
      "explanation": "I consent to the derivation of my content data on your servers for auxiliary purposes."
    },
    {
      "id": "86371d81-30ff-49c4-897f-5e6dbc721e85",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Demographic",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#ProcessorServers",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Move",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Browsing",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsDelivery",
      "explanation": "I consent to the moving of my demographic data on processor servers for the purpose of browsing."
    },
    {
      "id": "4d675233-279f-4b5e-8695-b0b66be4f0f9",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Derived",
      "storageCollection": "http://www.specialprivacy.eu/vocabs/locations#ThirdParty",
      "processingCollection": "http://www.specialprivacy.eu/vocabs/processing#Aggregate",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/purposes#Charity",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/recipientsOtherRecipient",
      "explanation": "I consent to the aggregation of my derived data on third-party servers for the purpose of charity."
    }
  ], { conflict: "replace" }).run(conn))

  promises.push(applicationsTable.insert([
    {
      "id": "d5aca7a6-ed5f-411c-b927-6f19c36b93c3",
      "name": "Application A",
      "policies":
        [
          "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
          "54ff9c00-1b47-4389-8390-870b2ee9a03c",
          "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
          "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
          "be155566-7b56-4265-92fe-cb474aa0ed42",
          "8a7cf1f6-4c34-497f-8a65-4c985eb47a35"
        ]
    },
    {
      "id": "c52dcc17-89f7-4a56-8836-bad27fd15bb3",
      "name": "Application B",
      "policies":
        [
          "be155566-7b56-4265-92fe-cb474aa0ed42",
          "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
          "2f274ae6-6c2e-4350-9109-6c15e50ba670",
          "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
          "86371d81-30ff-49c4-897f-5e6dbc721e85",
          "4d675233-279f-4b5e-8695-b0b66be4f0f9"
        ]
    }
  ], { conflict: "replace" }).run(conn))

  return Promise.all(promises).then(() => {
    log.debug("Data inserted")
    return conn.close()
  })
}
