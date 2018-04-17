// TODO: Add tests and documentation

const app = require("express")();
const rethinkDB = require("rethinkdb");
const bodyParser = require("body-parser");
const Kafka = require("node-rdkafka");

const producer = new Kafka.Producer({
  "metadata.broker.list": process.env["KAFKA_BROKER_LIST"] || "localhost:9092",
  "api.version.request":false,
  "dr_cb": true
});

// TODO: Take parameter / ENV
const dbHost = process.env["RETHINKDB_HOST"] || "localhost";
const dbPort = process.env["RETHINKDB_PORT"] || 28015;

const r = rethinkDB;
const dbName = "changeLogsProducer";
const db = r.db(dbName);
const applicationsTableName = "applications";
const applicationsTable = db.table(applicationsTableName);
const dataControllerPoliciesTableName = "dataControllerPolicies";
const dataControllerPoliciesTable = db.table(dataControllerPoliciesTableName);
const dataSubjectsTableName = "dataSubjects";
const dataSubjectsTable = db.table(dataSubjectsTableName);

const dbTables = [applicationsTableName, dataControllerPoliciesTableName, dataSubjectsTableName];

app.use(bodyParser.json());

app.use(createConnection);

// TODO: check if still needed
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, Content-Length, X-Requested-With, APP_KEY");

  //intercepts OPTIONS method
  if ("OPTIONS" === req.method) {
    //respond with 200
    res.status(200).send();
  }
  next();
});

// TODO: ADMIN
// Get policies for an application
app.get("/applications/:id/policies", (req, res, next) => {
  let appId = req.params.id;
  console.debug("Received request to get policies for application %s", appId);

  if(!appId){
    console.error("No appId specified, can't GET.");
    res.status(400).send();
    next();
  }

  dataControllerPoliciesTable.getAll(
    r.args(applicationsTable.get(appId)("policies").default([]).coerceTo("array"))
  ).run(req._rdbConn)
    .then(cursor => {
    return cursor.toArray();
  }).then(policies => {
      return res.status(200).json({policies});
    })
    .finally(() => {next();})
    .error(handleError(res));
});

// TODO: ADMIN
// Get application based on ID
app.get("/applications/:id", (req, res, next) => {
  let appId = req.params.id;
  console.debug("Received request to get application %s", appId);

  if(!appId){
    console.error("No appId specified, can't GET.");
    res.status(400).send();
    next();
  }

  applicationsTable.get(appId).without({"policies": true}).run(req._rdbConn)
    .then(application => {
      application["links"] = {
        "policies": "/applications/"+application["id"]+"/policies"
      };
      return res.status(200).json({"applications": [application]});
    })
    .finally(() => {next();})
    .error(handleError(res));
});

app.put("/applications/:id", (req, res, next) => {
  let appId = req.params.id;
  if (!appId) {
    console.error("No id specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to update application: %s", appId);

  let application = req.body.application;
  application["id"] = appId;

  applicationsTable.get(appId).update(application).run(req._rdbConn)
    .then(updateResult=> {
      console.debug("Application [%s] updated: %s", appId, JSON.stringify(updateResult));
      delete application["policies"];
      application["links"] = {
        "policies": "/applications/"+appId+"/policies"
      };
      return res.status(200).json(application);
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

app.delete("/applications/:id", (req, res, next) => {
  let appId = req.params.id;
  if (!appId) {
    console.error("No id specified, can't delete.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to delete application: %s", appId);

  applicationsTable.get(appId).delete().run(req._rdbConn)
    .then(updateResult=> {
      console.debug("Application [%s] deleted: %s", appId, JSON.stringify(updateResult));
      return res.status(204).send();
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

// TODO: ADMIN
// Get all applications
app.get("/applications", (req, res, next) => {
  console.debug("Received request to get applications");

  applicationsTable.without({"policies": true}).run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray();
    })
    .then(applications => {
      applications = applications.map(application => {
        application["links"] = {
          "policies": "/applications/"+application["id"]+"/policies"
        };
        return application;
      });
      res.status(200).send({applications})
    })
    .finally(() => {next();})
    .error(handleError(res));
});

app.post("/applications", (req, res, next) => {
  console.debug("Received request to POST new application");

  let application = req.body.application;

  applicationsTable.insert(application, {"return_changes": true}).run(req._rdbConn)
    .then(updateResult=> {
      console.debug("Application inserted: %s", JSON.stringify(updateResult));
      let application = updateResult.changes[0]["new_val"];
      delete application["policies"];
      application["links"] = {
        "policies": "/applications/"+application["id"]+"/policies"
      };
      return res.status(200).json({"application": application});
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

app.get("/policies/:id", (req, res, next) => {
  let policyId = req.params.id;
  console.debug("Received request to get policy: %s", policyId);

  if(!policyId){
    console.error("No id specified, can't GET.");
    res.status(400).send();
    next();
  }

  dataControllerPoliciesTable.get(policyId).run(req._rdbConn)
    .then(policy => {
      return res.status(200).json({"policies": [policy]});
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

app.put("/policies/:id", (req, res, next) => {
  let policyId = req.params.id;
  if (!policyId) {
    console.error("No id specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to update policy: %s", policyId);

  let policy = req.body.policy;
  policy["id"] = policyId;

  dataControllerPoliciesTable.get(policyId).update(policy).run(req._rdbConn)
    .then(updateResult=> {
      console.debug("Policy [%s] updated: %s", policyId, JSON.stringify(updateResult));
      return res.status(200).json(policy);
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

app.delete("/policies/:id", (req, res, next) => {
  let policyId = req.params.id;
  if (!policyId) {
    console.error("No id specified, can't delete.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to delete policy: %s", policyId);

  dataControllerPoliciesTable.get(policyId).delete().run(req._rdbConn)
    .then(updateResult=> {
      console.debug("Policy [%s] deleted: %s", policyId, JSON.stringify(updateResult));
      return res.status(204).send();
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

// Get policies for an application
app.get("/policies", (req, res, next) => {
  console.debug("Received request to get policies");
  let appId = req.header("APP_KEY");

  let query = dataControllerPoliciesTable;

  // If an APP_KEY is specified, we get the policies for that application only.
  if(appId){
    query = query.filter(policy => {return r.expr(applicationsTable.get(appId)("policies")).contains(policy("id"))})
  }
  else {
    // TODO: ADMIN, otherwise error
  }
  query
    .map(row => {return row.merge({"type": "policy"})})
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray()
    })
    .then(policies=> {
      return res.status(200).json({"policies": policies});
    })
    .finally(() => {next();})
    .error(handleError(res));
});

app.post("/policies", (req, res, next) => {
  console.debug("Received request to POST new policy");

  let policy = req.body.policy;

  dataControllerPoliciesTable.insert(policy, {"return_changes": true}).run(req._rdbConn)
    .then(updateResult=> {
      console.debug("Policy inserted: %s", JSON.stringify(updateResult));
      return res.status(200).json({"policy": updateResult.changes[0]["new_val"]});
    })
    .finally(() => {
      next();
    })
    .error(handleError(res));
});

// Get policies linked to specified data subject
app.get("/users/:id/policies", (req, res, next) => {
  let dataSubjectId = req.params.id;
  if(!dataSubjectId){
    console.error("No dataSubjectId specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to get policies for data subject: %s", dataSubjectId);

  let appId = req.header("APP_KEY");

  let query = dataControllerPoliciesTable.getAll(r.args(dataSubjectsTable.get(dataSubjectId)("policies").coerceTo("array")));

  // If an APP_KEY is specified, we get the policies for that application only.
  if(appId){
    query = query.filter(policy => {return r.expr(applicationsTable.get(appId)("policies")).contains(policy("id"))})
  }
  query
    .map(row => {return row.merge({"dataSubjectId": dataSubjectId, "type": "policy"})})
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray();
    })
    .then(policies=> {
      return res.status(200).json({"policies": policies});
    })
    .finally(() => {next();})
    .error(handleError(res));
});

app.get("/users/:id", (req, res, next) => {

  let dataSubjectId = req.params.id;
  if(!dataSubjectId){
    console.error("No dataSubjectId specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to get data subject: %s", dataSubjectId);

  dataSubjectsTable.get(dataSubjectId).without({"policies": true}).run(req._rdbConn)
    .then(dataSubject => {
    dataSubject["links"] = {
      "policies": "/users/"+dataSubject["id"]+"/policies"
    };
    res.status(200).json({"users": [dataSubject]});
  })
    .error(handleError(res))
    .finally(() => {
      next();
    })
});

app.put("/users/:id", (req, res, next) => {
  let dataSubjectId = req.params.id;
  if(!dataSubjectId){
    console.error("No dataSubjectId specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to update data subject: %s", dataSubjectId);

  let appId = req.header("APP_KEY");
  let dataSubject = req.body.user;

  let query = applicationsTable;
  let dbUser = null;

  // If an APP_KEY is specified, we get the policies for that application only.
  if(appId){
    query = query.get(appId);
  }
  query
    ("policies")
    .coerceTo("array")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray();
    })
    .then(appPolicies => {
      return dataSubjectsTable.get(dataSubjectId).run(req._rdbConn).then(dbUser => {
        return {
          appPolicies, dbUser
        };
      })
    })
    .then(hash => {
      dbUser = hash["dbUser"];

      let dbPolicies = dbUser["policies"];
      let appPolicies = hash["appPolicies"];
      let newPolicies = dataSubject["policies"];


      for(let appPolicy of appPolicies){
        if(dbPolicies.includes(appPolicy)){
          dbPolicies.splice(dbPolicies.indexOf(appPolicy));
        }
      }
      for(let newPolicy of newPolicies){
        if(appPolicies.includes(newPolicy)){
          dbPolicies.push(newPolicy);
        }
      }

      return dataSubjectsTable.get(dataSubjectId).update(dbUser).run(req._rdbConn).then(updateResult => {
        console.debug("User [%s] updated: %s", dataSubjectId, JSON.stringify(updateResult));
        dbUser["policies"] = newPolicies;
        return dbUser;
      });
    })
    .then(newUser => {
      res.status(200).send({"user": newUser});
    })
    .error(handleError(res))
    .finally(() => {
      next();
    })
});

app.use(closeConnection);

const server = app.listen(8081, async () => {
  const { address } = server.address();
  const { port } = server.address();

  await generateData();
  producer.connect();
  // Any errors we encounter, including connection errors
  producer.on("event.error", function(err) {
    console.error("Error from kafka producer");
    console.error(err);
  });

  producer.on("ready", async function() {
    await watchDataSubjects();
    await watchPolicies();
  });
  console.debug("App listening at http://%s:%s", address, port);
});

function handleError(res) {
  return function(error) {
    res.status(500).send({error: error.message});
  }
}


function createConnection(req, res, next) {
  return r.connect({"host": dbHost, "port": dbPort}).then(function(conn) {
    req._rdbConn = conn;
    console.debug("Creating connection to database for request %s...", req.url);
    next();
  }).error(handleError(res));
}

function closeConnection(req, res, next) {
  console.debug("Closing connection to database for request %s...", req.url);
  req._rdbConn.close();
}

// Handle SIGTERM gracefully
process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);
process.on("SIGHUP", gracefulShutdown);
function gracefulShutdown () {
  // Serve existing requests, but refuse new ones
  console.warn("Received signal to terminate: wrapping up existing requests");
  server.close(() => {
    // Exit once all existing requests have been served
    console.warn("Received signal to terminate: done serving existing requests. Exiting");
    process.exit(0)
  })
}

watchPolicies = async function(){
  console.debug("Starting to watch policies changes...");
  let conn = await r.connect({"host": dbHost, "port": dbPort});
  let cursor = await dataControllerPoliciesTable.changes({"include_types": true}).run(conn);
  return cursor.each(async (err, row) => {
    if(row["type"] === "remove"){
      // TODO: Remove that ID from applications and data subjects
    }
  },() =>{
    return conn.close();
  })
};

watchDataSubjects = async function(){
  console.debug("Starting to watch data subject changes...");
  let conn = await r.connect({"host": dbHost, "port": dbPort});
  let cursor = await dataSubjectsTable.changes({"includeInitial":true}).run(conn);
  return cursor.each(async (err, row) => {
    if(err){console.error("Error occurred on data subject modification: %s", err);}

    let policyIds = [];
    if(row["old_val"]) {
      policyIds = policyIds.concat(row["old_val"]["policies"]);
    }
    if(row["new_val"]) {
      policyIds = policyIds.concat(row["new_val"]["policies"]);
    }

    let policies = {};
    try {
      let cursor = await dataControllerPoliciesTable.getAll(r.args(r.expr(policyIds).distinct())).run(conn);
      let policiesArr = await cursor.toArray();
      policiesArr.forEach(policy => {
        return policies[policy["id"]] = policy;
      });
    }
    catch(error){
      console.error("Couldn't fetch policies: %s", error);
    }

    let withdrawn = [], added = [], newPolicies = null;
    try {
      let dataSubjectId;
      if(!row["new_val"]){
        dataSubjectId = row["old_val"]["id"];
        // Remove policies from topic
        console.debug("User [%s] removed. ", dataSubjectId);

        withdrawn = row["old_val"]["policies"];

        newPolicies = null;
      }
      else{
        dataSubjectId = row["new_val"]["id"];

        if(row["old_val"]){
          // Check and propagate withdrawals of consent to history kafka topic
          withdrawn = row["old_val"]["policies"].filter(item => {return !row["new_val"]["policies"].includes(item)});
          added = row["new_val"]["policies"].filter(item => {return !row["old_val"]["policies"].includes(item)});
        }
        else {
          // New data subject
          added = row["new_val"]["policies"];
        }

        // Create new list of policies for data subject
        console.debug("Data subject policies modified, generating new set of policies.  --%s", JSON.stringify(policies));

        newPolicies = {
          "simplePolicies": row["new_val"]["policies"].map(policy => {return policies[policy];})
        };
        console.log("newPolicies: %s", JSON.stringify(newPolicies));
      }

      for(let consent of withdrawn){
        console.log("Removing data subject [%s] consent for policy [%s].", dataSubjectId, consent);
        let message = policies[consent];
        message["given"] = false;
        message["data-subject"] = dataSubjectId;
        console.log("WITHDRAWN : %s", JSON.stringify(message));
        producer.produce(
          "changeLogs", // Topic
          null, // Partition, null uses default
          new Buffer(JSON.stringify(message)), // Message
          null, // We do not need a key
          Date.now()
        );
      }

      for(let consent of added){
        console.log("Adding data subject [%s] consent for policy [%s].", dataSubjectId, consent);
        let message = policies[consent];
        message["given"] = true;
        message["data-subject"] = dataSubjectId;
        console.log("ADDED : %s", JSON.stringify(message));
        producer.produce(
          "changeLogs", // Topic
          null, // Partition, null uses default
          new Buffer(JSON.stringify(message)), // Message
          null, // We do not need a key
          Date.now()
        );
      }


      console.log("NEW POLICIES : %s", newPolicies ? JSON.stringify(newPolicies) : "null");
      producer.produce(
        "dataSubjectPolicies", // Topic
        null, // Partition, null uses default
        newPolicies ? new Buffer(JSON.stringify(newPolicies)) : null, // Either null in case of removal or the new set of policies
        dataSubjectId, // To ensure we only keep the latest set of policies
        Date.now()
      );

    } catch (err) {
      console.error("A problem occurred when sending our message");
      console.error(err);
    }
  },() =>{
    return conn.close();
  })
};

generateData = async function(){
  let conn = await r.connect({"host": dbHost, "port": dbPort});
  console.debug("Creating database...");
  try {
    await r.dbCreate(dbName).run(conn, function (err, result) {
      if(!err){console.debug("Database created: %s", result);}
    });
  }
  catch(error){console.debug("Database already exists.");}

  console.debug("Creating tables...");
  try {
    await r.expr(dbTables).forEach(db.tableCreate(r.row)).run(conn, function (err, result) {
      if(!err){console.debug("Tables created: %s", result);}
    });
  }
  catch(error){console.debug("Tables already exist.")}

  let promises = [];

  console.debug("Inserting base data");

  promises.push(dataControllerPoliciesTable.insert([
    {
      "id": "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymized",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EU",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Collect",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Account",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Delivery"
    },
    {
      "id": "54ff9c00-1b47-4389-8390-870b2ee9a03c",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Derived",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EULike",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Copy",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Same"
    },
    {
      "id": "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Computer",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ThirdParty",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Move",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Browsing",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Public"
    },
    {
      "id": "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Activity",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ControllerServers",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Aggregate",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Account",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Delivery"
    },
    {
      "id": "be155566-7b56-4265-92fe-cb474aa0ed42",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymized",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EU",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Analyze",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Admin",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Ours"
    },
    {
      "id": "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#AudiovisualActivity",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#EULike",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Anonymize",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#AnyContact",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Public"
    },
    {
      "id": "2f274ae6-6c2e-4350-9109-6c15e50ba670",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Computer",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ThirdCountries",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Copy",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Arts",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Same"
    },
    {
      "id": "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Content",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#OurServers",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Derive",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#AuxPurpose",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Unrelated"
    },
    {
      "id": "86371d81-30ff-49c4-897f-5e6dbc721e85",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Demographic",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ProcessorServers",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Move",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Browsing",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#Delivery"
    },
    {
      "id": "4d675233-279f-4b5e-8695-b0b66be4f0f9",
      "dataCollection": "http://www.specialprivacy.eu/vocabs/data#Derived",
      "locationCollection": "http://www.specialprivacy.eu/vocabs/data#ThirdParty",
      "processCollection": "http://www.specialprivacy.eu/vocabs/data#Aggregate",
      "purposeCollection": "http://www.specialprivacy.eu/vocabs/data#Charity",
      "recipientCollection": "http://www.specialprivacy.eu/vocabs/data#OtherRecipient"
    }
  ], {conflict: "replace"}).run(conn));

  promises.push(applicationsTable.insert([
    {
      "id": "d5aca7a6-ed5f-411c-b927-6f19c36b93c3",
      "name": "Super application",
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
      "name": "Super duper application",
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
  ], {conflict: "replace"}).run(conn));

  promises.push(dataSubjectsTable.insert([
    {
      "id": "9b84f8a5-e37c-4baf-8bdd-92135b1bc0f9",
      "name": "Bernard Antoine",
      "policies": [
        "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998"
      ]
    },
    {
      "id": "14f97114-bb25-43d2-85f9-b42c10538c09",
      "name": "Roger Frederick",
      "policies": [
        "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c"
      ]
    }
  ], {conflict: "replace"}).run(conn));

  await Promise.all(promises);
  console.debug("Data inserted");

  return conn.close();
};
