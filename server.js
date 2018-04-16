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


// Get policies for an application
app.get("/policies", (req, res, next) => {
  console.debug("Received request to get policies");
  let appId = req.header("APP_KEY");

  let query = dataControllerPoliciesTable;

  // If an APP_KEY is specified, we get the policies for that application only.
  if(appId){
    query = query.filter(policy => {return r.expr(applicationsTable.get(appId)("needed-policies")).contains(policy("id"))})
  }
  query
    .map(row => {return row.merge({"type": "policy"})})
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      cursor.toArray()
        .then(policies=> {
          res.status(200).json({"policies": policies});
        })
        .error(error => {
          res.status(500).send(error);
        })
        .finally(() => {next();})
    })
    .error(handleError(res));
});

// Get policies linked to specified user
app.get("/users/:id/policies", (req, res, next) => {

  let userId = req.params.id;
  if(!userId){
    console.error("No userId specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to get policies for user: %s", userId);

  let appId = req.header("APP_KEY");

  let query = dataControllerPoliciesTable.getAll(r.args(dataSubjectsTable.get(userId)("policies").coerceTo("array")));

  // If an APP_KEY is specified, we get the policies for that application only.
  if(appId){
    query = query.filter(policy => {return r.expr(applicationsTable.get(appId)("needed-policies")).contains(policy("id"))})
  }
  query
    .map(row => {return row.merge({"userId": userId, "type": "policy"})})
    .orderBy("id")
    .run(req._rdbConn)
    .then(cursor => {
      cursor.toArray()
        .then(policies=> {
          res.status(200).json({"policies": policies});
        })
        .error(handleError(res))
        .finally(() => {next();})
    })
    .error(handleError(res));
});

app.get("/users/:id", (req, res, next) => {

  let userId = req.params.id;
  if(!userId){
    console.error("No userId specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to get user: %s", userId);

  dataSubjectsTable.get(userId).without({"policies": true}).run(req._rdbConn).then(user => {
    user["links"] = {
      "policies": "/users/"+userId+"/policies"
    };
    res.status(200).json({users: [user]});
  })
    .error(handleError(res))
    .finally(() => {
      next();
    })
});

app.put("/users/:id", (req, res, next) => {
  let userId = req.params.id;
  if(!userId){
    console.error("No userId specified, can't update.");
    res.status(400).send();
    next();
  }
  console.debug("Received request to update user: %s", userId);

  let appId = req.header("APP_KEY");
  let user = req.body.user;

  let query = applicationsTable;
  let dbUser = null;

  // If an APP_KEY is specified, we get the policies for that application only.
  if(appId){
    query = query.get(appId);
  }
  query
    ("needed-policies")
    .coerceTo("array")
    .run(req._rdbConn)
    .then(cursor => {
      return cursor.toArray();
    })
    .then(appPolicies => {
      return dataSubjectsTable.get(userId).run(req._rdbConn).then(dbUser => {
        return {
          appPolicies, dbUser
        };
      })
    })
    .then(hash => {
      dbUser = hash["dbUser"];

      let dbPolicies = dbUser["policies"];
      let appPolicies = hash["appPolicies"];
      let newPolicies = user["policies"];


      for(let appPolicy of appPolicies){
        if(dbPolicies.includes(appPolicy)){
          dbPolicies.splice(dbPolicies.indexOf(appPolicy));
        }
      }
      for(let newPolicy of newPolicies){
        dbPolicies.push(newPolicy);
      }

      return dataSubjectsTable.get(userId).update(dbUser).run(req._rdbConn).then(updateResult => {
        console.debug("User [%s] updated: ", userId, updateResult);
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
    await watchUsers();
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

watchUsers = async function(){
  console.debug("Starting to watch user changes...");
  let conn = await r.connect({"host": dbHost, "port": dbPort});
  let cursor = await dataSubjectsTable.changes({"includeInitial":true}).run(conn);
  return cursor.each(async (err, row) => {
    if(err){console.error("Error occurred on user modification: %s", err);}

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
      let userId;
      if(!row["new_val"]){
        userId = row["old_val"]["id"];
        // Remove policies from topic
        console.debug("User [%s] removed. ", userId);

        withdrawn = row["old_val"]["policies"];

        newPolicies = null;
      }
      else{
        userId = row["new_val"]["id"];

        if(row["old_val"]){
          // Check and propagate withdrawals of consent to history kafka topic
          withdrawn = row["old_val"]["policies"].filter(item => {return !row["new_val"]["policies"].includes(item)});
          added = row["new_val"]["policies"].filter(item => {return !row["old_val"]["policies"].includes(item)});
        }
        else {
          // New user
          added = row["new_val"]["policies"];
        }

        // Create new list of policies for user
        console.debug("User policies modified, generating new set of policies.  --%s", JSON.stringify(policies));

        newPolicies = {
          "simplePolicies": row["new_val"]["policies"].map(policy => {return policies[policy];})
        };
        console.log("newPolicies: %s", JSON.stringify(newPolicies));
      }

      for(let consent of withdrawn){
        console.log("Removing user [%s] consent for policy [%s].", userId, consent);
        let message = policies[consent];
        message["given"] = false;
        message["data-subject"] = userId;
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
        console.log("Adding user [%s] consent for policy [%s].", userId, consent);
        let message = policies[consent];
        message["given"] = true;
        message["data-subject"] = userId;
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
        "userPolicies", // Topic
        null, // Partition, null uses default
        newPolicies ? new Buffer(JSON.stringify(newPolicies)) : null, // Either null in case of removal or the new set of policies
        userId, // To ensure we only keep the latest set of policies
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
      "dataCollection": "Anonymized",
      "locationCollection": "Europe",
      "processCollection": "Collect",
      "purposeCollection": "Account",
      "recipientCollection": "Delivery"
    },
    {
      "id": "54ff9c00-1b47-4389-8390-870b2ee9a03c",
      "dataCollection": "Derived",
      "locationCollection": "EULike",
      "processCollection": "Copy",
      "purposeCollection": "Admin",
      "recipientCollection": "Same"
    },
    {
      "id": "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
      "dataCollection": "Computer",
      "locationCollection": "ThirdParty",
      "processCollection": "Move",
      "purposeCollection": "Browsing",
      "recipientCollection": "Public"
    },
    {
      "id": "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
      "dataCollection": "Activity",
      "locationCollection": "ControllerServers",
      "processCollection": "Aggregate",
      "purposeCollection": "Account",
      "recipientCollection": "Delivery"
    },
    {
      "id": "be155566-7b56-4265-92fe-cb474aa0ed42",
      "dataCollection": "Anonymized",
      "locationCollection": "EU",
      "processCollection": "Analyze",
      "purposeCollection": "Admin",
      "recipientCollection": "Ours"
    },
    {
      "id": "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
      "dataCollection": "AudiovisualActivity",
      "locationCollection": "EULike",
      "processCollection": "Anonymize",
      "purposeCollection": "AnyContact",
      "recipientCollection": "Public"
    },
    {
      "id": "2f274ae6-6c2e-4350-9109-6c15e50ba670",
      "dataCollection": "Computer",
      "locationCollection": "ThirdCountries",
      "processCollection": "Copy",
      "purposeCollection": "Arts",
      "recipientCollection": "Same"
    },
    {
      "id": "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
      "dataCollection": "Content",
      "locationCollection": "OurServers",
      "processCollection": "Derive",
      "purposeCollection": "AuxPurpose",
      "recipientCollection": "Unrelated"
    },
    {
      "id": "86371d81-30ff-49c4-897f-5e6dbc721e85",
      "dataCollection": "Demographic",
      "locationCollection": "ProcessorServers",
      "processCollection": "Move",
      "purposeCollection": "Browsing",
      "recipientCollection": "Delivery"
    },
    {
      "id": "4d675233-279f-4b5e-8695-b0b66be4f0f9",
      "dataCollection": "Derived",
      "locationCollection": "ThirdParty",
      "processCollection": "Aggregate",
      "purposeCollection": "Charity",
      "recipientCollection": "OtherRecipient"
    }
  ], {conflict: "replace"}).run(conn));

  promises.push(applicationsTable.insert([
    {
      "id": "d5aca7a6-ed5f-411c-b927-6f19c36b93c3",
      "name": "Super application",
      "needed-policies":
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
      "needed-policies":
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
