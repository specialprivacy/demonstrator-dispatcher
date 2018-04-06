const app = require("express")();
const ws = require("express-ws")(app);
const rethinkDB = require("rethinkdb");
const EventEmitter = require('events');


class MyEmitter extends EventEmitter {}
const myEmitter = new MyEmitter();

const dbHost = "localhost";
const dbPort = 28015;

const r = rethinkDB;
const dbName = "changeLogsProducer";
const db = r.db(dbName);
const applicationsTableName = "applications";
const applicationsTable = db.table(applicationsTableName);
const consentsTableName = "consents";
const consentsTable = db.table(consentsTableName);
const dataControllerPoliciesTableName = "dataControllerPolicies";
const dataControllerPoliciesTable = db.table(dataControllerPoliciesTableName);
const dataSubjectsTableName = "dataSubjects";
const dataSubjectsTable = db.table(dataSubjectsTableName);

const dbTables = [applicationsTableName, dataControllerPoliciesTableName, dataSubjectsTableName, consentsTableName];

let apps = {};

app.use(createConnection);

app.ws("/follow", async (ws, req, next) => {
  console.debug("/follow");

  console.log("req:" +JSON.stringify(req.query));

  ws.on("message", function incoming(message) {
    console.debug("Received message: %s", message);
    let consent = JSON.parse(message);
    consentsTable.get(consent["id"]).update({"given": consent["given"], "date": Date.now()}).run(req._rdbConn);
  });

  ws.on("close", function(){
    console.debug("Websocket was closed.");
    ws.terminate();
    next();
  });

  let appId = req.query.applicationId;
  let userId = req.query.userId;

  myEmitter.on("appChanged-"+appId, async () => {
    console.log("Event received");

    let app = apps[appId];
    let policies = app["needed-policies"];

    console.debug("Application needs following policies: "+JSON.stringify(policies));

    // TODO: /!\ Send a message to the client telling him remove all existing consents then we'll start filling them up again
    ws.send(JSON.stringify({operationType: "unloadAll"}));

    console.debug("Calculating and inserting missing consents");
    let promises = [];
    for(let policyKey in policies){
      let policy = policies[policyKey];
      await consentsTable.filter({userId})("policyId").coerceTo("array").contains(policy["id"]).not().run(req._rdbConn, function(err, result){
        if(err){throw err;}
        if(result){
          console.debug("No consent generated for this policy, generating now.");
          promises.push(consentsTable.insert({
            userId,
            policyId: policy["id"],
            given: false,
            date: Date.now()
          }).run(req._rdbConn))
        }
        else{
          console.debug("Consent already exists for this user and this policy.");
        }
      });
    }

    await Promise.all(promises);
    console.debug("Missing consents have been inserted.");

    consentsTable
    // We're only interested in consents for the user
      .filter({userId})
      // And consents related to the current application
      .filter(function(consent){
        return r.expr(Object.keys(policies)).contains(consent("policyId"))
      })
      // Let's subscribe to the changes shall we?
      .changes({squash: true, includeTypes: true, includeInitial: true})
      .run(req._rdbConn, function(err, cursor) {
        if (err) throw err;
        console.debug("Starting to watch changes to consents for application %s and user %s", appId, userId);

        cursor.each(function(err, row) {
          if(err){throw err;}

          // We received an update to the consent, need to join the data from the policies needed by the frontend with our consent object
          let consent = row["new_val"];
          let data = {
            operationType: "update",
            consent: Object.assign({}, policies[consent["policyId"]], consent)
          };
          ws.send(JSON.stringify(data));

        }, function(){
          console.debug("No longer following changes");
          ws.close();
          next();
        });
      })
  });
  myEmitter.emit("appChanged-"+appId);
});


app.use(closeConnection);

const server = app.listen(8081, async () => {
  const { address } = server.address();
  const { port } = server.address();

  await generateData();

  r.connect({dbHost, dbPort}).then(function(conn) {
    applicationsTable.changes({includeInitial: true}).run(conn, function(err, cursor){
      cursor.each(function(err, row) {
        if (err) {throw err;}
        let app = row["new_val"];

        dataControllerPoliciesTable.filter(policy => {
          return r.expr(app["needed-policies"]).contains(policy("id"))
        }).run(conn, function(err, cursor){
          if(err){throw err;}
          cursor.toArray().then(policies => {
            let policiesObj = {};
            for(let policy of policies){
              policiesObj[policy["id"]] = policy;
            }
            app["needed-policies"] = policiesObj;
            apps[app["id"]] = app;
            console.log("Apps after change: "+JSON.stringify(apps));
            myEmitter.emit("appChanged-"+app["id"]);
            console.log("Event sent");
          })
        });
      }, function(){
        console.debug("No longer following changes");
        conn.close();
      });
    })
  });


  console.debug("App listening at http://%s:%s", address, port);
});

function handleError(res) {
  return function(error) {
    res.status(500).send({error: error.message});
  }
}


function createConnection(req, res, next) {
  return r.connect({dbHost, dbPort}).then(function(conn) {
    req._rdbConn = conn;
    console.debug("Creating connection to database...");
    next();
  }).error(handleError(res));
}

function closeConnection(req, res, next) {
  console.debug("Closing connection to database...");
  req._rdbConn.close();
  next();
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


generateData = async function(){
  let conn = await r.connect({dbHost, dbPort});
  console.debug("Creating database...");
  try {
    await r.dbCreate(dbName).run(conn, function (err, result) {
      //if (err) throw err; // ignore errors
      if(!err){console.debug("Database created.");}
    });
  }
  catch(error){console.debug("Database already exists");}

  console.debug("Creating tables...");
  try {
    await r.expr(dbTables).forEach(db.tableCreate(r.row)).run(conn, function (err, result) {
      //if (err) throw err; //ignore errors
      if(!err){console.debug("Tables created.");}
    });
  }
  catch(error){console.debug("Tables already exist")}

  let promises = [];

  console.debug("Inserting base data");
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

  promises.push(dataControllerPoliciesTable.insert([
    {
      "id": "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
      "dataAtom": "Anonymized",
      "locationAtom": "Europe",
      "processAtom": "Collect",
      "purposeAtom": "Account",
      "recipientAtom": "Delivery"
    },
    {
      "id": "54ff9c00-1b47-4389-8390-870b2ee9a03c",
      "dataAtom": "Derived",
      "locationAtom": "EULike",
      "processAtom": "Copy",
      "purposeAtom": "Admin",
      "recipientAtom": "Same"
    },
    {
      "id": "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
      "dataAtom": "Computer",
      "locationAtom": "ThirdParty",
      "processAtom": "Move",
      "purposeAtom": "Browsing",
      "recipientAtom": "Public"
    },
    {
      "id": "fcef1dbf-7b3d-4608-bebc-3f7ff6ae4f29",
      "dataAtom": "Activity",
      "locationAtom": "ControllerServers",
      "processAtom": "Aggregate",
      "purposeAtom": "Account",
      "recipientAtom": "Delivery"
    },
    {
      "id": "be155566-7b56-4265-92fe-cb474aa0ed42",
      "dataAtom": "Anonymized",
      "locationAtom": "EU",
      "processAtom": "Analyze",
      "purposeAtom": "Admin",
      "recipientAtom": "Ours"
    },
    {
      "id": "8a7cf1f6-4c34-497f-8a65-4c985eb47a35",
      "dataAtom": "AudiovisualActivity",
      "locationAtom": "EULike",
      "processAtom": "Anonymize",
      "purposeAtom": "AnyContact",
      "recipientAtom": "Public"
    },
    {
      "id": "2f274ae6-6c2e-4350-9109-6c15e50ba670",
      "dataAtom": "Computer",
      "locationAtom": "ThirdCountries",
      "processAtom": "Copy",
      "purposeAtom": "Arts",
      "recipientAtom": "Same"
    },
    {
      "id": "5f8d8a7b-e250-41ca-b23e-efbfd2d83911",
      "dataAtom": "Content",
      "locationAtom": "OurServers",
      "processAtom": "Derive",
      "purposeAtom": "AuxPurpose",
      "recipientAtom": "Unrelated"
    },
    {
      "id": "86371d81-30ff-49c4-897f-5e6dbc721e85",
      "dataAtom": "Demographic",
      "locationAtom": "ProcessorServers",
      "processAtom": "Move",
      "purposeAtom": "Browsing",
      "recipientAtom": "Delivery"
    },
    {
      "id": "4d675233-279f-4b5e-8695-b0b66be4f0f9",
      "dataAtom": "Derived",
      "locationAtom": "ThirdParty",
      "processAtom": "Aggregate",
      "purposeAtom": "Charity",
      "recipientAtom": "OtherRecipient"
    }
  ], {conflict: "replace"}).run(conn));

  promises.push(dataSubjectsTable.insert([
    {
      "id": "9b84f8a5-e37c-4baf-8bdd-92135b1bc0f9",
      "name": "Bernard Antoine"
    },
    {
      "id": "14f97114-bb25-43d2-85f9-b42c10538c09",
      "name": "Roger Frederick"
    }
  ], {conflict: "replace"}).run(conn));

  promises.push(consentsTable.insert([
    {
      "id": "cfccdf40-a816-44cf-a2f8-a336005b868b",
      "date": new Date(),
      "userId": "9b84f8a5-e37c-4baf-8bdd-92135b1bc0f9",
      "policyId": "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
      "given": true
    },
    {
      "id": "689d802b-f8e7-4f7e-b861-6f7ccdc31b1b",
      "date": new Date(),
      "userId": "14f97114-bb25-43d2-85f9-b42c10538c09",
      "policyId": "d308b593-a2ad-4d9f-bcc3-ff47f4acfe5c",
      "given": true
    }
  ], {conflict: "replace"}).run(conn));

  await Promise.all(promises);
  console.debug("Data inserted");

  conn.close();
};
