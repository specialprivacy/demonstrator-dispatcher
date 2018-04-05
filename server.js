const app = require("express")();
const ws = require("express-ws")(app);
const rethinkDB = require("rethinkdb");

const host = "localhost";
const port = 28015;

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

app.use(createConnection);

app.ws("/follow", (ws, req, next) => {
  console.debug("/follow");

  ws.on("message", function incoming(message) {
    console.debug("Received message: %s", message);
    let consent = JSON.parse(message);
    consentsTable.get(consent["id"]).update({"given": consent["given"], "date": Date.now()}).run(req._rdbConn);
  });

  let appId = "d5aca7a6-ed5f-411c-b927-6f19c36b93c3";
  let userId = "9b84f8a5-e37c-4baf-8bdd-92135b1bc0f9";


  console.debug("Getting policies needed by application...");
  dataControllerPoliciesTable.getAll(
    r.args(
      applicationsTable
        .get(appId)("needed-policies").coerceTo("array")
    )
  ).run(req._rdbConn, function(err, cursor) {
    if (err) throw err;
    cursor.toArray(async function(err, policies) {
      if(err) {
        return next(err);
      }
      console.debug("Application needs following policies: "+JSON.stringify(policies));

      console.debug("Calculating and inserting missing consents");
      let promises = [];
      for(let policy of policies){
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
        return r.expr(policies.map(policy => {return policy["id"]})).contains(consent("policyId"))
      })
        // Let's subscribe to the changes shall we?
        .changes({squash: true, includeTypes: true, includeInitial: true})
        .run(req._rdbConn, function(err, cursor) {
        if (err) throw err;
        console.debug("Starting to watch changes to consents for application %s and user %s", appId, userId);

        cursor.each(function(err, row) {
          if(err){throw err;}

          // We received an update to the consent, need to join the data from the policies needed by the frontend with our consent object
          consentsTable.filter({"id":row["new_val"]["id"]})
            .eqJoin("policyId", dataControllerPoliciesTable)
            .without({"right":{"id":true}}).zip()
            .run(req._rdbConn, function(err, cursor){
              if(err){throw err;}
              cursor.toArray(function(err, results) {
                if (err) {throw err;}
                // It's sending time!
                for (let result of results) {
                  let data = {
                    consent: result
                  };
                  ws.send(JSON.stringify(data));
                }
              })
            });

        }, function(){
          console.debug("No longer following changes");
          next();
        });
      })

    });
  });
});

app.get("/create-database", (req, res, next) => {
  console.debug("Creating database...");
  r.dbCreate(dbName).run(req._rdbConn, function(err, result) {
    if (err) throw err;
    res.send("Database created.");
    next();
  });
});

app.get("/create-tables", (req, res, next) => {
  console.debug("Creating tables...");
  r.expr(dbTables).forEach(db.tableCreate(r.row)).run(req._rdbConn, function(err, result) {
    if (err) throw err;
    res.send("Tables created.");
    next();
  });
});

app.get("/insert-base-data", async (req, res, next) => {
  console.debug("Inserting base data");
  await applicationsTable.insert([
    {
      "id": "d5aca7a6-ed5f-411c-b927-6f19c36b93c3",
      "name": "Super application",
      "needed-policies":
        [
          "d5bbb4cc-59c0-4077-9f7e-2fad74dc9998",
          "54ff9c00-1b47-4389-8390-870b2ee9a03c"
        ]
    }
  ]).run(req._rdbConn);

  await dataControllerPoliciesTable.insert([
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
    }
  ]).run(req._rdbConn);

  await dataSubjectsTable.insert([
    {
      "id": "9b84f8a5-e37c-4baf-8bdd-92135b1bc0f9",
      "name": "Bernard Antoine"
    },
    {
      "id": "14f97114-bb25-43d2-85f9-b42c10538c09",
      "name": "Roger Frederick"
    }
  ]).run(req._rdbConn);

  await consentsTable.insert([
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
  ]).run(req._rdbConn);
  res.status(200).send("Data inserted");
  next();
});


app.use(closeConnection);

const server = app.listen(8081, () => {
  const { address } = server.address();
  const { port } = server.address();

  console.debug("App listening at http://%s:%s", address, port);
});


function handleError(res) {
  return function(error) {
    res.status(500).send({error: error.message});
  }
}

function createConnection(req, res, next) {
  return r.connect({host, port}).then(function(conn) {
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

