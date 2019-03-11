let Ajv = require("ajv")
let ajv = new Ajv()
let chai = require("chai")
let expect = chai.expect
let proxyquire = require("proxyquire")

let { INTERNAL_SERVER_ERROR } = require("http-status-codes")
let request = (server) => chai.request(server)
let schema = require("../../schemas/json-api-error.schema.json")
let schemaValidator = ajv.compile(schema)

let app = proxyquire("../../../app.js", {
  "./utils/rethinkdb_config": {
    createConnection: () => {}
  }
})

describe("error-handler", () => {
  it("Should return a json error if content-type json was request", () => {
    let resp = request(app)
      .get("/callback")
      .set("accept", "application/json")

    return expect(resp).to.eventually.have.status(INTERNAL_SERVER_ERROR)
      .and.be.json
      .and.satisfy((payload) => schemaValidator(payload.body))
  })
})
