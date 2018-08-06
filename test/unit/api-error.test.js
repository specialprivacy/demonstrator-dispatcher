const APIError = require("../../utils/api-error")
const AJV = require("ajv")
const ajv = new AJV()
const {BAD_REQUEST, INTERNAL_SERVER_ERROR} = require("http-status-codes")
const {expect} = require("chai")
const rewire = require("rewire")

const errorSchema = require("../schemas/json-api-error.schema.json")
const errorValidator = ajv.compile(errorSchema)
const setDefaults = rewire("../../utils/api-error.js").__get__("setDefaults")

describe("APIError", () => {
  describe("toJSON()", () => {
    it("Should return a jsonapi error when using a single error as argument", () => {
      const input = new APIError()

      expect(input.toJSON()).to.satisfy(errorValidator)
    })

    it("Should return a jsonapi error when using an array of errors as argument", () => {
      const input = new APIError([{}, {title: "error2"}])
      expect(input.toJSON()).to.satisfy(errorValidator)
    })

    it("Should return a jsonapi error when using an array with 1 error as argument", () => {
      const input = new APIError([{}])

      expect(input.toJSON()).to.satisfy(errorValidator)
    })
  })

  describe("setDefaults()", () => {
    it("Should return all custom error attributes", () => {
      const input = {
        statusCode: INTERNAL_SERVER_ERROR,
        title: "Test error title",
        detail: "Test error detail",
        source: "Test error source"
      }

      expect(setDefaults(input)).to.deep.equal({
        statusCode: input.statusCode,
        title: input.title,
        detail: input.detail,
        source: input.source,
        error: {}
      })
    })

    it("Should default source.pointer \"/\"", () => {
      const input = {}

      expect(setDefaults(input)).to.have.deep.property("source", "/")
    })

    it("Should default title to \"Server Error\"", () => {
      const input = {}

      expect(setDefaults(input)).to.have.property("title", "Server Error")
    })

    it("Should default code to \"500\"", () => {
      const input = {}

      expect(setDefaults(input)).to.have.property("statusCode", INTERNAL_SERVER_ERROR)
    })

    it("Should default detail to equal title", () => {
      const input = {
        title: "Test error title"
      }

      expect(setDefaults(input)).to.have.property("detail", input.title)
    })

    it("Should pick up the status set by bodyParser", () => {
      const error = new Error("Body parsing error")
      error.status = BAD_REQUEST
      const input = {
        error
      }

      expect(setDefaults(input)).to.have.property("statusCode", BAD_REQUEST)
    })

    it("Should pick up the error message as title", () => {
      const error = new Error("Body parsing error")
      const input = {
        error
      }

      expect(setDefaults(input)).to.have.property("detail", "Body parsing error")
    })
  })

  describe("Constructor", () => {
    it("Should set the error statusCode to the options statusCode", () => {
      const input = new APIError({statusCode: BAD_REQUEST})

      expect(input).to.have.property("statusCode", BAD_REQUEST)
    })

    it("Should set name to APIError", () => {
      const input = new APIError()

      expect(input).to.have.property("name", "APIError")
    })

    it("Should copy the stacktrace from the wrapped error", () => {
      const error = new Error()
      const input = new APIError({error})

      expect(input).to.have.property("stack", error.stack)
    })

    it("Should copy the message from the wrapped error", () => {
      const message = "I am the message"
      const error = new Error(message)
      const input = new APIError({error})

      expect(input).to.have.property("message", error.message)
    })

    it("Should copy the message from the input options", () => {
      const message = "I am the message"
      const input = new APIError({message})

      expect(input).to.have.property("message", message)
    })

    it("Should prefer the message of a wrapped error over the options message", () => {
      const message = "I am the options message"
      const error = new Error("I am the error message")
      const input = new APIError({error, message})

      expect(input).to.have.property("message", error.message)
    })
  })

  describe("Getters and Setters", () => {
    it("Should set the error statusCode", () => {
      const input = new APIError([{statusCode: INTERNAL_SERVER_ERROR}, {}])
      input.statusCode = BAD_REQUEST

      expect(input).to.have.property("statusCode", BAD_REQUEST)
    })

    it("Should return the title attribute if there is only one wrapped error", () => {
      const title = "my-title"
      const input = new APIError({title})

      expect(input).to.have.property("title", title)
    })

    it("Should return the detail attribute if there is only one wrapped error", () => {
      const detail = "my-detail"
      const input = new APIError({detail})

      expect(input).to.have.property("detail", detail)
    })

    it("Should return the source attribute if there is only one wrapped error", () => {
      const source = "my-source"
      const input = new APIError({source})

      expect(input).to.have.property("source", source)
    })

    it("Should return an array of all titles if there are multiple wrapped errors", () => {
      const title = "my-title"
      const input = new APIError([{title}, {}, {title}])

      expect(input).to.have.property("title").that.deep.equals([title, "Server Error", title])
    })

    it("Should return an array of all details if there are multiple wrapped errors", () => {
      const detail = "my-detail"
      const input = new APIError([{detail}, {}, {detail}])

      expect(input).to.have.property("detail").that.deep.equals([detail, "Server Error", detail])
    })

    it("Should return an array of all pointers if there are multiple wrapped errors", () => {
      const source = "my-source"
      const input = new APIError([{source}, {}, {source}])

      expect(input).to.have.property("source").that.deep.equals([source, "/", source])
    })
  })
})
