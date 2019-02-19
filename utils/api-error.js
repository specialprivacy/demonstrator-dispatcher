const { getStatusText, INTERNAL_SERVER_ERROR } = require("http-status-codes")

class APIError extends Error {
  constructor (options = {}) {
    super(options.title || "APIError")

    this.name = "APIError"
    this._statusCode = options.statusCode || INTERNAL_SERVER_ERROR
    if (options.error) {
      this.stack = options.error.stack
      this.message = options.error.message
    } else if (options.message) {
      this.message = options.message
    }
    if (!Array.isArray(options)) options = [options]

    this.errors = options.map(setDefaults)
  }

  get statusCode () {
    return this._statusCode
  }

  set statusCode (statusCode) {
    this._statusCode = statusCode
  }

  get title () {
    let output = this.errors.map((item) => item.title)
    return output.length === 1 ? output[0] : output
  }

  get detail () {
    let output = this.errors.map((item) => item.detail)
    return output.length === 1 ? output[0] : output
  }

  get source () {
    let output = this.errors.map((item) => item.source)
    return output.length === 1 ? output[0] : output
  }

  toJSON () {
    let errors = this.errors.map((error) => ({
      code: error.statusCode.toString(),
      title: error.title,
      detail: error.detail,
      source: { pointer: error.source }
    }))
    return { errors }
  }
}

/**
 * Set Default values for the jsonapi error attributes
 * @param  {object} error An object containing the jsonapi error attributes
 * @return {object}        An object containing the jsonapi error attributes with appropriate default values
 */
function setDefaults (error) {
  error.error = error.error ? error.error : {}
  let statusCode = error.statusCode || error.error.status || INTERNAL_SERVER_ERROR
  return {
    statusCode,
    title: error.title || getStatusText(statusCode),
    detail: error.detail || error.error.message || error.title || getStatusText(statusCode),
    source: error.source || "/",
    error: error.error
  }
}

module.exports = APIError
