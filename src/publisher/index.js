'use strict'

function createPublisher (type, options) {
  try {
    const Publisher = require('./' + type)
    return new Publisher(options)
  } catch (err) {
    throw new Error(`unable to load publisher type: ${type}`)
  }
}

module.exports = createPublisher
