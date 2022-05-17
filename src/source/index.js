'use strict'

function createSource (type, options) {
  try {
    const Source = require('./' + type)
    return new Source(options)
  } catch (err) {
    throw new Error(`unable to load source type: ${type}`)
  }
}

module.exports = createSource
