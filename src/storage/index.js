'use strict'

function createStorage (type, options) {
  try {
    const Storage = require('./' + type)
    return new Storage(options)
  } catch (err) {
    throw new Error(`unable to load storage type: ${type}`)
  }
}

module.exports = createStorage
