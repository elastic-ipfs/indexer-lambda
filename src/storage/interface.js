'use strict'

class StorageInterface {
  constructor (options) {
    this.options = options
  }

  async readItem (table, keyName, keyValue) { throw new Error('storage readItem method not implemented') }
  async writeItem (create, table, keyName, keyValue, data, conditions) { throw new Error('storage writeItem method not implemented') }
  async deleteItem (table, keyName, keyValue) { throw new Error('storage deleteItem method not implemented') }
}

module.exports = StorageInterface
