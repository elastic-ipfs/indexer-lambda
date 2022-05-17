'use strict'

const t = require('tap')
const createStorage = require('../src/storage')
const StorageInterface = require('../src/storage/interface')

t.test('storage', async t => {
  t.test('should get error creating a non existing storage type', async t => {
    t.throws(() => createStorage('non-existing'), 'unable to load storage type: non-existing')
  })

  t.test('should get an error implementing storage interfaces without get method', async t => {
    class BadStorage extends StorageInterface { }

    const badStorage = new BadStorage()

    for (const method of ['readItem', 'writeItem', 'deleteItem']) {
      try {
        await badStorage[method]()
        t.fail(`should throw an error on method ${method}`)
      } catch (err) {
        t.equal(err.message, `storage ${method} method not implemented`)
      }
    }
  })
})
