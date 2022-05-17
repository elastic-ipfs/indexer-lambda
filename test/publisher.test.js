'use strict'

const t = require('tap')
const createPublisher = require('../src/publisher')
const PublisherInterface = require('../src/publisher/interface')

t.test('publisher', async t => {
  t.test('should get error creating a non existing publisher type', async t => {
    t.throws(() => createPublisher('non-existing'), 'unable to load publisher type: non-existing')
  })

  t.test('should get an error implementing publisher interfaces without get method', async t => {
    class BadPublisher extends PublisherInterface { }

    const badPublisher = new BadPublisher()

    for (const method of ['send']) {
      try {
        await badPublisher[method]()
        t.fail(`should throw an error on method ${method}`)
      } catch (err) {
        t.equal(err.message, `publisher ${method} method not implemented`)
      }
    }
  })
})
