'use strict'

const t = require('tap')
const createSource = require('../src/source')
const SourceInterface = require('../src/source/interface')

t.test('source', async t => {
  t.test('should get error creating a non existing source type', async t => {
    t.throws(() => createSource('non-existing'), 'unable to load source type: non-existing')
  })

  t.test('should get an error implementing source interfaces without get method', async t => {
    class BadSource extends SourceInterface { }

    const badSource = new BadSource()

    for (const method of ['load']) {
      try {
        await badSource[method]()
        t.fail(`should throw an error on method ${method}`)
      } catch (err) {
        t.equal(err.message, `source ${method} method not implemented`)
      }
    }
  })
})
