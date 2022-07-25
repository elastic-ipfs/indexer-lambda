'use strict'

process.env.ENV_FILE_PATH = 'dev/null'
process.env.LOG_LEVEL = ''
process.env.NODE_DEBUG = 'indexing-lambda'

const t = require('tap')
const { elapsed } = require('../src/lib/logging')

t.test('logging - elapsed times are correctly evaluated', async t => {
  const start = process.hrtime.bigint()
  t.not(elapsed(start), '0')
  t.not(elapsed(start, 2, 'nanoseconds'), '0')
  t.equal(elapsed(start, 0, 'seconds'), '0')
})
