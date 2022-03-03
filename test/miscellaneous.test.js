'use strict'

process.env.ENV_FILE_PATH = 'dev/null'
process.env.LOG_LEVEL = ''
process.env.NODE_DEBUG = 'aws-ipfs-indexing-lambda'

const t = require('tap')
const { logger, elapsed } = require('../src/logging')
const telemetry = require('../src/telemetry')

t.test('logging - elapsed times are correctly evaluated', t => {
  t.plan(3)

  const start = process.hrtime.bigint()
  t.not(elapsed(start), '0')
  t.not(elapsed(start, 2, 'nanoseconds'), '0')
  t.equal(elapsed(start, 0, 'seconds'), '0')
})

t.test('telemetry', async t => {
  t.plan(3)

  // Reset other metrics
  telemetry.logger = {
    info(arg) {}
  }
  telemetry.flush()

  // Prepare metrics
  telemetry.createMetric('custom', 'Custom', 'count')
  telemetry.createMetric('active', 'Active', 'count')

  telemetry.logger = {
    info(arg) {
      t.strictSame(arg, {
        ipfs_provider_component: 'indexer-lambda',
        metrics: { 'custom-count': 1, 'active-count': -1 }
      })
    }
  }

  telemetry.increaseCount('custom')
  telemetry.increaseCount('custom')
  telemetry.decreaseCount('custom')
  telemetry.decreaseCount('active')
  telemetry.flush()

  // Set the logger to check the refresh
  telemetry.logger = {
    info(arg) {
      t.strictSame(arg, {
        ipfs_provider_component: 'indexer-lambda',
        metrics: { 'active-count': 0 }
      })
    }
  }

  telemetry.flush()

  // Now check other methods
  t.throws(() => telemetry.decreaseCount('unknown'), 'Metrics unknown not found.')

  // Reset the logger
  telemetry.logger = logger
})
