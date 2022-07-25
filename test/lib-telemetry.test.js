'use strict'

process.env.ENV_FILE_PATH = 'dev/null'
process.env.LOG_LEVEL = ''
process.env.NODE_DEBUG = 'indexing-lambda'

const t = require('tap')
const { logger } = require('../src/lib/logging')
const telemetry = require('../src/lib/telemetry')

t.test('telemetry', async t => {
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
