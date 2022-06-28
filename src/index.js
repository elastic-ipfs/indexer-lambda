'use strict'

const { logger, serializeError } = require('./lib/logging')
const telemetry = require('./lib/telemetry')
const { storeCar } = require('./lib/car')
const config = require('./config')

/**
 * Returns an empty object to signal we have consumed all the messages
 */
async function main(event) {
  if (event.Records.length !== 1) {
    logger.error(`Indexer Lambda invoked with ${event.Records.length} CARs while should be 1`)
    throw new Error(`Indexer Lambda invoked with ${event.Records.length} CARs while should be 1`)
  }
  const carId = event.Records[0].body
  const skipExists = Boolean(event.Records[0].skipExists)
  const decodeBlocks = event.Records[0].decodeBlocks ? Boolean(event.Records[0].decodeBlocks) : config.decodeBlocks

  try {
    try {
      logger.debug('Indexing CARs progress')

      const carLogger = logger.child({ car: carId })
      await storeCar({ id: carId, skipExists, decodeBlocks, logger: carLogger })
    } finally {
      telemetry.flush()
    }
  } catch (err) {
    logger.error({ car: carId, error: serializeError(err) }, 'Cannot index the CAR file')
    throw err
  }

  return {}
}

exports.handler = main
