'use strict'

const { logger, serializeError } = require('./lib/logging')
const telemetry = require('./lib/telemetry')
const { storeCar } = require('./lib/car')
const config = require('./config')

/**
 * Event to invoke the lambda
 * @typedef {Object} Event
 * @property {Array<Record>} Records
 */

/**
 * @typedef {Object} Record
 * @property {String} body - actually the CAR id
 * @property {?Bool} skipExists - default `false`, skip to process the CAR if already exists in db
 * @property {?Bool} decodeBlocks - default `false`, decode CAR blocks
 */

/**
 * Returns an empty object to signal we have consumed all the messages
 * @param {Event} event
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
