'use strict'

const { logger, serializeError } = require('./lib/logging')
const telemetry = require('./lib/telemetry')
const { storeCar } = require('./lib/car')

/**
 * Event to invoke the lambda
 * @typedef {Object} Event
 * @property {Array<Record>} Records
 */

/**
 * @typedef {Object} Record
 * @property {String} body - because of SQS message format, body can be the car id or the JSON stringified record with following properties:
 * - skipExists - default `false`, skip to process the CAR if already exists in db
 * @example { body: 'car-id' }
 * @example { body: '{"body":"car-id",skipExists:true}' }
 */

function parseEvent(event) {
  if (event.Records.length !== 1) {
    logger.error(`Indexer Lambda invoked with ${event.Records.length} CARs while should be 1`)
    throw new Error(`Indexer Lambda invoked with ${event.Records.length} CARs while should be 1`)
  }

  const body = event.Records[0].body
  const msgReceiveCount = event.Records[0].attributes?.ApproximateReceiveCount
  if (body[0] === '{') {
    try {
      const { body: carId, skipExists } = JSON.parse(body)
      return { carId, skipExists, msgReceiveCount }
    } catch {
      throw new Error('Invalid JSON in event body: ' + body)
    }
  }
  return { carId: event.Records[0].body, msgReceiveCount }
}

/**
 * Returns an empty object on success
 * @param {Event} event
 */
async function main(event) {
  const { carId, skipExists, msgReceiveCount } = parseEvent(event)

  try {
    // /// TODO: DEBUG If that's not the attribute, print all attributes to find the one
    logger.info('***** record object properties')
    logger.info(JSON.stringify(event.Records[0], null, 4))
    // ///
    logger.debug('Indexing CARs progress')
    const carLogger = logger.child({ car: carId })
    await storeCar({ id: carId, skipExists, logger: carLogger })
  } catch (err) {
    logger.error({ car: carId, error: serializeError(err) }, `Cannot index the CAR file. SQS MessageReceiveCount = ${msgReceiveCount}`)
    throw err
  } finally {
    telemetry.flush()
  }
}

exports.handler = main
