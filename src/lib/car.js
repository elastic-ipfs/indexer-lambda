'use strict'

const config = require('../config')
const { elapsed } = require('./logging')
const { openS3Stream } = require('./source')
const { createDynamoItem, readDynamoItem } = require('./storage')
const { publish, notify } = require('./publish')
const { now } = require('./util')
const { storeBlocks, publishBlocks } = require('./block')

function validateCar(id) {
  try {
    const info = id.match(/([^/]+)\/([^/]+)\/(.+)/)
    if (!info) {
      return
    }
    const [, bucketRegion, bucketName, key] = info
    const url = new URL(`s3://${bucketName}/${key}`)
    return { url, id, bucketRegion, bucketName, key }
    /* c8 ignore next */
  } catch (error) {}
}

/**
 * Load the file from input
 * @async
 */
function openSource({ car, logger }) {
  return openS3Stream({ bucketRegion: car.bucketRegion, url: car.url, logger })
}

/**
 * Load car from db
 * @async
 */
function retrieveCar({ id, logger }) {
  return readDynamoItem({ table: config.carsTable, keyName: config.carsTablePrimaryKey, keyValue: id, logger })
}

async function createCar({ car, source, logger }) {
  await createDynamoItem({
    table: config.carsTable,
    keyName: config.carsTablePrimaryKey,
    keyValue: car.id,
    data: {
      bucket: car.bucketName,
      bucketRegion: car.bucketRegion,
      key: car.key,
      createdAt: now(),
      roots: Array.from(new Set(source.roots.map(r => r.toString()))),
      version: source.version,
      fileSize: source.length
    },
    logger
  })
}

async function publishCar({ car, logger, queue = config.notificationsQueue }) {
  await publish({ queue, message: car.id, logger })
}

/**
 * Broadcast IndexerComplete event on events pubsub topic
 * @param {object} options
 * @param {object} options.car
 * @param {URL} options.car.url - url of CAR
 * @param {URL} options.car.contentLength - byte length of CAR file
 * @param {object} options.indexing - describes the indexing process
 * @param {Date} options.indexing.startTime - when indexing began
 * @param {Date} options.indexing.endTime - when indexing completed
 * @param {Logger} options.logger - used to log errors
 */
async function notifyIndexerCompletedEvent({
  topic = config.eventsTopic,
  car,
  indexing,
  logger
}) {
  await notify({
    logger,
    topic,
    message: JSON.stringify({
      type: 'IndexerCompleted',
      uri: car.url.toString(),
      byteLength: car.contentLength,
      indexing: {
        startTime: indexing.startTime.toISOString(),
        endTime: indexing.endTime.toISOString()
      }
    })
  })
}

async function storeCar({ id, skipExists, logger }) {
  const start = process.hrtime.bigint()

  const car = validateCar(id)
  if (!car) {
    logger.error('Invalid CAR file format')
    throw new Error('Invalid CAR file format')
  }

  if (skipExists) {
    const exists = await retrieveCar({ id: car.id, logger })
    if (exists) {
      logger.info({ elapsed: elapsed(start) }, 'CAR already exists, skipping')
      return
    }
  }

  const source = await openSource({ car, logger })

  await createCar({ car, source: source.indexer, logger })

  /* c8 ignore next */
  const onTaskComplete = config.skipPublishing ? undefined : publishBlocks
  const blocks = await storeBlocks({ car, source: source.indexer, logger, onTaskComplete })
  await publishCar({ car, logger })
  logger.info(
    {
      car: car.id,
      source: source.stats,
      blocks: blocks.count,
      duration: elapsed(start)
    },
    'Indexing CAR complete'
  )
  await notifyIndexerCompletedEvent({
    car: {
      url: car.url,
      contentLength: source.stats.contentLength
    },
    indexing: {
      // @todo - consider including duration as nanoseconds instead of startTime/endTime milliseconds-resolution Dates
      startTime: new Date(Number(start) / 1e6),
      endTime: new Date()
    }
  })
}

module.exports = {
  storeCar
}
