'use strict'

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')
const sleep = require('util').promisify(setTimeout)

const { s3MaxRetries, s3RetryDelay } = require('./config')
const { CarIterator } = require('./iterator')
const { logger, serializeError } = require('./logging')
const telemetry = require('./telemetry')

const s3Clients = {}

async function openS3Stream (bucketRegion, url, retries = s3MaxRetries, retryDelay = s3RetryDelay) {
  let s3Request

  if (!s3Clients[bucketRegion]) {
    s3Clients[bucketRegion] = new S3Client({
      region: bucketRegion,
      requestHandler: new NodeHttpHandler({ httpsAgent: new Agent({ keepAlive: true, keepAliveMsecs: 60000 }) })
    })
  }
  const s3Client = s3Clients[bucketRegion]
  telemetry.increaseCount('s3-fetchs')

  const Bucket = url.hostname
  const Key = url.pathname.slice(1)

  let attempts = 0
  let error
  do {
    error = null
    try {
      // this imports just the getObject operation from S3
      s3Request = await telemetry.trackDuration('s3-fetchs', s3Client.send(new GetObjectCommand({ Bucket, Key })))
      break
    } catch (err) {
      if (err.code === 'NoSuchKey') { // not found
        logger.error({ error: serializeError(err) }, `Cannot open file S3 URL ${url}, does not exists`)
        throw err
      }
      logger.warn(`S3 Error, URL: ${url} Error: "${err.message}" attempt ${attempts + 1} / ${retries}`)
      error = err
    }
    await sleep(retryDelay)
  } while (++attempts < retries)

  if (error) {
    logger.error({ error: serializeError(error) }, `Cannot open file S3 URL ${url} after ${attempts} attempts`)
    throw error
  }

  // Start parsing as CAR file
  try {
    return await CarIterator.fromReader(s3Request.Body, s3Request.ContentLength)
  } catch (e) {
    logger.error({ error: serializeError(e) }, `Cannot parse file ${url} as CAR`)
    throw e
  }
}

module.exports = {
  openS3Stream
}
