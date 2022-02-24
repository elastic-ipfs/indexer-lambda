'use strict'

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')

const { CarIterator } = require('./iterator')
const { logger, serializeError } = require('./logging')
const telemetry = require('./telemetry')

const s3Client = new S3Client({
  requestHandler: new NodeHttpHandler({ httpsAgent: new Agent({ keepAlive: true, keepAliveMsecs: 60000 }) })
})

async function openS3Stream(url) {
  let s3Request

  // Load the file from input
  try {
    telemetry.increaseCount('s3-fetchs')

    const Bucket = url.hostname
    const Key = url.pathname.slice(1)

    // this imports just the getObject operation from S3
    s3Request = await telemetry.trackDuration('s3-fetchs', s3Client.send(new GetObjectCommand({ Bucket, Key })))
  } catch (e) {
    logger.error(`Cannot open file ${url}: ${serializeError(e)}`)
    throw e
  }

  // Start parsing as CAR file
  try {
    return await CarIterator.fromReader(s3Request.Body, s3Request.ContentLength)
  } catch (e) {
    logger.error(`Cannot parse file ${url} as CAR: ${serializeError(e)}`)
    throw e
  }
}

module.exports = {
  openS3Stream
}
