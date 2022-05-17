'use strict'

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')

const { CarIterator } = require('../iterator')
const SourceInterface = require('./interface')
const { serializeError } = require('../logging')

class SourceAwsS3 extends SourceInterface {
  constructor(options) {
    super(options)

    if (!options.telemetry) {
      throw new Error('telemetry instance is required')
    }
    if (!options.logger) {
      throw new Error('logger instance is required')
    }
    if (!options.httpsAgentKeepAlive) {
      throw new Error('httpsAgentKeepAlive instance is required')
    }

    this.telemetry = options.telemetry
    this.logger = options.logger

    this.clients = {}
  }

  /**
   * create a new S3 client for the given region
   * @param {string} region
   * @param {?S3Client} client optional client to use, if not provided a new one will be created
   */
  createClient(region, client) {
    if (client) {
      this.clients[region] = client
      return
    }
    this.clients[region] = new S3Client({
      region: region,
      requestHandler: new NodeHttpHandler({ httpsAgent: new Agent({ keepAlive: true, keepAliveMsecs: this.options.httpsAgentKeepAlive }) })
    })
  }

  /**
   * load a file from S3; will create a client for the region if one does not already exist
   * @param {string} url
   * @param {string} region
   */
  async load (url, region) {
    let request
    try {
      if (!this.clients[region]) {
        this.createClient(region)
      }
      const s3Client = this.clients[region]
      this.telemetry.increaseCount('s3-fetchs')

      const Bucket = url.hostname
      const Key = url.pathname.slice(1)

      // this imports just the getObject operation from S3
      request = await this.telemetry.trackDuration('s3-fetchs', s3Client.send(new GetObjectCommand({ Bucket, Key })))
    } catch (err) {
      this.logger.error(`Cannot open file ${url}: ${serializeError(err)}`)
      throw err
    }

    // Start parsing as CAR file
    try {
      return await CarIterator.fromReader(request.Body, request.ContentLength)
    } catch (err) {
      this.logger.error(`Cannot parse file ${url} as CAR: ${serializeError(err)}`)
      throw err
    }
  }
}

module.exports = SourceAwsS3
