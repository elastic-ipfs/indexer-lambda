'use strict'

const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')

const PublisherInterface = require('./interface')
const { serializeError } = require('../logging')

class PublisherAwsSqs extends PublisherInterface {
  constructor(options) {
    super(options)

    if (!options.telemetry) {
      throw new Error('telemetry instance is required')
    }
    if (!options.logger) {
      throw new Error('logger instance is required')
    }
    if (!options.client && !options.agent && !options.httpsAgentKeepAlive) {
      throw new Error('one of client instance, agent instance or httpsAgentKeepAlive is required')
    }

    this.client = options.client ?? new SQSClient({
      requestHandler: new NodeHttpHandler({
        httpsAgent: options.agent ?? new Agent({ keepAlive: true, keepAliveMsecs: options.httpsAgentKeepAlive })
      })
    })
    this.telemetry = options.telemetry
    this.logger = options.logger
  }

  async send (queue, data) {
    try {
      this.telemetry.increaseCount('sqs-publishes')

      await this.telemetry.trackDuration(
        'sqs-publishes',
        this.client.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: data }))
      )
    } catch (err) {
      this.logger.error(`Cannot publish a block to ${queue}: ${serializeError(err)}`)

      throw err
    }
  }
}

module.exports = PublisherAwsSqs
