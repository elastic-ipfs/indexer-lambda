'use strict'

const { SQSClient, SendMessageCommand, SendMessageBatchCommand } = require('@aws-sdk/client-sqs')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')

const telemetry = require('./telemetry')
const { serializeError } = require('./logging')

const agent = new Agent({ keepAlive: true, keepAliveMsecs: 60000 })

const sqsClient = new SQSClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})

// todo retry logic

async function publish({ queue, message, logger }) {
  try {
    telemetry.increaseCount('sqs-publishes')
    await telemetry.trackDuration('sqs-publishes', sqsClient.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: message })))
  } catch (error) {
    logger.error({ error: serializeError(error), queue, message }, 'Cannot send notification')
  }
}

async function publishBatch({ queue, messages, logger }) {
  try {
    telemetry.increaseCount('sqs-publishes-batch')
    const entries = messages.map(message => ({
      Id: message,
      MessageBody: message
    }))
    await telemetry.trackDuration('sqs-publishes-batch', sqsClient.send(new SendMessageBatchCommand({ QueueUrl: queue, Entries: entries })))
  } catch (error) {
    logger.error({ error: serializeError(error), queue, messages }, 'Cannot send notification batch')
  }
}

module.exports = {
  publish,
  publishBatch
}
