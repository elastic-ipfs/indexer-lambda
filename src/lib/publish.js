'use strict'

const { SQSClient, SendMessageCommand, SendMessageBatchCommand } = require('@aws-sdk/client-sqs')
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')

const telemetry = require('./telemetry')
const { serializeError } = require('./logging')

const agent = new Agent({ keepAlive: true, keepAliveMsecs: 60000 })

const snsClient = new SNSClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})
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

    const entries = [...new Set(messages)].map(message => ({
      Id: message,
      MessageBody: message
    }))
    await telemetry.trackDuration('sqs-publishes-batch', sqsClient.send(new SendMessageBatchCommand({ QueueUrl: queue, Entries: entries })))
  } catch (error) {
    logger.error({ error: serializeError(error), queue, messages }, 'Cannot send notification batch')
  }
}

/**
 * publish a message to a notification topic
 * @param topic {string} - topic to publish message to, e.g. an SNS ARN
 * @param message - message to send
 * @param {SNSClient} client - SNS client to issue command to
 * @returns Promise<void>
 */
async function notify({ client = snsClient, message, topic }) {
  const command = new PublishCommand({
    Message: (typeof message === 'string') ? message : JSON.stringify(message),
    TopicArn: topic
  })
  await client.send(command)
}

module.exports = {
  notify,
  publish,
  publishBatch
}
