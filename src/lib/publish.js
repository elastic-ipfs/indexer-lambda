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
 * @param {object} arg
 * @param {string} arg.topic - topic to publish message to, e.g. an SNS ARN
 * @param {string} arg.message - message to send
 * @param {SNSClient} [arg.client] - SNS client to issue command to
 * @param {Logger} logger - logger to use to log errors et al
 * @returns {Promise<void>}
 */
async function notify({ client = snsClient, message, topic, logger }) {
  telemetry.increaseCount('sns-publishes')
  const send = async () => {
    await client.send(new PublishCommand({
      Message: message,
      TopicArn: topic
    }))
  }
  try {
    await telemetry.trackDuration('sns-publishes', send())
  } catch (error) {
    logger.error({ error: serializeError(error), topic, message }, 'Cannot notify topic of message')
  }
}

module.exports = {
  notify,
  publish,
  publishBatch
}
