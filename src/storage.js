'use strict'

const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  UpdateItemCommand
} = require('@aws-sdk/client-dynamodb')
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const {
  marshall: serializeDynamoItem,
  unmarshall: deserializeDynamoItem,
  convertToAttr
} = require('@aws-sdk/util-dynamodb')
const { Agent } = require('https')
const { base58btc: base58 } = require('multiformats/bases/base58')
const { dynamoMaxRetries, dynamoRetryDelay } = require('./config')
const sleep = require('util').promisify(setTimeout)

const { logger, serializeError } = require('./logging')
const telemetry = require('./telemetry')

const agent = new Agent({ keepAlive: true, keepAliveMsecs: 60000 })

const dynamoClient = new DynamoDBClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})

const sqsClient = new SQSClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})

function cidToKey(cid) {
  return base58.encode(cid.multihash.bytes)
}

async function readDynamoItem(table, keyName, keyValue) {
  try {
    telemetry.increaseCount('dynamo-reads')

    const record = await telemetry.trackDuration(
      'dynamo-reads',
      sendCommand(dynamoClient, new GetItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) }))
    )

    if (!record.Item) {
      return null
    }

    return deserializeDynamoItem(record.Item)
  } catch (e) {
    logger.error({ error: serializeError(e), item: { [keyName]: keyValue } }, `Cannot get item from DynamoDB table ${table}`)
    throw e
  }
}

async function writeDynamoItem(create, table, keyName, keyValue, data, conditions = {}) {
  let command

  if (create) {
    command = new PutItemCommand({
      TableName: table,
      Item: serializeDynamoItem({ [keyName]: keyValue, ...data }, { removeUndefinedValues: true })
    })
  } else {
    const update = []
    const values = {}
    const condition = []

    for (const [key, value] of Object.entries(data)) {
      update.push(`${key} = :${key}`)
      values[`:${key}`] = value
    }

    for (const [key, [operator, value]] of Object.entries(conditions)) {
      condition.push(`${key} ${operator} :${key}Condition`)
      values[`:${key}Condition`] = value
    }

    command = new UpdateItemCommand({
      TableName: table,
      Key: { [keyName]: convertToAttr(keyValue) },
      UpdateExpression: `SET ${update.join(', ')}`,
      ConditionExpression: condition.length ? condition.join(' AND ') : undefined,
      ExpressionAttributeValues: serializeDynamoItem(values, { removeUndefinedValues: true })
    })
  }

  try {
    telemetry.increaseCount(create ? 'dynamo-creates' : 'dynamo-updates')

    await telemetry.trackDuration(create ? 'dynamo-creates' : 'dynamo-updates', sendCommand(dynamoClient, command))
  } catch (e) {
    logger.error(
      { command, error: serializeError(e) },
      `Cannot ${create ? 'insert item into' : 'update item in'} DynamoDB table ${table}`
    )
    throw e
  }
}

async function deleteDynamoItem(table, keyName, keyValue) {
  try {
    telemetry.increaseCount('dynamo-deletes')

    await telemetry.trackDuration(
      'dynamo-deletes',
      sendCommand(dynamoClient, new DeleteItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) }))
    )
  } catch (e) {
    logger.error({ error: serializeError(e), item: { [keyName]: keyValue } }, `Cannot delete item from DynamoDB table ${table}`)
    throw e
  }
}

async function publishToSQS(queue, data) {
  try {
    telemetry.increaseCount('sqs-publishes')

    await telemetry.trackDuration(
      'sqs-publishes',
      sqsClient.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: data }))
    )
  } catch (e) {
    logger.error({ error: serializeError(e), data }, `Cannot publish a block to ${queue}`)

    throw e
  }
}

async function sendCommand(client, command, retries = dynamoMaxRetries, retryDelay = dynamoRetryDelay) {
  let attempts = 0
  let error
  do {
    try {
      return await client.send(command)
    } catch (err) {
      // Ignore condition failure errors in updates
      if (!(command instanceof PutItemCommand) && err.name === 'ConditionalCheckFailedException') {
        return
      }
      error = err
      logger.warn({ command, error: serializeError(err) }, `DynamoDB Error, attempt ${attempts + 1} / ${retries}`)
    }
    await sleep(retryDelay)
  } while (++attempts < retries)

  logger.error({ command, error: serializeError(error) }, `Cannot send command to DynamoDB after ${attempts} attempts`)
  throw new Error('Cannot send command to DynamoDB')
}

module.exports = {
  cidToKey,
  readDynamoItem,
  writeDynamoItem,
  deleteDynamoItem,
  publishToSQS
}
