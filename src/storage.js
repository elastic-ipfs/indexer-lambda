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

const { logger, serializeError } = require('./logging')
const { metrics, trackDuration } = require('./telemetry')

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
    metrics.dynamoReads.add(1)

    const record = await trackDuration(
      metrics.dynamoReadsDurations,
      dynamoClient.send(new GetItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) }))
    )

    if (!record.Item) {
      return null
    }

    return deserializeDynamoItem(record.Item)
  } catch (e) {
    logger.error(`Cannot get item from DynamoDB table ${table}: ${serializeError(e)}`)
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
    metrics[create ? 'dynamoCreates' : 'dynamoUpdates'].add(1)

    await trackDuration(
      metrics[create ? 'dynamoCreatesDurations' : 'dynamoUpdatesDurations'],
      dynamoClient.send(command)
    )
  } catch (e) {
    // Ignore condition failure errors in updates
    if (!create && e.name === 'ConditionalCheckFailedException') {
      return
    }

    logger.error(
      { command },
      `Cannot ${create ? 'insert item into' : 'update item in'} DynamoDB table ${table}: ${serializeError(e)}`
    )

    throw e
  }
}

async function deleteDynamoItem(table, keyName, keyValue) {
  try {
    metrics.dynamoDeletes.add(1)

    return await trackDuration(
      metrics.dynamoDeletesDurations,
      dynamoClient.send(new DeleteItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) }))
    )
  } catch (e) {
    logger.error(`Cannot delete item from DynamoDB table ${table}: ${serializeError(e)}`)
    throw e
  }
}

async function publishToSQS(queue, data) {
  try {
    metrics.sqsPublishes.add(1)

    await trackDuration(
      metrics.sqsPublishesDurations,
      sqsClient.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: data }))
    )
  } catch (e) {
    logger.error(`Cannot publish a block to ${queue}: ${serializeError(e)}`)

    throw e
  }
}

module.exports = {
  cidToKey,
  readDynamoItem,
  writeDynamoItem,
  deleteDynamoItem,
  publishToSQS
}
