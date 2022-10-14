'use strict'

const { DynamoDBClient, GetItemCommand, PutItemCommand, BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { marshall, unmarshall } = require('@aws-sdk/util-dynamodb')
const { Agent } = require('https')
const { base58btc: base58 } = require('multiformats/bases/base58')
const { dynamoMaxRetries, dynamoRetryDelay } = require('../config')
const sleep = require('util').promisify(setTimeout)

const { serializeError } = require('./logging')
const telemetry = require('./telemetry')

const agent = new Agent({ keepAlive: true, keepAliveMsecs: 60000 })

const dynamoClient = new DynamoDBClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})

function cidToKey(cid) {
  return base58.encode(cid.multihash.bytes)
}

async function readDynamoItem({ table, keyName, keyValue, logger }) {
  const key = { [keyName]: keyValue }
  try {
    telemetry.increaseCount('dynamo-reads')

    const record = await telemetry.trackDuration(
      'dynamo-reads',
      sendCommand({
        client: dynamoClient,
        command: new GetItemCommand({
          TableName: table,
          Key: marshall(key)
        }),
        logger
      })
    )

    if (!record?.Item) {
      return null
    }

    return unmarshall(record.Item)
  } catch (error) {
    logger.error({ error: serializeError(error), table, key }, 'Cannot get item from DynamoDB')
    throw error
  }
}

async function createDynamoItem({ table, keyName, keyValue, data = {}, logger }) {
  const command = new PutItemCommand({
    TableName: table,
    Item: marshall({ [keyName]: keyValue, ...data }, { removeUndefinedValues: true })
  })

  try {
    telemetry.increaseCount('dynamo-creates')

    await telemetry.trackDuration('dynamo-creates', sendCommand({ client: dynamoClient, command, logger }))
  } catch (error) {
    logger.error({ table, command, error: serializeError(error) }, 'Cannot insert item into DynamoDB')
    throw error
  }
}

async function batchWriteDynamoItems({ batch, logger }) {
  try {
    telemetry.increaseCount('dynamo-batch-inserts')
    const request = composeBatchInsert(batch)
    const command = new BatchWriteItemCommand(request)
    const response = await telemetry.trackDuration('dynamo-batch-inserts', sendCommand({ client: dynamoClient, command, logger }))
    if (response?.UnprocessedItems && Object.keys(response.UnprocessedItems).length > 0) {
      logger.warn({ UnprocessedItems: JSON.stringify(response.UnprocessedItems) }, 'UnprocessedItems in BatchWriteItemCommand')
    }
    // todo retry if error https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.TransactionalErrors
  } catch (error) {
    logger.error({ error: serializeError(error), batch }, 'Cannot write batch items to DynamoDB table')
    throw error
  }
}

function composeBatchInsert(batch) {
  const request = { RequestItems: {} }
  for (const set of batch) {
    request.RequestItems[set.table] = set.items.map(item => ({ PutRequest: { Item: marshall(item, { removeUndefinedValues: true }) } }))
  }
  return request
}

async function sendCommand({ client, command, logger, retries = dynamoMaxRetries, retryDelay = dynamoRetryDelay }) {
  let attempts = 0
  let error
  do {
    try {
      return await client.send(command)
    } catch (err) {
      error = err
      logger.debug({ error: serializeError(err) }, `DynamoDB Error, attempt ${attempts + 1} / ${retries}`)
    }
    await sleep(retryDelay)
  } while (++attempts < retries)

  logger.error({ error: serializeError(error) }, `Cannot send command to DynamoDB after ${attempts} attempts`)
  throw new Error('Cannot send command to DynamoDB')
}

module.exports = {
  cidToKey,
  createDynamoItem,
  readDynamoItem,
  batchWriteDynamoItems
}
