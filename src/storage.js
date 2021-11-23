'use strict'

const {
  DynamoDBClient,
  GetItemCommand,
  ExecuteStatementCommand,
  DeleteItemCommand
} = require('@aws-sdk/client-dynamodb')
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const {
  marshall: serializeDynamoItem,
  unmarshall: deserializeDynamoItem,
  convertToAttr
} = require('@aws-sdk/util-dynamodb')
const { Agent } = require('https')

const { logger, serializeError } = require('./logging')

const agent = new Agent({ keepAlive: true, keepAliveMsecs: 60000 })

const dynamoClient = new DynamoDBClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})

const sqsClient = new SQSClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: agent })
})

async function readDynamoItem(table, keyName, keyValue) {
  try {
    const record = await dynamoClient.send(
      new GetItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) })
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
  const expression = []
  const values = []
  let conditionsExpression = []
  let statement

  if (create) {
    expression.push(`'${keyName}': ?`)
    values.push(convertToAttr(keyValue, { removeUndefinedValues: true }))

    for (const [key, value] of Object.entries(data)) {
      expression.push(`'${key}': ?`)
      values.push(convertToAttr(value, { removeUndefinedValues: true }))
    }

    statement = {
      Statement: `INSERT INTO ${table} VALUE {${expression.join(', ')}}`,
      Parameters: values
    }
  } else {
    for (const [key, value] of Object.entries(data)) {
      expression.push(`SET "${key}" = ?`)
      values.push(convertToAttr(value, { removeUndefinedValues: true }))
    }

    values.push(convertToAttr(keyValue, { removeUndefinedValues: true }))

    for (const [key, [operator, value]] of Object.entries(conditions)) {
      conditionsExpression.push(`"${key}" ${operator} ?`)
      values.push(convertToAttr(value, { removeUndefinedValues: true }))
    }

    if (conditionsExpression.length) {
      conditionsExpression = `AND ${conditionsExpression.join(' AND ')}`
    }

    statement = {
      Statement: `UPDATE ${table} ${expression.join(' ')} WHERE ${keyName} = ? ${conditionsExpression}`,
      Parameters: values
    }
  }

  try {
    await dynamoClient.send(new ExecuteStatementCommand(statement))
  } catch (e) {
    // Ignore condition failure errors in updates
    if (!create && e.name === 'ConditionalCheckFailedException') {
      return
    }

    logger.error(
      { statement },
      `Cannot ${create ? 'insert item into' : 'update item in'} DynamoDB table ${table}: ${serializeError(e)}`
    )

    throw e
  }
}

async function deleteDynamoItem(table, keyName, keyValue) {
  try {
    return await dynamoClient.send(
      new DeleteItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) })
    )
  } catch (e) {
    logger.error(`Cannot delete item from DynamoDB table ${table}: ${serializeError(e)}`)
    throw e
  }
}

async function publishToSQS(queue, data) {
  try {
    await sqsClient.send(new SendMessageCommand({ QueueUrl: queue, MessageBody: data }))
  } catch (e) {
    logger.error(`Cannot publish a block to ${queue}: ${serializeError(e)}`)

    throw e
  }
}

module.exports = {
  readDynamoItem,
  writeDynamoItem,
  deleteDynamoItem,
  publishToSQS
}
