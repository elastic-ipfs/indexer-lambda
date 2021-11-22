'use strict'

const { DynamoDBClient, GetItemCommand, ExecuteStatementCommand } = require('@aws-sdk/client-dynamodb')
const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const {
  marshall: serializeDynamoItem,
  unmarshall: deserializeDynamoItem,
  convertToAttr
} = require('@aws-sdk/util-dynamodb')
const { Agent } = require('https')

const { logger, serializeError } = require('./logging')

const dynamoClient = new DynamoDBClient({
  requestHandler: new NodeHttpHandler({ httpsAgent: new Agent({ keepAlive: true, keepAliveMsecs: 60000 }) })
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

async function writeDynamoItem(create, table, keyName, keyValue, data) {
  const expression = []
  const values = []
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

    statement = {
      Statement: `UPDATE ${table} ${expression.join(' ')} WHERE ${keyName} = ?`,
      Parameters: values
    }
  }

  try {
    await dynamoClient.send(new ExecuteStatementCommand(statement))
  } catch (e) {
    logger.error(
      { statement },
      `Cannot ${create ? 'insert item into' : 'update item in'}  DynamoDB table ${table}: ${serializeError(e)}`
    )

    throw e
  }
}

module.exports = {
  readDynamoItem,
  writeDynamoItem
}
