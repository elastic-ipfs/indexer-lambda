'use strict'

const { NodeHttpHandler } = require('@aws-sdk/node-http-handler')
const { Agent } = require('https')
const {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  UpdateItemCommand
} = require('@aws-sdk/client-dynamodb')
const {
  marshall: serializeDynamoItem,
  unmarshall: deserializeDynamoItem,
  convertToAttr
} = require('@aws-sdk/util-dynamodb')

const StorageInterface = require('./interface')
const { serializeError } = require('../logging')

class StorageAwsDynamo extends StorageInterface {
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

    this.client = options.client ?? new DynamoDBClient({
      requestHandler: new NodeHttpHandler({
        httpsAgent: options.agent ?? new Agent({ keepAlive: true, keepAliveMsecs: options.httpsAgentKeepAlive })
      })
    })
    this.telemetry = options.telemetry
    this.logger = options.logger
  }

  async readItem(table, keyName, keyValue) {
    try {
      this.telemetry.increaseCount('dynamo-reads')

      const record = await this.telemetry.trackDuration(
        'dynamo-reads',
        this.client.send(new GetItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) }))
      )

      if (!record.Item) {
        return null
      }

      return deserializeDynamoItem(record.Item)
    } catch (err) {
      this.logger.error(`Cannot get item from DynamoDB table ${table}: ${serializeError(err)}`)
      throw err
    }
  }

  async writeItem(create, table, keyName, keyValue, data, conditions = {}) {
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
      this.telemetry.increaseCount(create ? 'dynamo-creates' : 'dynamo-updates')

      await this.telemetry.trackDuration(create ? 'dynamo-creates' : 'dynamo-updates', this.client.send(command))
    } catch (err) {
      // Ignore condition failure errors in updates
      if (!create && err.name === 'ConditionalCheckFailedException') {
        return
      }

      this.logger.error(
        { command },
        `Cannot ${create ? 'insert item into' : 'update item in'} DynamoDB table ${table}: ${serializeError(err)}`
      )

      throw err
    }
  }

  async deleteItem(table, keyName, keyValue) {
    try {
      this.telemetry.increaseCount('dynamo-deletes')

      await this.telemetry.trackDuration(
        'dynamo-deletes',
        this.client.send(new DeleteItemCommand({ TableName: table, Key: serializeDynamoItem({ [keyName]: keyValue }) }))
      )
    } catch (err) {
      this.logger.error(`Cannot delete item from DynamoDB table ${table}: ${serializeError(err)}`)
      throw err
    }
  }
}

module.exports = StorageAwsDynamo
