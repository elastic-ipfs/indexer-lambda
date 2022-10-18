'use strict'

const t = require('tap')
const { BatchWriteItemCommand, GetItemCommand, PutItemCommand } = require('@aws-sdk/client-dynamodb')
const { batchWriteDynamoItems, createDynamoItem, readDynamoItem } = require('../src/lib/storage')
const { dynamoMock } = require('./utils/mock')

t.test('batchWriteDynamoItems', async t => {
  t.test('should call DynamoDBClient with "BatchWriteItemCommand"', async t => {
    t.plan(1)

    dynamoMock.on(BatchWriteItemCommand).callsFake(params => {
      t.same(params, {
        RequestItems: {
          table1: [{
            PutRequest: {
              Item: {
                block: { S: 'abc' },
                car: { S: '123' },
                offset: { N: '1' },
                length: { N: '2' }
              }
            }
          }],
          table2: [{ PutRequest: { Item: { block: { S: 'abc' }, type: { S: 'raw' } } } }]
        }
      })
    })

    const batch = [
      {
        table: 'table1',
        items: [{
          block: 'abc',
          car: '123',
          offset: 1,
          length: 2
        }]
      },
      {
        table: 'table2',
        items: [{
          block: 'abc',
          type: 'raw'
        }]
      }]

    await batchWriteDynamoItems({ batch })
  })

  t.test('should handle unprocessed items from batch write response', async t => {
    const unprocessedItems =
      { 'prod-ep-v1-blocks': [{ PutRequest: { Item: { type: { S: 'raw' }, createdAt: { S: '2022-10-16T20:34:14.465Z' }, multihash: { S: 'z2DrjgbByyopTuqrKjRhhSJWksbK3FcfxicDvBav7gPzoXDfWdf' } } } }] }

    const firstRequest = {
      table1: [{ PutRequest: { Item: { block: { S: 'abc' }, car: { S: '123' }, offset: { N: '1' }, length: { N: '2' } } } }],
      table2: [{ PutRequest: { Item: { block: { S: 'abc' }, type: { S: 'raw' } } } }]
    }

    dynamoMock.on(BatchWriteItemCommand, { RequestItems: firstRequest })
      .resolves({ UnprocessedItems: unprocessedItems })

    dynamoMock.on(BatchWriteItemCommand, { RequestItems: unprocessedItems })
      .resolves({})

    const messages = { debug: [], error: [], warn: [] }
    const loggerSpy = {
      debug: (_, message) => messages.debug.push(message),
      warn: (_, message) => messages.warn.push(message),
      error: (_, message) => messages.error.push(message)
    }
    const batch = [
      {
        table: 'table1',
        items: [{ block: 'abc', car: '123', offset: 1, length: 2 }]
      },
      {
        table: 'table2',
        items: [{ block: 'abc', type: 'raw' }]
      }]

    await batchWriteDynamoItems({ batch, logger: loggerSpy })

    t.equal(messages.debug.length, 0)
    t.equal(messages.error.length, 0)
    t.equal(messages.warn.length, 1)
    t.equal(messages.warn[0], 'UnprocessedItems in BatchWriteItemCommand')
  })

  t.test('should handle more unprocessed items from batch write response', async t => {
    const unprocessedItems1 =
      { 'prod-ep-v1-blocks': [{ PutRequest: { Item: { type: { S: 'raw' }, createdAt: { S: '2022-10-16T20:34:14.465Z' }, multihash: { S: 'hash1' } } } }] }
    const unprocessedItems2 =
      { 'prod-ep-v1-blocks': [{ PutRequest: { Item: { type: { S: 'raw' }, createdAt: { S: '2022-10-16T20:34:14.465Z' }, multihash: { S: 'hash2' } } } }] }

    const firstRequest = {
      table1: [{ PutRequest: { Item: { block: { S: 'abc' }, car: { S: '123' }, offset: { N: '1' }, length: { N: '2' } } } }],
      table2: [{ PutRequest: { Item: { block: { S: 'abc' }, type: { S: 'raw' } } } }]
    }

    dynamoMock.on(BatchWriteItemCommand, { RequestItems: firstRequest })
      .resolves({ UnprocessedItems: unprocessedItems1 })

    dynamoMock.on(BatchWriteItemCommand, { RequestItems: unprocessedItems1 })
      .resolves({ UnprocessedItems: unprocessedItems2 })

    dynamoMock.on(BatchWriteItemCommand, { RequestItems: unprocessedItems2 })
      .resolves({})

    const messages = { debug: [], error: [], warn: [] }
    const loggerSpy = {
      debug: (_, message) => messages.debug.push(message),
      warn: (_, message) => messages.warn.push(message),
      error: (_, message) => messages.error.push(message)
    }
    const batch = [
      {
        table: 'table1',
        items: [{ block: 'abc', car: '123', offset: 1, length: 2 }]
      },
      {
        table: 'table2',
        items: [{ block: 'abc', type: 'raw' }]
      }]

    await batchWriteDynamoItems({ batch, logger: loggerSpy })

    t.equal(messages.debug.length, 0)
    t.equal(messages.error.length, 0)
    t.equal(messages.warn.length, 2)
    t.equal(messages.warn[0], 'UnprocessedItems in BatchWriteItemCommand')
    t.equal(messages.warn[1], 'UnprocessedItems in BatchWriteItemCommand')
  })

  t.test('writeDynamoItem - reports DynamoDB errors', async t => {
    dynamoMock.on(BatchWriteItemCommand).rejects(new Error('FAILED'))
    const messages = { debug: [], error: [] }
    const loggerSpy = {
      debug: (_, message) => messages.debug.push(message),
      error: (_, message) => messages.error.push(message)
    }
    const batch = [
      {
        table: 'table1',
        items: [{
          block: 'abc',
          car: '123',
          offset: 1,
          length: 2
        }]
      },
      {
        table: 'table2',
        items: [{
          block: 'abc',
          type: 'raw'
        }]
      }]

    await t.rejects(() => batchWriteDynamoItems({ batch, logger: loggerSpy }), { message: 'Cannot send command to DynamoDB' })
    t.equal(messages.debug[0], 'DynamoDB Error, attempt 1 / 3')
    t.equal(messages.debug[1], 'DynamoDB Error, attempt 2 / 3')
    t.equal(messages.debug[2], 'DynamoDB Error, attempt 3 / 3')
    t.equal(messages.error[0], 'Cannot send command to DynamoDB after 3 attempts')
    t.equal(messages.error[1], 'Cannot write batch items to DynamoDB table')
  })
})

t.test('createDynamoItem', async t => {
  t.test('writeDynamoItem - reports DynamoDB errors', async t => {
    dynamoMock.on(PutItemCommand).rejects(new Error('FAILED'))
    const messages = { debug: [], error: [] }
    const loggerSpy = {
      debug: (_, message) => messages.debug.push(message),
      error: (_, message) => messages.error.push(message)
    }

    await t.rejects(() => createDynamoItem({ table: 'TABLE', keyName: 'KEY', keyValue: 'VALUE', logger: loggerSpy }), { message: 'Cannot send command to DynamoDB' })
    t.equal(messages.debug[0], 'DynamoDB Error, attempt 1 / 3')
    t.equal(messages.debug[1], 'DynamoDB Error, attempt 2 / 3')
    t.equal(messages.debug[2], 'DynamoDB Error, attempt 3 / 3')
    t.equal(messages.error[0], 'Cannot send command to DynamoDB after 3 attempts')
    t.equal(messages.error[1], 'Cannot insert item into DynamoDB')
  })
})

t.test('readDynamoItem', async t => {
  t.test('query non existing item', async t => {
    dynamoMock.on(GetItemCommand).callsFake(() => null)
    const dummyLogger = { debug: () => { }, error: () => { } }

    t.equal(await readDynamoItem({ table: 'TABLE', keyName: 'KEY', keyValue: 'VALUE', logger: dummyLogger }), null)
  })

  t.test('reports DynamoDB errors', async t => {
    dynamoMock.on(GetItemCommand).rejects(new Error('FAILED'))
    const messages = { debug: [], error: [] }
    const loggerSpy = {
      debug: (_, message) => messages.debug.push(message),
      error: (_, message) => messages.error.push(message)
    }

    await t.rejects(() => readDynamoItem({ table: 'TABLE', keyName: 'KEY', keyValue: 'VALUE', logger: loggerSpy }), { message: 'Cannot send command to DynamoDB' })
    t.equal(messages.debug[0], 'DynamoDB Error, attempt 1 / 3')
    t.equal(messages.debug[1], 'DynamoDB Error, attempt 2 / 3')
    t.equal(messages.debug[2], 'DynamoDB Error, attempt 3 / 3')
    t.equal(messages.error[0], 'Cannot send command to DynamoDB after 3 attempts')
    t.equal(messages.error[1], 'Cannot get item from DynamoDB')
  })
})
