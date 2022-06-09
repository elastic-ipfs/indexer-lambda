'use strict'

const t = require('tap')
const sinon = require('sinon')
const { Readable } = require('stream')
const { GetItemCommand, PutItemCommand, UpdateItemCommand, DeleteItemCommand } = require('@aws-sdk/client-dynamodb')
const { GetObjectCommand } = require('@aws-sdk/client-s3')
const { SendMessageCommand } = require('@aws-sdk/client-sqs')
const { readDynamoItem, writeDynamoItem, deleteDynamoItem, publishToSQS } = require('../src/storage')
const { openS3Stream } = require('../src/source')
const { logger } = require('../src/logging')
const { s3Mock, dynamoMock, sqsMock, mockS3GetObject, readMockData } = require('./utils/mock')

const sandbox = sinon.createSandbox()

t.beforeEach(() => {
  sandbox.spy(logger)
})

t.afterEach(() => {
  sandbox.restore()
})

t.test('openS3Stream - succeed', async t => {
  mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)

  await openS3Stream("us-east-1", new URL('s3://cars/file1.car'), 3, 10)
  t.equal(logger.warn.getCalls().length, 0)
  t.equal(logger.error.getCalls().length, 0)
})

t.test('openS3Stream - reports S3 errors', async t => {
  s3Mock.on(GetObjectCommand).rejects(new Error('FAILED'))

  await t.rejects(() => openS3Stream("us-east-1", new URL('s3://bucket/key'), 3, 10), { message: 'FAILED' })
  t.equal(logger.warn.getCall(0).lastArg, 'S3 Error, URL: s3://bucket/key Error: "FAILED" attempt 1 / 3')
  t.equal(logger.warn.getCall(1).lastArg, 'S3 Error, URL: s3://bucket/key Error: "FAILED" attempt 2 / 3')
  t.equal(logger.warn.getCall(2).lastArg, 'S3 Error, URL: s3://bucket/key Error: "FAILED" attempt 3 / 3')
  t.equal(logger.error.getCall(0).lastArg, 'Cannot open file S3 URL s3://bucket/key after 3 attempts')
})

t.test('openS3Stream - S3 url does not exists', async t => {
  const err = new Error('FAILED')
  err.code = 'NoSuchKey'
  s3Mock.on(GetObjectCommand).rejects(err)

  await t.rejects(() => openS3Stream("us-east-1", new URL('s3://bucket/key'), 3, 10), { message: 'FAILED' })
  t.equal(logger.warn.getCalls().length, 0)
  t.equal(logger.error.getCall(0).lastArg, 'Cannot open file S3 URL s3://bucket/key, does not exists')
})

t.test('openS3Stream - reports invalid CARS', async t => {
  s3Mock.on(GetObjectCommand).resolves({ Body: Readable.from(Buffer.from([0, 1, 2, 3])) })

  await t.rejects(() => openS3Stream("us-east-1", new URL('s3://bucket/key'), 2, 10), { message: 'Invalid CAR header (zero length)' })
  t.equal(logger.warn.getCalls().length, 0)
  t.equal(logger.error.getCall(0).lastArg, 'Cannot parse file s3://bucket/key as CAR')
})

t.test('readDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(GetItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => readDynamoItem('TABLE', 'KEY', 'VALUE'), { message: 'Cannot send command to DynamoDB' })
  t.equal(logger.warn.getCall(0).lastArg, 'DynamoDB Error, attempt 1 / 3')
  t.equal(logger.warn.getCall(1).lastArg, 'DynamoDB Error, attempt 2 / 3')
  t.equal(logger.warn.getCall(2).lastArg, 'DynamoDB Error, attempt 3 / 3')
  t.equal(logger.error.getCall(0).lastArg, 'Cannot send command to DynamoDB after 3 attempts')
  t.equal(logger.error.getCall(1).lastArg, 'Cannot get item from DynamoDB table TABLE')
})

t.test('writeDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(PutItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => writeDynamoItem(true, 'TABLE', 'KEY', 'VALUE', {}), { message: 'Cannot send command to DynamoDB' })
  t.equal(logger.warn.getCall(0).lastArg, 'DynamoDB Error, attempt 1 / 3')
  t.equal(logger.warn.getCall(1).lastArg, 'DynamoDB Error, attempt 2 / 3')
  t.equal(logger.warn.getCall(2).lastArg, 'DynamoDB Error, attempt 3 / 3')
  t.equal(logger.error.getCall(0).lastArg, 'Cannot send command to DynamoDB after 3 attempts')
  t.equal(logger.error.getCall(1).lastArg, 'Cannot insert item into DynamoDB table TABLE')
})

t.test('writeDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(UpdateItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => writeDynamoItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'Cannot send command to DynamoDB' })
  t.equal(logger.warn.getCall(0).lastArg, 'DynamoDB Error, attempt 1 / 3')
  t.equal(logger.warn.getCall(1).lastArg, 'DynamoDB Error, attempt 2 / 3')
  t.equal(logger.warn.getCall(2).lastArg, 'DynamoDB Error, attempt 3 / 3')
  t.equal(logger.error.getCall(0).lastArg, 'Cannot send command to DynamoDB after 3 attempts')
  t.equal(logger.error.getCall(1).lastArg, 'Cannot update item in DynamoDB table TABLE')
})

t.test('writeDynamoItem - ignores DynamoDB precondition failures errors', async t => {
  const error = new Error('DYNAMO_ERROR')
  error.name = 'ConditionalCheckFailedException'
  dynamoMock.on(UpdateItemCommand).rejects(error)

  await t.resolves(() => writeDynamoItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'DYNAMO_ERROR' })
  t.equal(logger.error.getCalls().length, 0)
})

t.test('deleteDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(DeleteItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => deleteDynamoItem('TABLE', 'KEY', 'VALUE'), { message: 'Cannot send command to DynamoDB' })
  t.equal(logger.warn.getCall(0).lastArg, 'DynamoDB Error, attempt 1 / 3')
  t.equal(logger.warn.getCall(1).lastArg, 'DynamoDB Error, attempt 2 / 3')
  t.equal(logger.warn.getCall(2).lastArg, 'DynamoDB Error, attempt 3 / 3')
  t.equal(logger.error.getCall(0).lastArg, 'Cannot send command to DynamoDB after 3 attempts')
  t.equal(logger.error.getCall(1).lastArg, 'Cannot delete item from DynamoDB table TABLE')
})

t.test('publishToSQS - reports SQS errors', async t => {
  t.plan(1)

  sqsMock.on(SendMessageCommand).rejects(new Error('FAILED'))

  await t.rejects(() => publishToSQS('QUEUE', 'DATA'), { message: 'FAILED' })
})
