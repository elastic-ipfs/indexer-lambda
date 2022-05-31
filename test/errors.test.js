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
const { s3Mock, dynamoMock, sqsMock } = require('./utils/mock')

const sandbox = sinon.createSandbox()

t.beforeEach(() => {
  sandbox.spy(logger)
})

t.afterEach(() => {
  sandbox.restore()
})

t.test('openS3Stream - reports S3 errors', async t => {
  t.plan(1)

  s3Mock.on(GetObjectCommand).rejects(new Error('FAILED'))

  await t.rejects(() => openS3Stream("us-east-1", new URL('s3://bucket/key')), { message: 'FAILED' })
})

t.test('openS3Stream - reports invalid CARS', async t => {
  t.plan(1)

  s3Mock.on(GetObjectCommand).resolves({ Body: Readable.from(Buffer.from([0, 1, 2, 3])) })

  await t.rejects(() => openS3Stream("us-east-1", new URL('s3://bucket/key')), { message: 'Invalid CAR header (zero length)' })
})

t.test('readDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(GetItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => readDynamoItem('TABLE', 'KEY', 'VALUE'), { message: 'Cannot send command to DynamoDB' })
  t.ok(logger.error.calledWith('Cannot send command to DynamoDB after 3 attempts'))
  t.equal(logger.error.getCalls().length, 4)
})

t.test('writeDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(PutItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => writeDynamoItem(true, 'TABLE', 'KEY', 'VALUE', {}), { message: 'Cannot send command to DynamoDB' })
  t.ok(logger.error.calledWith('Cannot send command to DynamoDB after 3 attempts'))
  t.equal(logger.error.getCalls().length, 4)
})

t.test('writeDynamoItem - reports DynamoDB errors', async t => {
  dynamoMock.on(UpdateItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => writeDynamoItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'Cannot send command to DynamoDB' })
  t.ok(logger.error.calledWith('Cannot send command to DynamoDB after 3 attempts'))
  t.equal(logger.error.getCalls().length, 4)
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
  t.ok(logger.error.calledWith('Cannot send command to DynamoDB after 3 attempts'))
  t.equal(logger.error.getCalls().length, 4)
})

t.test('publishToSQS - reports SQS errors', async t => {
  t.plan(1)

  sqsMock.on(SendMessageCommand).rejects(new Error('FAILED'))

  await t.rejects(() => publishToSQS('QUEUE', 'DATA'), { message: 'FAILED' })
})
