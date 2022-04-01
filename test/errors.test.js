'use strict'

const { GetItemCommand, PutItemCommand, UpdateItemCommand, DeleteItemCommand } = require('@aws-sdk/client-dynamodb')
const { GetObjectCommand } = require('@aws-sdk/client-s3')
const { Readable } = require('stream')
const t = require('tap')
const { readDynamoItem, writeDynamoItem, deleteDynamoItem, publishToSQS } = require('../src/storage')
const { openS3Stream } = require('../src/source')
const { s3Mock, dynamoMock, sqsMock } = require('./utils/mock')
const { SendMessageCommand } = require('@aws-sdk/client-sqs')

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
  t.plan(1)

  dynamoMock.on(GetItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => readDynamoItem('TABLE', 'KEY', 'VALUE'), { message: 'FAILED' })
})

t.test('writeDynamoItem - reports DynamoDB errors', async t => {
  t.plan(1)

  dynamoMock.on(PutItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => writeDynamoItem(true, 'TABLE', 'KEY', 'VALUE', {}), { message: 'FAILED' })
})

t.test('writeDynamoItem - reports DynamoDB errors', async t => {
  t.plan(1)

  dynamoMock.on(UpdateItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => writeDynamoItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'FAILED' })
})

t.test('writeDynamoItem - ignores DynamoDB precondition failures errors', async t => {
  t.plan(1)

  const error = new Error('FAILED')
  error.name = 'ConditionalCheckFailedException'
  dynamoMock.on(UpdateItemCommand).rejects(error)

  await t.resolves(() => writeDynamoItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'FAILED' })
})

t.test('deleteDynamoItem - reports DynamoDB errors', async t => {
  t.plan(1)

  dynamoMock.on(DeleteItemCommand).rejects(new Error('FAILED'))

  await t.rejects(() => deleteDynamoItem('TABLE', 'KEY', 'VALUE'), { message: 'FAILED' })
})

t.test('publishToSQS - reports SQS errors', async t => {
  t.plan(1)

  sqsMock.on(SendMessageCommand).rejects(new Error('FAILED'))

  await t.rejects(() => publishToSQS('QUEUE', 'DATA'), { message: 'FAILED' })
})
