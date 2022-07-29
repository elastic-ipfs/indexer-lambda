'use strict'

const { DynamoDBClient, GetItemCommand, PutItemCommand, BatchWriteItemCommand } = require('@aws-sdk/client-dynamodb')
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3')
const { marshall } = require('@aws-sdk/util-dynamodb')
const { SendMessageCommand, SendMessageBatchCommand, SQSClient } = require('@aws-sdk/client-sqs')
const { SNSClient, PublishCommand } = require('@aws-sdk/client-sns')
const { mockClient } = require('aws-sdk-client-mock')
const { readFileSync } = require('fs')
const { resolve } = require('path')
const { Readable } = require('stream')
const { setTimeout: sleep } = require('timers/promises')

const dynamoMock = mockClient(DynamoDBClient)
const s3Mock = mockClient(S3Client)
const sqsMock = mockClient(SQSClient)
const snsMock = mockClient(SNSClient)

function readMockData(file, from, to) {
  const buffer = readFileSync(resolve(process.cwd(), `test/fixtures/${file}`))

  return from && to ? buffer.slice(from, to + 1) : buffer
}

function readMockJSON(file, ...args) {
  return JSON.parse(readFileSync(resolve(process.cwd(), `test/fixtures/${file}`), 'utf-8'))
}

function mockDynamoGetItemCommand(table, keyName, keyValue, response) {
  const params = { TableName: table, Key: marshall({ [keyName]: keyValue }) }

  if (typeof response === 'function') {
    dynamoMock.on(GetItemCommand, params).callsFake(response)
  } else {
    dynamoMock.on(GetItemCommand, params).resolves({ Item: response })
  }
}

function mockS3GetObject(bucket, key, response, length, modified = Date.now(), index = 0) {
  s3Mock
    .on(GetObjectCommand, {
      Bucket: bucket,
      Key: key
    })
    // Recreate the stream every time in order to being able to return the same content multiple times
    .callsFake(async () => {
      // Introduce delays so that we simulate a bit of network latency and replies are in order
      await sleep(index * 50)

      if (typeof response === 'function') {
        response = await response()
      }

      return { Body: response ? Readable.from(response) : undefined, ContentLength: length, LastModified: new Date(modified) }
    })
}

function trackDynamoUsages(t) {
  t.context.dynamo = {
    creates: [],
    batchCreates: []
  }

  dynamoMock.on(PutItemCommand).callsFake(params => {
    t.context.dynamo.creates.push(params)
  })

  dynamoMock.on(BatchWriteItemCommand).callsFake(params => {
    t.context.dynamo.batchCreates.push(params)
  })
}

function trackSQSUsages(t) {
  t.context.sqs = {
    publishes: [],
    batchPublishes: []
  }

  sqsMock.on(SendMessageCommand).callsFake(params => {
    t.context.sqs.publishes.push(params)
  })
  sqsMock.on(SendMessageBatchCommand).callsFake(params => {
    t.context.sqs.batchPublishes.push(params)
  })
}

function trackSNSUsages(t) {
  t.context.sns = {
    publishes: []
  }

  snsMock.on(PublishCommand).callsFake(params => {
    t.context.sns.publishes.push(params)
  })
}

module.exports = {
  dynamoMock,
  s3Mock,
  snsMock,
  sqsMock,
  mockDynamoGetItemCommand,
  mockS3GetObject,
  trackDynamoUsages,
  trackSQSUsages,
  trackSNSUsages,
  readMockData,
  readMockJSON
}
