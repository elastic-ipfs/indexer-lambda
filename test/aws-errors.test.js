'use strict'

const t = require('tap')
const sinon = require('sinon')
const { Readable } = require('stream')
const telemetry = require('../src/telemetry')
const SourceAwsS3 = require('../src/source/aws-s3')
const StorageAwsDynamo = require('../src/storage/aws-dynamodb')
const PublisherAwsSqs = require('../src/publisher/aws-sqs')

const sandbox = sinon.createSandbox()
const dummyLogger = {
  warn: () => {},
  error: () => {}
}

t.beforeEach(() => {
  sandbox.spy(telemetry)
  sandbox.spy(dummyLogger)
})

t.afterEach(() => {
  sandbox.restore()
})

t.test('load - reports S3 errors', async t => {
  const source = new SourceAwsS3({
    httpsAgentKeepAlive: 1,
    telemetry,
    logger: dummyLogger
  })

  source.createClient('us-east-1', {
    send: async () => { throw new Error('FAILED') }
  })

  await t.rejects(() => source.load(new URL('s3://bucket/key'), 'us-east-1'), { message: 'FAILED' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('s3-fetchs'))
  t.ok(telemetry.trackDuration.calledOnceWith('s3-fetchs'))
})

t.test('load - reports invalid CARS', async t => {
  const source = new SourceAwsS3({
    httpsAgentKeepAlive: 1,
    telemetry,
    logger: dummyLogger
  })

  source.createClient('us-east-1', {
    send: async () => ({ Body: Readable.from(Buffer.from([0, 1, 2, 3])) })
  })

  await t.rejects(() => source.load(new URL('s3://bucket/key'), 'us-east-1'), { message: 'Invalid CAR header (zero length)' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('s3-fetchs'))
  t.ok(telemetry.trackDuration.calledOnceWith('s3-fetchs'))
})

t.test('readItem - reports DynamoDB errors', async t => {
  const storage = new StorageAwsDynamo({
    client: {
      send: async () => { throw new Error('FAILED') }
    },
    telemetry,
    logger: dummyLogger
  })

  await t.rejects(() => storage.readItem('TABLE', 'KEY', 'VALUE'), { message: 'FAILED' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('dynamo-reads'))
  t.ok(telemetry.trackDuration.calledOnceWith('dynamo-reads'))
})

t.test('writeItem - reports DynamoDB errors', async t => {
  const storage = new StorageAwsDynamo({
    client: {
      send: async () => { throw new Error('FAILED') }
    },
    telemetry,
    logger: dummyLogger
  })

  await t.rejects(() => storage.writeItem(true, 'TABLE', 'KEY', 'VALUE', {}), { message: 'FAILED' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('dynamo-creates'))
  t.ok(telemetry.trackDuration.calledOnceWith('dynamo-creates'))
})

t.test('storage.writeItem - reports DynamoDB errors', async t => {
  const storage = new StorageAwsDynamo({
    client: {
      send: async () => { throw new Error('FAILED') }
    },
    telemetry,
    logger: dummyLogger
  })

  await t.rejects(() => storage.writeItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'FAILED' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('dynamo-updates'))
  t.ok(telemetry.trackDuration.calledOnceWith('dynamo-updates'))
})

t.test('storage.writeItem - ignores DynamoDB precondition failures errors', async t => {
  const storage = new StorageAwsDynamo({
    client: {
      send: async () => {
        const error = new Error('FAILED')
        error.name = 'ConditionalCheckFailedException'
        throw error
      }
    },
    telemetry,
    logger: dummyLogger
  })

  await t.resolves(() => storage.writeItem(false, 'TABLE', 'KEY', 'VALUE', {}), { message: 'FAILED' })
  t.ok(dummyLogger.error.notCalled)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('dynamo-updates'))
  t.ok(telemetry.trackDuration.calledOnceWith('dynamo-updates'))
})

t.test('storage.deleteItem - reports DynamoDB errors', async t => {
  const storage = new StorageAwsDynamo({
    client: {
      send: async () => { throw new Error('FAILED') }
    },
    telemetry,
    logger: dummyLogger
  })

  await t.rejects(() => storage.deleteItem('TABLE', 'KEY', 'VALUE'), { message: 'FAILED' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('dynamo-deletes'))
  t.ok(telemetry.trackDuration.calledOnceWith('dynamo-deletes'))
})

t.test('publisher.send - reports SQS errors', async t => {
  const publisher = new PublisherAwsSqs({
    client: {
      send: async () => { throw new Error('FAILED') }
    },
    telemetry,
    logger: dummyLogger
  })

  await t.rejects(() => publisher.send('QUEUE', 'DATA'), { message: 'FAILED' })
  t.ok(dummyLogger.error.calledOnce)
  t.ok(telemetry.increaseCount.calledOnceWithExactly('sqs-publishes'))
  t.ok(telemetry.trackDuration.calledOnceWith('sqs-publishes'))
})
