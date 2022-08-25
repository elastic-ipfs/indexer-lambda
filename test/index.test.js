'use strict'

const t = require('tap')
const config = require('../src/config')
const { handler } = require('../src/index')
const helper = require('./utils/helpers')
const { mockDynamoGetItemCommand, mockS3GetObject, trackDynamoUsages, trackSQSUsages, readMockJSON, readMockData, trackSNSUsages } = require('./utils/mock')

t.test('handler', async t => {
  t.test('indexes a new car file', async t => {
    mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)
    mockDynamoGetItemCommand(config.carsTable, config.carsTablePrimaryKey, 'us-east-2/cars/file1.car', undefined)
    trackDynamoUsages(t)
    trackSQSUsages(t)
    trackSNSUsages(t)

    await handler(helper.generateEvent({ bucketRegion: 'us-east-2', bucket: 'cars', key: 'file1.car' }))
    t.equal(t.context.sns.publishes.length, 1)
    t.ok(t.context.sns.publishes.some(p => p.Message.includes('IndexerCompleted')), 'handler published IndexerCompleted event to SNS')
    t.matchSnapshot({ dynamo: t.context.dynamo, sqs: t.context.sqs })
  })

  t.test('indexes a new car file with unsupported blocks', async t => {
    mockS3GetObject('cars', 'file2.car', readMockData('cars/file2.car'), 148)
    mockDynamoGetItemCommand(config.carsTable, config.carsTablePrimaryKey, 'us-east-2/cars/file2.car', undefined)
    trackDynamoUsages(t)
    trackSQSUsages(t)

    await handler(helper.generateEvent({ bucketRegion: 'us-east-2', bucket: 'cars', key: 'file2.car' }))

    t.matchSnapshot({ dynamo: t.context.dynamo, sqs: t.context.sqs })
  })

  t.test('skip already parsed CAR files', async t => {
    mockDynamoGetItemCommand(config.carsTable, config.carsTablePrimaryKey, 'aws-region/bucket/car-file.car', readMockJSON('parsed-cars/file1.json'))
    trackDynamoUsages(t)
    trackSQSUsages(t)

    // TODO: Also user helper for keeping consistency
    await handler({ Records: [{ body: JSON.stringify({ body: 'aws-region/bucket/car-file.car', skipExists: true }), Attributes: { ApproximateReceiveCount: 0 } }] })

    t.same(t.context.dynamo.creates.length, 0)
    t.same(t.context.dynamo.batchCreates.length, 0)
    t.equal(t.context.sqs.batchPublishes.length, 0)
    t.equal(t.context.sqs.publishes.length, 0)
  })

  t.test('fails because of event has too many car files', async t => {
    await t.rejects(() => handler({ Records: [{ body: 'aws-region/bucket/car-file.car' }, { body: 'aws-region/bucket/car-file.car' }] }),
      { message: 'Indexer Lambda invoked with 2 CARs while should be 1' })
  })

  t.test('fails because of event has no car files', async t => {
    await t.rejects(() => handler({ Records: [] }),
      { message: 'Indexer Lambda invoked with 0 CARs while should be 1' })
  })

  t.test('fails because of invalid car file', async t => {
    await t.rejects(() => handler({ Records: [{ body: 'not-a-car-file' }] }),
      { message: 'Invalid CAR file format' })
  })

  // disabled decodeBlocks https://github.com/elastic-ipfs/indexer-lambda/pull/54#discussion_r913665164
  // t.test('fails indexing a new car file decoding unsupported blocks', async t => {
  //   process.env.DECODE_BLOCKS = 'true'
  //   mockS3GetObject('cars', 'file2.car', readMockData('cars/file2.car'), 148)

  //   await t.rejects(() => handler({ Records: [{ body: JSON.stringify({ body: 'us-east-2/cars/file2.car' }) }] }),
  //     { message: 'Unsupported codec 35 in the block at offset 96' })
  // })

  t.test('fails indexing a new car file decoding invalida body as json', async t => {
    mockS3GetObject('cars', 'file2.car', readMockData('cars/file2.car'), 148)

    await t.rejects(() => handler({ Records: [{ body: '{invalid-json' }] }),
      { message: 'Invalid JSON in event body: {invalid-json' })
  })
})
