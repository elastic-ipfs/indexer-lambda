'use strict'

const t = require('tap')
const { CarWriter } = require('@ipld/car')
const { CID } = require('multiformats/cid')
const raw = require('multiformats/codecs/raw')
const { sha256 } = require('multiformats/hashes/sha2')
const { concat } = require('uint8arrays/concat')

const config = require('../src/config')
const { handler } = require('../src/index')
const { Nanoseconds, NANOSECONDS_UNECE_UNIT_CODE } = require('../src/lib/time')
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
    const [publish] = t.context.sns.publishes
    assertIsIndexerCompletedPublish(t, publish)
    t.matchSnapshot({ dynamo: t.context.dynamo, sqs: t.context.sqs })
  })

  t.test('indexes a new car file but not invalid blocks', async t => {
    const bytes = new Uint8Array([1, 2, 3])
    const hash = await sha256.digest(bytes)
    const cid = CID.create(1, raw.code, hash)
    bytes[bytes.length - 1] = bytes[bytes.length - 1] + 1 // mangle a byte
    const { writer, out } = CarWriter.create([cid])
    writer.put({ cid, bytes })
    writer.close()

    const carParts = []
    for await (const part of out) {
      carParts.push(part)
    }
    const carBytes = concat(carParts)

    mockS3GetObject('cars', 'file3.car', Buffer.from(carBytes))
    mockDynamoGetItemCommand(config.carsTable, config.carsTablePrimaryKey, 'us-east-2/cars/file3.car', undefined)
    trackDynamoUsages(t)
    trackSQSUsages(t)

    await handler(helper.generateEvent({ bucketRegion: 'us-east-2', bucket: 'cars', key: 'file3.car' }))

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

    await handler({ Records: [{ body: JSON.stringify({ body: 'aws-region/bucket/car-file.car', skipExists: true }) }] })

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

  t.test('fails indexing a new car file decoding invalida body as json', async t => {
    mockS3GetObject('cars', 'file2.car', readMockData('cars/file2.car'), 148)

    await t.rejects(() => handler({ Records: [{ body: '{invalid-json' }] }),
      { message: 'Invalid JSON in event body: {invalid-json' })
  })
})

function assertIsIndexerCompletedPublish(t, snsPublish) {
  const event = JSON.parse(snsPublish.Message)
  assertIsIndexerCompletedEvent(t, event)
}

const ONE_HOUR_MILLISECONDS = 1000 * 60 * 60

function assertIsIndexerCompletedEvent(t, event, maxDurationMs = ONE_HOUR_MILLISECONDS) {
  t.equal(event.type, 'IndexerCompleted', 'event has type IndexerCompleted')
  t.ok(event.indexing, 'event has .indexing')
  for (const property of ['startTime', 'endTime']) {
    t.equal(typeof event.indexing[property], 'string', `${property} is a string`)
    t.equal(isNaN(Date.parse(event.indexing[property])), false, `${property} is parseable as a Date`)
  }
  if (event.indexing.startTime && event.indexing.endTime) {
    const toDate = (dateString) => new Date(Date.parse(dateString))
    t.equal(toDate(event.indexing.startTime) < toDate(event.indexing.endTime), true, 'startTime is before endTime')
    t.equal(toDate(event.indexing.endTime) - toDate(event.indexing.startTime) < maxDurationMs, true, 'endTime-startTime difference should be less than maxDurationMs')
  }
  t.equal(event.indexing.duration.unitCode, NANOSECONDS_UNECE_UNIT_CODE, 'indexing duration unitCode indicates nanoseconds')
  t.equal(typeof event.indexing.duration.value, 'string', 'indexing duration value is a string')
  const indexingDuration = Nanoseconds.fromJSON(event.indexing.duration)
  t.equal(indexingDuration.value > BigInt(0), true, 'indexing duration is greater than zero')
}
