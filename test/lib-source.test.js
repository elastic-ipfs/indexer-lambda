'use strict'

const t = require('tap')
const { Readable } = require('stream')
const { GetObjectCommand } = require('@aws-sdk/client-s3')
const { s3Mock, mockS3GetObject, readMockData } = require('./utils/mock')
const { openS3Stream } = require('../src/lib/source')

t.test('openS3Stream', async t => {
  t.test('succeed', async t => {
    const length = 148
    const modified = new Date('2022-06-13T10:04:00.044Z').getTime()
    mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), length, modified)
    const loggerSpy = {
      error: () => { t.fail('no errors') }
    }

    const { stats } = await openS3Stream({ bucketRegion: 'us-east-1', url: new URL('s3://cars/file1.car'), logger: loggerSpy })
    t.equal(stats.contentLength, length)
    t.equal(stats.lastModified, modified)
  })

  t.test('reports S3 errors', async t => {
    t.plan(3)

    const debugMessages = []
    s3Mock.on(GetObjectCommand).rejects(new Error('FAILED'))
    const loggerSpy = {
      error: (_, message) => { t.equal(message, 'Cannot open file S3 URL s3://bucket/key after 3 attempts') },
      debug: (message) => { debugMessages.push(message) }
    }

    await t.rejects(() => openS3Stream({ bucketRegion: 'us-east-1', url: new URL('s3://bucket/key'), logger: loggerSpy, retries: 3, retryDelay: 10 }), { message: 'FAILED' })
    t.same(debugMessages, [
      'S3 Error, URL: s3://bucket/key Error: "FAILED" attempt 1 / 3',
      'S3 Error, URL: s3://bucket/key Error: "FAILED" attempt 2 / 3',
      'S3 Error, URL: s3://bucket/key Error: "FAILED" attempt 3 / 3'])
  })

  t.test('S3 url does not exists', async t => {
    t.plan(2)

    const err = new Error('FAILED')
    err.code = 'NoSuchKey'
    s3Mock.on(GetObjectCommand).rejects(err)
    const loggerSpy = {
      debug: () => { t.fail('no debugs') },
      error: (_, message) => { t.equal(message, 'Cannot open file S3 URL s3://bucket/key, does not exists') }
    }

    await t.rejects(() => openS3Stream({ bucketRegion: 'us-east-1', url: new URL('s3://bucket/key'), logger: loggerSpy, retries: 3, retryDelay: 10 }), { message: 'FAILED' })
  })

  t.test('reports invalid CARS', async t => {
    t.plan(2)

    s3Mock.on(GetObjectCommand).resolves({ Body: Readable.from(Buffer.from([0, 1, 2, 3])) })
    const loggerSpy = {
      debug: () => { t.fail('no debugs') },
      error: (_, message) => { t.equal(message, 'Cannot parse file s3://bucket/key as CAR') }
    }

    await t.rejects(() => openS3Stream({ bucketRegion: 'us-east-1', url: new URL('s3://bucket/key'), logger: loggerSpy, retries: 2, retryDelay: 10 }), { message: 'Invalid CAR header (zero length)' })
  })
})
