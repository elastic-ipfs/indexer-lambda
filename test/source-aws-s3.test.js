'use strict'

const t = require('tap')
const SourceAwsS3 = require('../src/source/aws-s3')

t.test('source aws-s3', async t => {
  t.test('options', async t => {
    t.test('shold get error missing telemetry', async t => {
      t.throws(() => { new SourceAwsS3({ telemetry: null }) }, 'telemetry instance is required')
    })
    t.test('shold get error missing logger', async t => {
      t.throws(() => { new SourceAwsS3({ telemetry: {}, logger: null }) }, 'logger instance is required')
    })
    t.test('shold get error missing httpsAgentKeepAlive', async t => {
      t.throws(() => { new SourceAwsS3({ telemetry: {}, logger: {} }) }, 'httpsAgentKeepAlive instance is required')
    })
  })
})
