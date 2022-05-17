'use strict'

const t = require('tap')
const PublisherAwsSqs = require('../src/publisher/aws-sqs')

t.test('publisher aws-sqs', async t => {
  t.test('options', async t => {
    t.test('shold get error missing telemetry', async t => {
      t.throws(() => { new PublisherAwsSqs({ telemetry: null }) }, 'telemetry instance is required')
    })
    t.test('shold get error missing logger', async t => {
      t.throws(() => { new PublisherAwsSqs({ telemetry: {}, logger: null }) }, 'logger instance is required')
    })
    t.test('shold get error missing client and agent and httpsAgentKeepAlive', async t => {
      t.throws(() => { new PublisherAwsSqs({ telemetry: {}, logger: {} }) }, 'one of client instance, agent instance or httpsAgentKeepAlive is required')
    })
    t.test('shold create the client passing agent instance', async t => {
      const p = new PublisherAwsSqs({ telemetry: {}, logger: {}, agent: {} })
      t.ok(p.client)
    })
    t.test('shold create the client passing httpsAgentKeepAlive value', async t => {
      const p = new PublisherAwsSqs({ telemetry: {}, logger: {}, httpsAgentKeepAlive: 1000 })
      t.ok(p.client)
    })
  })
})
