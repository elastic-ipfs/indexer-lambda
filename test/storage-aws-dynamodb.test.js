'use strict'

const t = require('tap')
const StorageAwsDynamo = require('../src/storage/aws-dynamodb')

t.test('storage aws-dynamo', async t => {
  t.test('options', async t => {
    t.test('shold get error missing telemetry', async t => {
      t.throws(() => { new StorageAwsDynamo({ telemetry: null }) }, 'telemetry instance is required')
    })
    t.test('shold get error missing logger', async t => {
      t.throws(() => { new StorageAwsDynamo({ telemetry: {}, logger: null }) }, 'logger instance is required')
    })
    t.test('shold get error missing client and agent and httpsAgentKeepAlive', async t => {
      t.throws(() => { new StorageAwsDynamo({ telemetry: {}, logger: {} }) }, 'one of client instance, agent instance or httpsAgentKeepAlive is required')
    })
    t.test('shold create the client passing agent instance', async t => {
      const p = new StorageAwsDynamo({ telemetry: {}, logger: {}, agent: {} })
      t.ok(p.client)
    })
    t.test('shold create the client passing httpsAgentKeepAlive value', async t => {
      const p = new StorageAwsDynamo({ telemetry: {}, logger: {}, httpsAgentKeepAlive: 1000 })
      t.ok(p.client)
    })
  })
})
