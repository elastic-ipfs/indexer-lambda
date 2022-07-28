'use strict'

const t = require('tap')
const { SendMessageCommand, SendMessageBatchCommand } = require('@aws-sdk/client-sqs')
const { serializeError } = require('../src/lib/logging')
const { publish, publishBatch, notify } = require('../src/lib/publish')
const { sqsMock } = require('./utils/mock')

t.test('publish', async t => {
  t.test('should call SQSClient with "SendMessageCommand"', async t => {
    t.plan(1)

    sqsMock.on(SendMessageCommand).callsFake(params => {
      t.same(params, { QueueUrl: 'the-queue', MessageBody: 'the-message' })
    })

    await publish({ queue: 'the-queue', message: 'the-message' })
  })

  t.test('should log on error', async t => {
    t.plan(1)

    const message = 'the-message'
    const queue = 'the-queue'
    const error = new Error('GENERIC_ERROR')
    sqsMock.on(SendMessageCommand).callsFake(params => { throw error })
    const loggerSpy = {
      error: (...args) => {
        t.same(args, [{ error: serializeError(error), queue, message }, 'Cannot send notification'])
      }
    }

    await publish({ queue, message, logger: loggerSpy })
  })
})

t.test('publishBatch', async t => {
  t.test('should call SQSClient with "SendMessageBatchCommand"', async t => {
    t.plan(1)

    const messages = ['message1', 'message2', 'message3']

    sqsMock.on(SendMessageBatchCommand).callsFake(params => {
      t.same(params, {
        QueueUrl: 'the-queue',
        Entries: [
          { Id: 'message1', MessageBody: 'message1' },
          { Id: 'message2', MessageBody: 'message2' },
          { Id: 'message3', MessageBody: 'message3' }
        ]
      })
    })

    await publishBatch({ queue: 'the-queue', messages })
  })

  t.test('should log on error', async t => {
    t.plan(1)

    const messages = ['message1', 'message2', 'message3']
    const queue = 'the-queue'
    const error = new Error('GENERIC_ERROR')
    sqsMock.on(SendMessageBatchCommand).callsFake(params => {
      throw error
    })
    const loggerSpy = {
      error: (...args) => {
        t.same(args, [{ error: serializeError(error), queue, messages }, 'Cannot send notification batch'])
      }
    }

    await publishBatch({ queue, messages, logger: loggerSpy })
  })
})

t.test('notify', async t => {
  t.test('calls SNSClient with "SendMessageCommand"', async t => {
    const topic = `topic-${Math.random().toString().slice(2)}`
    const message = JSON.stringify({ name: `message-${topic} ` })
    const sentCommands = []
    const fakeClient = {
      send(command) {
        sentCommands.push(command)
        return Promise.resolve()
      }
    }
    await notify({ client: fakeClient, message, topic })
    t.equal(sentCommands.length, 1)
    const [command] = sentCommands
    t.equal(command.input.Message, message)
    t.equal(command.input.TopicArn, topic)
  })
})
