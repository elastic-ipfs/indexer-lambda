'use strict'

const { marshall: serializeDynamoItem } = require('@aws-sdk/util-dynamodb')
const t = require('tap')
const {
  mockDynamoGetItemCommand,
  mockS3GetObject,
  trackDynamoUsages,
  trackSQSUsages,
  readMockJSON,
  readMockData
} = require('./utils/mock')
const { generateEvent } = require('./utils/helpers')
const { now, notificationsQueue, publishingQueue } = require('../src/config')
const { handler } = require('../src/index')

t.test('indexing - skip already parsed CAR files', async t => {
  t.plan(1)

  mockS3GetObject('cars', 'file1.car', () => {
    t.fail('Existing CAR has not been skipped.')
    return Buffer.alloc(0)
  })
  mockDynamoGetItemCommand('cars', 'path', 'cars/file1.car', readMockJSON('parsed-cars/file1-completed.json'))

  await handler(generateEvent({ bucket: 'cars', key: 'file1.car' }))

  t.pass('CAR has not been analyzed')
})

t.test('indexing - indexes a new car', async t => {
  t.plan(13)

  mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)
  mockDynamoGetItemCommand('cars', 'path', 'cars/file1.car', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn', undefined)

  trackDynamoUsages(t)
  trackSQSUsages(t)
  await handler(generateEvent({ bucket: 'cars', key: 'file1.car' }))

  t.strictSame(t.context.dynamo.creates[0], {
    TableName: 'cars',
    Item: serializeDynamoItem({
      path: 'cars/file1.car',
      bucket: 'cars',
      key: 'file1.car',
      createdAt: now,
      roots: ['bafybeib2u4mc4vsgpxp7fktmhpg5ncdviinwpfigagndcek43tde7uak2i'],
      version: 1,
      fileSize: 148,
      currentPosition: 59,
      completed: false
    })
  })

  t.strictSame(t.context.dynamo.creates[1], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC',
      type: 'raw',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 96, length: 4 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.creates[2], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 137, length: 51 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.creates[3], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 225, length: 51 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.creates[4], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 313, length: 51 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.creates[5], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 401, length: 4 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.updates.pop(), {
    TableName: 'cars',
    Key: {
      path: {
        S: 'cars/file1.car'
      }
    },
    ConditionExpression: undefined,
    UpdateExpression: 'SET currentPosition = :currentPosition, completed = :completed, durationTime = :durationTime',
    ExpressionAttributeValues: {
      ':currentPosition': {
        N: '148'
      },
      ':completed': {
        BOOL: true
      },
      ':durationTime': {
        N: '0'
      }
    }
  })

  t.strictSame(t.context.sqs.publishes[0], {
    QueueUrl: publishingQueue,
    MessageBody: 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC'
  })

  t.strictSame(t.context.sqs.publishes[1], {
    QueueUrl: publishingQueue,
    MessageBody: 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL'
  })

  t.strictSame(t.context.sqs.publishes[2], {
    QueueUrl: publishingQueue,
    MessageBody: 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX'
  })

  t.strictSame(t.context.sqs.publishes[3], {
    QueueUrl: publishingQueue,
    MessageBody: 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339'
  })

  t.strictSame(t.context.sqs.publishes[4], {
    QueueUrl: publishingQueue,
    MessageBody: 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn'
  })

  t.strictSame(t.context.sqs.publishes[5], {
    QueueUrl: notificationsQueue,
    MessageBody: 'cars/file1.car'
  })
})

t.test('indexing - indexes an already started CAR', async t => {
  t.plan(2)

  mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)
  mockDynamoGetItemCommand('cars', 'path', 'cars/file1.car', readMockJSON('parsed-cars/file1-partial.json'))
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC', () =>
    t.fail('Previous CID request')
  )
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL', () =>
    t.fail('Previous CID request')
  )
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX', () =>
    t.fail('Previous CID request')
  )
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn', undefined)

  trackDynamoUsages(t)
  await handler(generateEvent({ bucket: 'cars', key: 'file1.car' }))

  t.strictSame(t.context.dynamo.creates[0], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 313, length: 51 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.creates[1], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 401, length: 4 }],
      data: {}
    })
  })
})

t.test('indexing - can overwrite existing data', async t => {
  t.plan(1)

  mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)
  mockDynamoGetItemCommand('cars', 'path', 'cars/file1.car', undefined)
  mockDynamoGetItemCommand(
    'blocks',
    'multihash',
    'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC',
    serializeDynamoItem({
      multihash: 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC',
      type: 'raw',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 96, length: 4 }],
      data: {}
    })
  )
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn', undefined)

  trackDynamoUsages(t)
  await handler(generateEvent({ bucket: 'cars', key: 'file1.car' }))

  t.strictSame(t.context.dynamo.creates[1], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC',
      type: 'raw',
      createdAt: now,
      cars: [{ car: 'cars/file1.car', offset: 96, length: 4 }],
      data: {}
    })
  })
})

t.test('indexing - can append data to an existing CAR', async t => {
  t.plan(1)

  mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)
  mockDynamoGetItemCommand('cars', 'path', 'cars/file1.car', undefined)
  mockDynamoGetItemCommand(
    'blocks',
    'multihash',
    'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC',
    serializeDynamoItem({
      multihash: 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC',
      type: 'raw',
      createdAt: now,
      cars: [
        { car: 'cars/file2.car', offset: 96, length: 4 },
        { car: 'cars/file2.car', offset: 196, length: 4 }
      ],
      data: {}
    })
  )
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn', undefined)

  trackDynamoUsages(t)
  await handler(generateEvent({ bucket: 'cars', key: 'file1.car' }))

  t.strictSame(t.context.dynamo.updates[0], {
    TableName: 'blocks',
    Key: {
      multihash: {
        S: 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC'
      }
    },
    ConditionExpression: undefined,
    UpdateExpression: 'SET cars = :cars',
    ExpressionAttributeValues: {
      ':cars': {
        L: [
          {
            M: {
              car: {
                S: 'cars/file1.car'
              },
              offset: {
                N: '96'
              },
              length: {
                N: '4'
              }
            }
          },
          {
            M: {
              car: {
                S: 'cars/file2.car'
              },
              offset: {
                N: '96'
              },
              length: {
                N: '4'
              }
            }
          },
          {
            M: {
              car: {
                S: 'cars/file2.car'
              },
              offset: {
                N: '196'
              },
              length: {
                N: '4'
              }
            }
          }
        ]
      }
    }
  })
})

t.test('indexing - should not fail on unsupported blocks', async t => {
  t.plan(1)

  mockS3GetObject('cars', 'file2.car', readMockData('cars/file2.car'), 59)
  mockDynamoGetItemCommand('cars', 'path', 'cars/file2.car', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4', undefined)

  trackDynamoUsages(t)
  await handler(generateEvent({ bucket: 'cars', key: 'file2.car' }))

  t.strictSame(t.context.dynamo.creates[1], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4',
      type: 'unsupported',
      createdAt: now,
      cars: [{ car: 'cars/file2.car', offset: 96, length: 8 }],
      data: {}
    })
  })
})
