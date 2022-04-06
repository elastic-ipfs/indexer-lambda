'use strict'

process.env.ENV_FILE_PATH = 'dev/null'
process.env.DECODE_BLOCKS = 'true'
process.env.SKIP_PUBLISHING = 'true'

const { marshall: serializeDynamoItem } = require('@aws-sdk/util-dynamodb')
const t = require('tap')
const { mockDynamoGetItemCommand, mockS3GetObject, trackDynamoUsages, readMockData } = require('./utils/mock')
const { generateEvent } = require('./utils/helpers')
const { now } = require('../src/config')
const { handler } = require('../src/index')

t.test('indexing - can decode blocks', async t => {
  t.plan(6)

  mockS3GetObject('cars', 'file1.car', readMockData('cars/file1.car'), 148)
  mockDynamoGetItemCommand('cars', 'path', 'us-east-2/cars/file1.car', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmY13QWtykrcwmQmLVdxAQnJsRq7xBs5FAqH5zpG9ZvJpC', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn', undefined)

  trackDynamoUsages(t)
  await handler(generateEvent({ bucketRegion: 'us-east-2', bucket: 'cars', key: 'file1.car' }))

  t.strictSame(t.context.dynamo.creates[0], {
    TableName: 'cars',
    Item: serializeDynamoItem({
      path: 'us-east-2/cars/file1.car',
      bucket: 'cars',
      bucketRegion: 'us-east-2',
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
      cars: [{ car: 'us-east-2/cars/file1.car', offset: 96, length: 4 }],
      data: {}
    })
  })

  t.strictSame(t.context.dynamo.creates[2], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmSGtsqx7aYH8gP21AgidxXuX5vsseFJgHKa75kg8HepXL',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'us-east-2/cars/file1.car', offset: 137, length: 51 }],
      data: {
        Data: { type: 'directory' },
        Links: [{ Hash: 'bafkreiepr3vjk3iouugwiqx5vmqtgjxxlo3plbbgrmdzllkff6vilw27tu', Name: '111', Tsize: 4 }]
      }
    })
  })

  t.strictSame(t.context.dynamo.creates[3], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmSHc8o3PxQgMccYgGtuStaNQKXTBX1rTHN5W9cUCwrcHX',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'us-east-2/cars/file1.car', offset: 225, length: 51 }],
      data: {
        Data: { type: 'directory' },
        Links: [{ Hash: 'bafybeicpkdkuqpotuk4kq2auc76thx3zo7cfocotlwk4h7l42bdhsi7auq', Name: 'aaa', Tsize: 106 }]
      }
    })
  })

  t.strictSame(t.context.dynamo.creates[4], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmTgGQZ3ZcbcHxZiFNHs76Y7Ca8DfFGjdsxXDVnr41h339',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'us-east-2/cars/file1.car', offset: 313, length: 51 }],
      data: {
        Data: { type: 'directory' },
        Links: [{ Hash: 'bafybeib2pby3jnma7ha2gdcyudafcys6rvmdctw6oaukro2ppaqimf2l4m', Name: 'ccc', Tsize: 55 }]
      }
    })
  })

  t.strictSame(t.context.dynamo.creates[5], {
    TableName: 'blocks',
    Item: serializeDynamoItem({
      multihash: 'zQmUNLLsPACCz1vLxQVkXqqLX5R1X345qqfHbsf67hvA3Nn',
      type: 'dag-pb',
      createdAt: now,
      cars: [{ car: 'us-east-2/cars/file1.car', offset: 401, length: 4 }],
      data: {
        Data: { type: 'directory' },
        Links: []
      }
    })
  })
})

t.test('indexing - decoding should fail on unsupported blocks', async t => {
  t.plan(1)

  mockS3GetObject('cars', 'file2.car', readMockData('cars/file2.car'), 59)
  mockDynamoGetItemCommand('cars', 'path', 'us-east-2/cars/file2.car', undefined)
  mockDynamoGetItemCommand('blocks', 'multihash', 'zQmPH3Su9xAqw4WRbXT6DvwNpmaXYvTKKAY2hBKJsC7j2b4', undefined)

  trackDynamoUsages(t)
  await t.rejects(() => handler(generateEvent({ bucketRegion: 'us-east-2', bucket: 'cars', key: 'file2.car' })), {
    message: 'Unsupported codec 35 in the block at offset 96'
  })
})
