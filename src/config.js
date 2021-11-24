'use strict'

require('dotenv').config()

const {
  CONCURRENCY: rawConcurrency,
  DYNAMO_BLOCKS_TABLE: blocksTable,
  DYNAMO_CARS_TABLE: carsTable,
  SQS_PUBLISHING_QUEUE: publishingQueue
} = process.env

// Load all supported codecs
const supportedCodes = {
  json: 'multiformats/codecs/json',
  'dag-cbor': '@ipld/dag-cbor',
  'dag-pb': '@ipld/dag-pb',
  'dag-jose': 'dag-jose'
}

const codecs = Object.entries(supportedCodes).reduce((accu, [label, mod]) => {
  const { decode, code } = require(mod)

  accu[code] = { decode, label }
  return accu
}, {})

const concurrency = parseInt(rawConcurrency)

module.exports = {
  concurrency: !isNaN(concurrency) && concurrency > 0 ? concurrency : 16,
  blocksTable: blocksTable ?? 'blocks',
  carsTable: carsTable ?? 'cars',
  publishingQueue: publishingQueue ?? 'publishingQueue',
  primaryKeys: {
    blocks: 'multihash',
    cars: 'path'
  },
  codecs
}
