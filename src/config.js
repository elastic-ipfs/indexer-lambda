'use strict'

require('dotenv').config()

const {
  CONCURRENCY: rawConcurrency,
  DYNAMO_BLOCKS_TABLE: blocksTable,
  DYNAMO_CARS_TABLE: carsTable,
  SQS_PUBLISHING_QUEUE_URL: publishingQueue
} = process.env

// Load all supported codecs
const RAW_BLOCK_CODEC = 0x55

const supportedCodes = {
  json: 'multiformats/codecs/json',
  'dag-cbor': '@ipld/dag-cbor',
  'dag-pb': '@ipld/dag-pb',
  'dag-jose': 'dag-jose'
}

const codecs = Object.entries(supportedCodes).reduce(
  (accu, [label, mod]) => {
    const { decode, code } = require(mod)

    accu[code] = { decode, label }
    return accu
  },
  { [RAW_BLOCK_CODEC]: { label: 'raw', decode: d => d } }
)

const concurrency = parseInt(rawConcurrency)

module.exports = {
  RAW_BLOCK_CODEC,
  blocksTable: blocksTable ?? 'blocks',
  carsTable: carsTable ?? 'cars',
  codecs,
  concurrency: !isNaN(concurrency) && concurrency > 0 ? concurrency : 16,
  decodeBlocks: process.env.DECODE_BLOCKS === 'true',
  primaryKeys: {
    blocks: 'multihash',
    cars: 'path'
  },
  publishingQueue: publishingQueue ?? 'publishingQueue',
  skipPublishing: process.env.SKIP_PUBLISHING === 'true'
}
