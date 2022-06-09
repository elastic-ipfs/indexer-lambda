'use strict'

const { resolve } = require('path')

/* c8 ignore next */
require('dotenv').config({ path: process.env.ENV_FILE_PATH || resolve(process.cwd(), '.env') })

const {
  CONCURRENCY: rawConcurrency,
  DYNAMO_BLOCKS_TABLE: blocksTable,
  DYNAMO_CARS_TABLE: carsTable,
  SQS_NOTIFICATIONS_QUEUE_URL: notificationsQueue,
  SQS_PUBLISHING_QUEUE_URL: publishingQueue,

  DYNAMO_MAX_RETRIES: dynamoMaxRetries,
  DYNAMO_RETRY_DELAY: dynamoRetryDelay,

  S3_MAX_RETRIES: s3MaxRetries,
  S3_RETRY_DELAY: s3RetryDelay
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
  { [RAW_BLOCK_CODEC]: { label: 'raw' } }
)

const concurrency = parseInt(rawConcurrency)

module.exports = {
  RAW_BLOCK_CODEC,
  blocksTable: blocksTable ?? 'blocks',
  carsTable: carsTable ?? 'cars',
  codecs,
  concurrency: !isNaN(concurrency) && concurrency > 0 ? concurrency : 16,
  decodeBlocks: process.env.DECODE_BLOCKS === 'true',
  notificationsQueue: notificationsQueue ?? 'notificationsQueue',
  now: process.env.NOW,
  primaryKeys: {
    blocks: 'multihash',
    cars: 'path'
  },
  publishingQueue: publishingQueue ?? 'publishingQueue',
  skipPublishing: process.env.SKIP_PUBLISHING === 'true',
  skipDurations: process.env.SKIP_DURATIONS === 'true',

  dynamoMaxRetries: dynamoMaxRetries ?? 3,
  dynamoRetryDelay: dynamoRetryDelay ?? 100, // ms

  s3MaxRetries: s3MaxRetries ?? 3,
  s3RetryDelay: s3RetryDelay ?? 100 // ms
}
