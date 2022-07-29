'use strict'

const { resolve } = require('path')

/* c8 ignore next */
require('dotenv').config({ path: process.env.ENV_FILE_PATH || resolve(process.cwd(), '.env') })

const {
  CONCURRENCY: rawConcurrency,

  DYNAMO_BLOCKS_TABLE: blocksTable,
  DYNAMO_CARS_TABLE: carsTable,
  DYNAMO_LINK_TABLE: linkTable,

  DYNAMO_MAX_RETRIES: dynamoMaxRetries,
  DYNAMO_RETRY_DELAY: dynamoRetryDelay,

  BLOCKS_BATCH_SIZE: blocksBatchSize,

  SNS_EVENTS_TOPIC: eventsTopic,

  SQS_NOTIFICATIONS_QUEUE_URL: notificationsQueue,
  SQS_PUBLISHING_QUEUE_URL: publishingQueue,

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
  blocksTable: blocksTable ?? 'v1-blocks',
  carsTable: carsTable ?? 'v1-cars',
  linkTable: linkTable ?? 'v1-blocks-cars-position',

  blocksTablePrimaryKey: 'multihash',
  carsTablePrimaryKey: 'path',
  linkTableBlockKey: 'blockmultihash',
  linkTableCarKey: 'carpath',

  codecs,
  now: process.env.NOW,

  /* c8 ignore next 10 */
  concurrency: !isNaN(concurrency) && concurrency > 0 ? concurrency : 32,
  // disbled https://github.com/elastic-ipfs/indexer-lambda/pull/54#discussion_r913665164
  // decodeBlocks: process.env.DECODE_BLOCKS === 'true', // decode CAR blocks
  notificationsQueue: notificationsQueue ?? 'notificationsQueue',
  publishingQueue: publishingQueue ?? 'publishingQueue',
  eventsTopic: eventsTopic ?? 'eventsTopic',
  skipPublishing: process.env.SKIP_PUBLISHING === 'true',
  skipDurations: process.env.SKIP_DURATIONS === 'true',

  blocksBatchSize: blocksBatchSize ? parseInt(blocksBatchSize, 10) : 10,

  dynamoMaxRetries: dynamoMaxRetries ?? 3,
  dynamoRetryDelay: dynamoRetryDelay ?? 100, // ms

  s3MaxRetries: s3MaxRetries ?? 3,
  s3RetryDelay: s3RetryDelay ?? 100 // ms
}
