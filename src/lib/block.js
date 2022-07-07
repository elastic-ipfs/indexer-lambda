'use strict'

const {
  UnixFS: { unmarshal: decodeUnixFs }
} = require('ipfs-unixfs')
const config = require('../config')
const { now, queuedTasks } = require('./util')
const { serializeError } = require('./logging')
const { cidToKey, batchWriteDynamoItems } = require('./storage')
const { publishBatch } = require('./publish')

function blockType(blockId, codecs = config.codecs) {
  return codecs[blockId.code]?.label ?? 'unsupported'
}

function blockData(block) {
  if (!block.data) {
    return
  }

  const data = decodeBlock(block)
  if (data.Links) {
    for (const link of data.Links) {
      link.Hash = link.Hash?.toString()
    }
  }

  return data
}

function decodeBlock(block, codecs = config.codecs) {
  const codec = codecs[block.cid.code]

  if (!codec) {
    throw new Error(`Unsupported codec ${block.cid.code} in the block at offset ${block.blockOffset}`)
  }

  const data = codec.decode(block.data)

  if (codec.label.startsWith('dag')) {
    const { type, blocks } = decodeUnixFs(data.Data)

    data.Data = { type, blocks }
  }

  return data
}

async function publishBlocks({ blocks, logger, queue = config.publishingQueue }) {
  await publishBatch({ queue, messages: blocks.map(b => b.key), logger })
}

function storeBlocksTaskGenerator({ blocks, car, logger }) {
  return async function storeBlocksTask() {
    await writeBlocksBatch({ blocks, car, logger })
    return { blocks, car }
  }
}

async function storeBlocks({ car, source, logger, batchSize = config.blocksBatchSize, concurrency = config.concurrency, onTaskComplete }) {
  let count = 0

  const writes = queuedTasks({ concurrency, onTaskComplete })

  try {
    let batch = []
    for await (const block of source) {
      if (batch.length === batchSize) {
        // the return of storeBlocksTask will be passed to publishBlocks
        writes.add(storeBlocksTaskGenerator({ blocks: batch, car, logger }))
        batch = []
      }

      batch.push(block)
      count++
    }

    // process remaning blocks in the last batch
    if (batch.length > 0) {
      writes.add(storeBlocksTaskGenerator({ blocks: batch, car, logger }))
    }
  } catch (error) {
    logger.error({ error: serializeError(error) }, 'Error queuing block tasks')
  }

  const result = await writes.done()
  if (result.error) {
    logger.error({ error: serializeError(result.error) }, 'Error writing blocks')
    throw result.error
  }

  return { count }
}

/**
 * batch insert on blocks and link table
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.BatchOperations
 */
async function writeBlocksBatch({ blocks, car, logger }) {
  const blockItems = []
  const linkItems = []
  for (let i = 0; i < blocks.length; i++) {
    const block = blocks[i]
    if (!block.key) {
      block.key = cidToKey(block.cid)
    }

    blockItems.push({
      [config.blocksTablePrimaryKey]: block.key,
      type: blockType(block.cid),
      createdAt: now(),
      data: blockData(block)
    })

    linkItems.push({
      [config.linkTableBlockKey]: block.key,
      [config.linkTableCarKey]: car.id,
      offset: block.blockOffset,
      length: block.blockLength
    })
  }

  const batch = [
    { table: config.blocksTable, items: blockItems },
    { table: config.linkTable, items: linkItems }
  ]

  await batchWriteDynamoItems({ batch, logger })
}

module.exports = {
  storeBlocks,
  writeBlocksBatch,
  publishBlocks
}
