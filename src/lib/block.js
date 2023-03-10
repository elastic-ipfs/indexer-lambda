'use strict'

const { validateBlock } = require('@web3-storage/car-block-validator')

const config = require('../config')
const { queuedTasks } = require('./util')
const { serializeError } = require('./logging')
const { cidToKey, batchWriteDynamoItems } = require('./storage')
const { publishBatch } = require('./publish')

async function publishBlocks({ blocks, logger, queue = config.publishingQueue }) {
  await publishBatch({ queue, messages: blocks.map(b => b.key), logger })
}

function storeBlocksTaskGenerator({ blocks, car, logger }) {
  return async function storeBlocksTask() {
    await writeBlocksBatch({ blocks, car, logger })
    return { blocks, car, logger }
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
  const linkItems = []
  const keys = []
  // TODO: Validate all the blocks first
  for (let i = 0; i < blocks.length; i++) {
    const block = blocks[i]
    if (!block.key) {
      block.key = cidToKey(block.cid)
    }

    // Validate block
    try {
      await validateBlock({
        cid: block.cid,
        bytes: block.data
      })
    } catch (error) {
      logger.error({ error: serializeError(error) }, 'Error validating block')
      console.log('not valid block', error)
      continue
    }

    // HOTFIX
    if (keys.includes(block.key)) {
      continue
    }

    linkItems.push({
      [config.linkTableBlockKey]: block.key,
      [config.linkTableCarKey]: car.id,
      offset: block.blockOffset,
      length: block.blockLength
    })

    // HOTFIX
    keys.push(block.key)
  }

  const batch = [
    { table: config.linkTable, items: linkItems }
  ]

  await batchWriteDynamoItems({ batch, logger })
}

module.exports = {
  storeBlocks,
  writeBlocksBatch,
  publishBlocks
}
