'use strict'

const { forEach } = require('hwp')
const {
  UnixFS: { unmarshal: decodeUnixFs }
} = require('ipfs-unixfs')

const { concurrency, codecs, blocksTable, primaryKeys, carsTable, publishingQueue } = require('./config')
const { logger, elapsed } = require('./logging')
const { openS3Stream } = require('./source')
const { readDynamoItem, writeDynamoItem, deleteDynamoItem, publishToSQS, cidToKey } = require('./storage')

function decodeBlock(block) {
  const codec = codecs[block.cid.code]

  if (!codec) {
    logger.error(`Unsupported codec ${block.cid.code} in the block at offset ${block.blockOffset}`)
    throw new Error(`Unsupported codec ${block.cid.code} in the block at offset ${block.blockOffset}`)
  }

  // Decoded the block data
  const data = codec.decode(block.data)

  if (codec.label.startsWith('dag')) {
    const { type, blocks } = decodeUnixFs(data.Data)

    data.Data = { type, blocks }
  }

  return data
}

async function updateCarStatus(carId, block) {
  const currentPosition = block.offset + block.length

  /*
    The condition is needed if another block, next to this one,
    has been evaluated first. This can happen due to hwp induced parallelism.
  */
  return writeDynamoItem(
    false,
    carsTable,
    'path',
    carId,
    {
      currentPosition
    },
    {
      currentPosition: ['<', currentPosition]
    }
  )
}

async function storeNewBlock(car, type, block, data = {}) {
  for (const link of data?.Links ?? []) {
    link.Hash = link.Hash.toString()
  }

  const cid = cidToKey(block.cid)

  await writeDynamoItem(true, blocksTable, primaryKeys.blocks, cid, {
    type,
    createdAt: new Date().toISOString(),
    cars: [{ car, offset: block.blockOffset, length: block.blockLength }],
    data
  })

  return publishToSQS(publishingQueue, cid)
}

async function appendCarToBlock(block, cars, carId) {
  cars.push({ car: carId, offset: block.blockOffset, length: block.blockLength })

  return writeDynamoItem(false, blocksTable, primaryKeys.blocks, cidToKey(block.cid), {
    cars: cars.sort((a, b) => {
      return a.offset !== b.offset ? a.offset - b.offset : a.car.localeCompare(b.car)
    })
  })
}

async function main(event) {
  const start = process.hrtime.bigint()

  // For each Record in the event
  let currentCar = 0
  const totalCars = event.Records.length

  for (const record of event.Records) {
    const partialStart = process.hrtime.bigint()

    const carUrl = new URL(`s3://${record.s3.bucket.name}/${record.s3.object.key}`)
    const carId = carUrl.toString().replace('s3://', '')

    currentCar++

    // Check if the CAR exists and it has been already analyzed
    const existingCar = await readDynamoItem(carsTable, primaryKeys.cars, carId)

    if (existingCar?.completed) {
      logger.info(
        { elapsed: elapsed(start), progress: { records: { current: currentCar, total: totalCars } } },
        `Skipping CAR ${carUrl} (${currentCar} of ${totalCars}), as it has already been analyzed.`
      )

      continue
    }

    // Show event progress
    logger.info(
      { elapsed: elapsed(start), progress: { records: { current: currentCar, total: totalCars } } },
      `Analyzing CAR ${currentCar} of ${totalCars} with concurrency ${concurrency}: ${carUrl}`
    )

    // Load the file from input
    const indexer = await openS3Stream(carUrl)

    // If the CAR is existing and not completed, just move the stream to the last analyzed block
    if (existingCar) {
      indexer.reader.seek(existingCar.currentPosition - indexer.reader.pos)
    } else {
      // Store the initial information of the CAR
      await writeDynamoItem(true, carsTable, primaryKeys.cars, carId, {
        bucket: record.s3.bucket.name,
        key: record.s3.object.key,
        createdAt: new Date().toISOString(),
        roots: new Set(indexer.roots.map(r => r.toString())),
        version: indexer.version,
        fileSize: indexer.length,
        currentPosition: indexer.reader.pos,
        completed: false
      })
    }

    // For each block in the indexer (which holds the start and end block)
    await forEach(
      indexer,
      async function (block) {
        // Show CAR progress
        logger.debug(
          {
            elapsed: elapsed(start),
            progress: {
              records: { current: currentCar, total: totalCars },
              car: { position: indexer.position, length: indexer.length }
            }
          },
          `Analyzing CID ${block.cid}`
        )

        // If the block is already in the storage, fetch it and then just update CAR informations
        const existingBlock = await readDynamoItem(blocksTable, primaryKeys.blocks, cidToKey(block.cid))
        if (existingBlock) {
          /*
          If the block has only one CAR and it is the current car,
          it means the same file is somehow analyzed again.
          Delete the old information in order to have a clean state.
        */
          const allCars = new Set(existingBlock.cars.map(c => c.car))

          if (allCars.size === 1 && allCars.has(carId)) {
            await deleteDynamoItem(blocksTable, primaryKeys.blocks, cidToKey(block.cid))
            await updateCarStatus(carId, block)
          } else {
            await appendCarToBlock(block, existingBlock.cars, carId)
            await updateCarStatus(carId, block)
            return
          }
        }

        /*
          Note that when DECODE_BLOCKS env variable is unset
          block.data is always undefined and therefore no decoding is performed.
        */
        await storeNewBlock(
          carId,
          codecs[block.cid.code]?.label ?? 'unsupported',
          block,
          block.data ? decodeBlock(block) : undefined
        )

        // Update the information about the CAR analysis progress
        await updateCarStatus(carId, block)
      },
      concurrency
    )

    // Mark the CAR as completed
    await writeDynamoItem(false, carsTable, 'path', carId, {
      currentPosition: indexer.length,
      completed: true,
      durationTime: elapsed(partialStart)
    })
  }
}

exports.handler = main
