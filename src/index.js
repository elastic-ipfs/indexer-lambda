'use strict'

const { forEach } = require('hwp')
const {
  UnixFS: { unmarshal: decodeUnixFs }
} = require('ipfs-unixfs')

const {
  now,
  blocksTable,
  carsTable,
  concurrency,
  codecs,
  primaryKeys,
  publishingQueue,
  skipPublishing,
  skipDurations
} = require('./config')
const { logger, elapsed, serializeError } = require('./logging')
const { openS3Stream } = require('./source')
const { readDynamoItem, writeDynamoItem, deleteDynamoItem, publishToSQS, cidToKey } = require('./storage')
const telemetry = require('./telemetry')

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
    /* c8 ignore next */
    createdAt: now || new Date().toISOString(),
    cars: [{ car, offset: block.blockOffset, length: block.blockLength }],
    data
  })

  if (skipPublishing) {
    return
  }

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
  try {
    const start = process.hrtime.bigint()

    // For each Record in the event
    let currentCar = 0
    const totalCars = event.Records.length

    for (const record of event.Records) {
      const partialStart = process.hrtime.bigint()
      const [, bucket, key] = record.body.match(/([^/]+)\/(.+)/)

      const carUrl = new URL(`s3://${bucket}/${key}`)
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
          bucket,
          key,
          /* c8 ignore next */
          createdAt: now || new Date().toISOString(),
          roots: Array.from(new Set(indexer.roots.map(r => r.toString()))),
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
            const allCars = new Set(existingBlock.cars.map(c => c.car))

            /*
            If the block has only one CAR and it is the current car,
            it means the same file is somehow analyzed again.
            Delete the old information in order to have a clean state.
          */
            if (allCars.size === 1 && allCars.has(carId)) {
              await deleteDynamoItem(blocksTable, primaryKeys.blocks, cidToKey(block.cid))
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
        durationTime: skipDurations ? 0 : elapsed(partialStart)
      })

      telemetry.flush()
    }

    // Return a empty object to signal we have consumed all the messages
    return {}
  } catch (e) {
    logger.error(`Cannot index a CAR file: ${serializeError(e)}`)

    throw e
    /* c8 ignore next */
  } finally {
    telemetry.flush()
  }
}

exports.handler = main
