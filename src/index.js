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
  notificationsQueue,
  primaryKeys,
  publishingQueue,
  skipPublishing,
  skipDurations
} = require('./config')
const { logger, elapsed, serializeError } = require('./logging')
const { openS3Stream } = require('./source')
const { readDynamoItem, writeDynamoItem, publishToSQS, cidToKey } = require('./storage')
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

async function main(event) {
  let currentCarFile
  try {
    const start = process.hrtime.bigint()
    
    // For each Record in the event
    const totalCars = event.Records.length
    let currentCarIndex = 0 

    for (const record of event.Records) {
      currentCarFile = record.body
      const partialStart = process.hrtime.bigint()
      const [, bucketRegion, bucketName, key] = currentCarFile.match(/([^/]+)\/([^/]+)\/(.+)/)

      const carUrl = new URL(`s3://${bucketName}/${key}`)
      const carId = `${bucketRegion}/${carUrl.toString().replace('s3://', '')}`

      currentCarIndex++

      // Check if the CAR exists and it has been already analyzed
      const existingCar = await readDynamoItem(carsTable, primaryKeys.cars, carId)

      if (existingCar?.completed) {
        logger.info(
          { elapsed: elapsed(start), progress: { records: { current: currentCarIndex, total: totalCars } } },
          `Skipping CAR ${carUrl} (${currentCarIndex} of ${totalCars}), as it has already been analyzed.`
        )

        continue
      }

      // Show event progress
      logger.info(
        { elapsed: elapsed(start), progress: { records: { current: currentCarIndex, total: totalCars } } },
        `Analyzing CAR ${currentCarIndex} of ${totalCars} with concurrency ${concurrency}: ${carUrl}`
      )

      // Load the file from input
      const indexer = await openS3Stream(bucketRegion, carUrl)

      // If the CAR is existing and not completed, just move the stream to the last analyzed block
      if (existingCar) {
        indexer.reader.seek(existingCar.currentPosition - indexer.reader.pos)
      } else {
        // Store the initial information of the CAR
        await writeDynamoItem(true, carsTable, primaryKeys.cars, carId, {
          bucket: bucketName,
          bucketRegion: bucketRegion,
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
                records: { current: currentCarIndex, total: totalCars },
                car: { position: indexer.position, length: indexer.length }
              }
            },
            `Analyzing CID ${block.cid}`
          )

          const existingBlock = await readDynamoItem(blocksTable, primaryKeys.blocks, cidToKey(block.cid))
          if (existingBlock) {
            return
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

      // Mark the CAR as completed and notify to SQS
      await writeDynamoItem(false, carsTable, 'path', carId, {
        currentPosition: indexer.length,
        completed: true,
        durationTime: skipDurations ? 0 : elapsed(partialStart)
      })

      await publishToSQS(notificationsQueue, currentCarFile)

      telemetry.flush()
    }

    // Return a empty object to signal we have consumed all the messages
    return {}
  } catch (e) {
    logger.error({ error: serializeError(e), car: currentCarFile }, 'Cannot index the CAR file')

    throw e
    /* c8 ignore next */
  } finally {
    telemetry.flush()
  }
}

exports.handler = main
