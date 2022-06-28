'use strict'

const { readHeader, readBlockHead, asyncIterableReader } = require('@ipld/car/decoder')

const { RAW_BLOCK_CODEC } = require('../config')

class CarIterator {
  constructor(version, roots, reader, length, decodeBlocks) {
    this.version = version
    this.roots = roots
    this.reader = reader
    this.position = reader.pos
    this.length = length
    this.decodeBlocks = decodeBlocks
  }

  // eslint-disable-next-line generator-star-spacing
  async *[Symbol.asyncIterator]() {
    while ((await this.reader.upTo(8)).length > 0) {
      // Get the block information
      const offset = (this.position = this.reader.pos)
      const { cid, length, blockLength } = await readBlockHead(this.reader)
      this.currentCid = cid
      const data = this.decodeBlocks && cid.code !== RAW_BLOCK_CODEC ? await this.reader.exactly(blockLength) : undefined

      yield {
        cid,
        blockLength,
        blockOffset: this.reader.pos,
        offset,
        length,
        data
      }

      this.reader.seek(blockLength)
    }
  }
}

CarIterator.fromReader = async function fromReader(asyncIterable, length, decodeBlocks) {
  const reader = asyncIterableReader(asyncIterable)
  const { version, roots } = await readHeader(reader)

  return new CarIterator(version, roots, reader, length, decodeBlocks)
}

module.exports = { CarIterator, RAW_BLOCK_CODEC }
