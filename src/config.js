'use strict'

require('dotenv').config()

const { DYNAMO_BLOCKS_TABLE: blocksTable, DYNAMO_CARS_TABLE: carsTable } = process.env

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

module.exports = {
  blocksTable: blocksTable ?? 'blocks',
  carsTable: carsTable ?? 'cars',
  primaryKeys: {
    blocks: 'cid',
    cars: 'path'
  },
  codecs
}
