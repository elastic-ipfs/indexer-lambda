'use strict'

const { base58btc: base58 } = require('multiformats/bases/base58')

function cidToKey(cid) {
  return base58.encode(cid.multihash.bytes)
}

module.exports = {
  cidToKey
}
