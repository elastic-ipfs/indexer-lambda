'use strict'

function generateEvent(...pairs) {
  return {
    Records: pairs.map(({ bucket, key }) => {
      return {
        s3: { bucket: { name: bucket }, object: { key } }
      }
    })
  }
}

module.exports = {
  generateEvent
}
