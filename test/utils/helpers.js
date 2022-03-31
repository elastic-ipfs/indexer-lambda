'use strict'

function generateEvent(...pairs) {
  return {
    Records: pairs.map(({ bucketRegion, bucket, key }) => {
      return {
        body: `${bucketRegion}/${bucket}/${key}`
      }
    })
  }
}

module.exports = {
  generateEvent
}
