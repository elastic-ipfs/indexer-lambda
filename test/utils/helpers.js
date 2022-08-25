'use strict'

function generateEvent(...pairs) {
  return {
    Records: pairs.map(({ bucketRegion, bucket, key }) => {
      return {
        body: `${bucketRegion}/${bucket}/${key}`,
        Attributes: { ApproximateReceiveCount: 0 }
      }
    })
  }
}

module.exports = {
  generateEvent
}
