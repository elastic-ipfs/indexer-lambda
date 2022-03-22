'use strict'

function generateEvent(...pairs) {
  return {
    Records: pairs.map(({ bucket, key }) => {
      return {
        body: `${bucket}/${key}`
      }
    })
  }
}

module.exports = {
  generateEvent
}
