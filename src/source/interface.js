'use strict'

class PublisherInterface {
  constructor (options) {
    this.options = options
  }

  async load (url, region) { throw new Error('source load method not implemented') }
}

module.exports = PublisherInterface
