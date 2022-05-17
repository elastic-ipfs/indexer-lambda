'use strict'

class PublisherInterface {
  constructor (options) {
    this.options = options
  }

  async send (queue, data) { throw new Error('publisher send method not implemented') }
}

module.exports = PublisherInterface
