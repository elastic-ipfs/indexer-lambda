// https://unece.org/fileadmin/DAM/cefact/recommendations/rec20/rec20_rev3_Annex3e.pdf
const NANOSECONDS_UNECE_UNIT_CODE = 'C47'

/**
 * @typedef JsonNanoseconds
 * @property {'C47'} unitCode - https://schema.org/unitCode https://unece.org/fileadmin/DAM/cefact/recommendations/rec20/rec20_rev3_Annex3e.pdf
 * @property {string} value
 */

class Nanoseconds {
  /**
     * @param {bigint} value
     */
  constructor(value) {
    this.value = value
  }

  /**
     * @returns {JsonNanoseconds}
     */
  toJSON() {
    return {
      unitCode: NANOSECONDS_UNECE_UNIT_CODE,
      value: this.value.toString()
    }
  }

  /**
     * @param {JsonNanoseconds} arg
     */
  static fromJSON(arg) {
    return new Nanoseconds(BigInt(arg.value))
  }
}

module.exports = {
  Nanoseconds,
  NANOSECONDS_UNECE_UNIT_CODE
}
