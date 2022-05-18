import fs from 'fs/promises'

export class Logger {
  constructor () {
    this._filePath = `results-${new Date().toISOString()}.log.ndjson`
  }

  /**
   * @param {string} peer
   * @param {string} cid
   * @param {number} size
   * @param {number} time
   */
  async logResult (peer, cid, size, time) {
    await fs.appendFile(this._filePath, `${JSON.stringify({ peer, cid, size, time })}\n`)
  }
}
