/* global AbortController */
import { parse } from 'it-ndjson'

const ENDPOINT = 'http://127.0.0.1:5001'
const TIMEOUT = 30000

export class IpfsClient {
  constructor (endpoint = ENDPOINT) {
    this._endpoint = endpoint
  }

  async id (options = {}) {
    const res = await withTimeout(signal => (
      fetch(new URL('api/v0/id', this._endpoint), { method: 'POST', signal })
    ), options.timeout)
    if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
    return res.json()
  }

  async swarmPeers (options = {}) {
    const res = await withTimeout(signal => (
      fetch(new URL('api/v0/swarm/peers', this._endpoint), { method: 'POST', signal })
    ), options.timeout)
    if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
    return res.json()
  }

  async swarmConnect (addr, options = {}) {
    const res = await withTimeout(signal => {
      const url = new URL('api/v0/swarm/connect', this._endpoint)
      url.searchParams.set('arg', addr)
      return fetch(url, { method: 'POST', signal })
    }, options.timeout)
    if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
    return res.json()
  }

  async swarmDisconnect (addr, options = {}) {
    const res = await withTimeout(signal => {
      const url = new URL('api/v0/swarm/disconnect', this._endpoint)
      url.searchParams.set('arg', addr)
      return fetch(url, { method: 'POST', signal })
    }, options.timeout)
    if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
    return res.json()
  }

  async blockGet (cid, options = {}) {
    const res = await withTimeout(signal => {
      const url = new URL('api/v0/block/get', this._endpoint)
      url.searchParams.set('arg', cid)
      return fetch(url, { method: 'POST', signal })
    }, options.timeout)
    if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
    return res.arrayBuffer()
  }

  async objectStat (cid, options = {}) {
    const res = await withTimeout(signal => {
      const url = new URL('api/v0/object/stat', this._endpoint)
      url.searchParams.set('arg', cid)
      return fetch(url, { method: 'POST', signal })
    }, options.timeout)
    if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
    return res.json()
  }

  /**
   * @returns {AsyncGenerator<Uint8Array>}
   */
  async * dagExport (cid, options = {}) {
    const endpoint = this._endpoint
    yield * withChunkTimeout(async function * (signal) {
      const url = new URL('api/v0/dag/export', endpoint)
      url.searchParams.set('arg', cid)
      const res = await fetch(url, { method: 'POST', signal })
      if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
      yield * res.body
    }, options.timeout)
  }

  /**
   * @returns {AsyncGenerator<{ Key: { '/': string }, Error?: string }>}
   */
  async * repoGc (options = {}) {
    const endpoint = this._endpoint
    yield * withChunkTimeout(async function * (signal) {
      const url = new URL('api/v0/repo/gc', endpoint)
      const res = await fetch(url, { method: 'POST', signal })
      if (!res.ok) throw new Error(`HTTP status not ok: ${res.status}`)
      yield * parse(res.body)
    }, options.timeout)
  }
}

/**
 * @template T
 * @param {(signal: AbortSignal) => Promise<T>} signalReceiver
 * @param {number} timeout
 * @returns {Promise<T>}
 */
async function withTimeout (signalReceiver, timeout = TIMEOUT) {
  const controller = new AbortController()
  const tid = setTimeout(() => controller.abort(), timeout)
  try {
    const res = await signalReceiver(controller.signal)
    return res
  } catch (err) {
    if (controller.signal.aborted && err.name === 'AbortError') {
      err.code = 'ERR_TIMEOUT'
    }
    throw err
  } finally {
    clearTimeout(tid)
  }
}

/**
 * @template T
 * @param {(signal: AbortSignal) => AsyncIterable<T>} signalReceiver
 * @param {number} timeout
 * @returns {AsyncIterable<T>}
 */
async function * withChunkTimeout (signalReceiver, timeout = TIMEOUT) {
  const controller = new AbortController()
  const onTimeout = () => controller.abort()
  let tid = setTimeout(onTimeout, timeout)
  try {
    for await (const chunk of signalReceiver(controller.signal)) {
      clearTimeout(tid)
      tid = setTimeout(onTimeout, timeout)
      yield chunk
    }
  } catch (err) {
    if (controller.signal.aborted && err.name === 'AbortError') {
      err.code = 'ERR_TIMEOUT'
    }
    throw err
  } finally {
    clearTimeout(tid)
  }
}
