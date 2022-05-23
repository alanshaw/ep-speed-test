import { S3Client, ListObjectsV2Command, HeadObjectCommand } from '@aws-sdk/client-s3'
import { CID } from 'multiformats'
import * as raw from 'multiformats/codecs/raw'
import * as dagPB from '@ipld/dag-pb'
import { CarReader } from '@ipld/car'
import { mustGetEnv } from './util.js'

/**
 * @typedef {Map<string, import('@ipld/car/api').Block>} Blockstore
 */

async function main () {
  const s3CarReader = new S3CarReader({
    bucket: mustGetEnv('ELASTIC_PROVIDER_S3_BUCKET'),
    region: mustGetEnv('ELASTIC_PROVIDER_S3_REGION'),
    accessKeyId: mustGetEnv('ELASTIC_PROVIDER_S3_ACCESS_KEY_ID'),
    secretAccessKey: mustGetEnv('ELASTIC_PROVIDER_S3_SECRET_ACCESS_KEY')
  })

  /** @type {CID} */
  let root
  try {
    root = CID.parse(process.argv[2])
  } catch (err) {
    throw new Error(`missing or invalid root CID argument: ${process.argv[2]}`, { cause: err })
  }

  const blockstore = s3CarReader.read(root)

  await walkDag(root, blockstore)
  console.log(`✅ ${root} is a complete DAG on S3`)
}

export class S3CarReader {
  /**
   * @param {{
   *   bucket: string
   *   region: string
   *   accessKeyId: string
   *   secretAccessKey: string
   * }} s3Config S3 config for the bucket that elastic provider uses.
   */
  constructor (s3Config) {
    this._s3 = new S3Client({
      region: s3Config.region,
      credentials: {
        accessKeyId: s3Config.accessKeyId,
        secretAccessKey: s3Config.secretAccessKey
      }
    })
    this._bucketName = s3Config.bucket
  }

  async read (cid) {
    const paths = await this._getCarPaths(cid)

    /** @type {Blockstore} */
    const blockstore = new Map()

    for (const path of paths) {
      console.log(`🚗 ${path}`)
      const size = blockstore.size
      await this._readCar(path, blockstore)
      console.log(`🧱 ${blockstore.size - size} new blocks`)
    }

    return blockstore
  }

  /**
   * @private
   * @param {CID} cid
   */
  async _getCarPaths (cid) {
    const carPaths = []
    const subPaths = await this._listObjects(`raw/${cid}`)

    for (const subPath of subPaths) {
      carPaths.push(...(await this._listObjects(subPath)))
    }

    const completePath = `complete/${cid}.car`
    if (await this._headObject(completePath)) {
      carPaths.push(completePath)
    }

    return carPaths
  }

  async _headObject (key) {
    const command = new HeadObjectCommand({
      Bucket: this._bucketName,
      Key: key
    })
    try {
      await this._s3.send(command)
      return true
    } catch (err) {
      return false
    }
  }

  async _listObjects (path) {
    const command = new ListObjectsV2Command({
      Bucket: this._bucketName,
      Prefix: path,
      MaxKeys: 1
    })
    const response = await this._s3.send(command)
    console.log(response.Contents)
    return response.Contents
  }

  /**
   * @private
   * @param {string} path
   * @param {Blockstore blockstore
   */
  async _readCar (path, blockstore) {

  }
}

/**
 * @param {CID} root
 * @param {Blockstore} blockstore
 */
export async function walkDag (root, blockstore) {
  let nextCids = [root]
  while (true) {
    const nextCid = nextCids.shift()
    if (!nextCid) return
    const block = blockstore.get(nextCid.toString())
    if (!block) throw new Error(`missing block: ${nextCid}`)

    switch (nextCid.code) {
      case raw.code:
        break
      case dagPB.code: {
        const data = dagPB.decode(block.bytes)
        nextCids = [...data.Links.map((l) => l.Hash), ...nextCids]
        break
      }
      default:
        throw new Error(`unsupported codec: ${nextCid.code}`)
    }
  }
}

main()
