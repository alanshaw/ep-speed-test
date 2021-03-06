import pg from 'pg'
import dotenv from 'dotenv'
import bytes from 'bytes'
import retry from 'p-retry'
import { fetch } from '@web-std/fetch'
import fs from 'fs/promises'
import path from 'path'
import toUri from 'multiaddr-to-uri'
import { IpfsClient } from './ipfs-client.js'
import { ElasticProvider } from './elastic-provider.js'
import { mustGetEnv } from './util.js'

global.fetch = fetch

dotenv.config()

const FETCH_CONTENT_SQL = 'SELECT cid FROM content TABLESAMPLE SYSTEM(1) WHERE dag_size IS NOT NULL AND dag_size > $1 AND dag_size < $2 ORDER BY RANDOM() LIMIT $3'
const MiB = 1024 * 1024
const LIMIT = 1000
const STALLED_TIMEOUT_DURATION = process.env.STALLED_TIMEOUT_DURATION || 1000 * 60 * 5;

async function main () {
  const db = new pg.Client({ connectionString: mustGetEnv('DATABASE_CONNECTION') })
  await db.connect()
  console.log('๐ PostgreSQL connected')

  const ipfsPath = process.env.IPFS_PATH || './.ipfs'
  const ipfsApiUrl = toUri(await retry((count) => {
    if (count >= 5) {
      throw new Error('error reading IPFS_PATH, be sure `ipfs daemon` is running with your desired IPFS_PATH')
    }
    return fs.readFile(path.join(ipfsPath, 'api'), 'utf8');
  }))
  const ipfs = new IpfsClient(ipfsApiUrl)
  const identity = await ipfs.id()
  console.log(`๐ช IPFS ready: ${identity.ID}`)

  const elastic = new ElasticProvider(mustGetEnv('ELASTIC_PROVIDER_ADDR'), {
    bucket: mustGetEnv('ELASTIC_PROVIDER_S3_BUCKET'),
    region: mustGetEnv('ELASTIC_PROVIDER_S3_REGION'),
    accessKeyId: mustGetEnv('ELASTIC_PROVIDER_S3_ACCESS_KEY_ID'),
    secretAccessKey: mustGetEnv('ELASTIC_PROVIDER_S3_SECRET_ACCESS_KEY')
  })
  console.log('๐ฉ Elastic Provider client configured')

  try {
    const limit = process.env.LIMIT ? parseInt(process.env.LIMIT) : LIMIT
    const min = 0
    const max = 25 * MiB

    console.log(`๐งช Fetching samples between ${bytes(min)} and ${bytes(max)}`)
    const res = await db.query(FETCH_CONTENT_SQL, [min, max, limit])
    if (!res.rows.length) throw new Error('no rows')

    console.log('๐ Filtering out CIDs that are not available on Elastic Provider')
    const wantlist = []
    for (const { cid } of res.rows) {
      if (!(await elastic.has(cid))) {
        console.log(`โญ Skipping ${cid}: not available on Elastic Provider`)
        continue
      }
      wantlist.push(cid)
    }

    console.log(`๐ Connecting ${elastic.multiaddr}`)
    await ipfs.swarmConnect(elastic.multiaddr)

    console.log(`โคต๏ธ Transferring ${wantlist.length} CIDs from ${elastic.multiaddr}`)
    const start = Date.now()
    let receivedBytes = 0
    let concurrentTransfers = 0
    let succeededTransfers = 0
    let failedTransfers = 0
    const logTransferRate = () => console.log(`๐ ${bytes(receivedBytes)} @ ${bytes(receivedBytes / ((Date.now() - start) / 1000))}/s (${concurrentTransfers} current, ${succeededTransfers} successful, ${failedTransfers} failed)`)
    const intervalId = setInterval(logTransferRate, 10000)
    await Promise.all(wantlist.map(async cid => {
      const stalledTimeout = setTimeout(() => {
        console.log('cid stalled', cid);
        clearTimeout(stalledTimeout);
      }, STALLED_TIMEOUT_DURATION)
      concurrentTransfers++
      try {
        await retry(async () => {
          for await (const chunk of ipfs.dagExport(cid, { timeout: 1000 * 60 * 30 })) {
            receivedBytes += chunk.length
          }
          succeededTransfers++
        })
      } catch (err) {
        console.error(`โ failed to transfer ${cid}`, err)
        failedTransfers++
      } finally {
        concurrentTransfers--
        clearTimeout(stalledTimeout)
      }
    }))

    clearInterval(intervalId)
    logTransferRate()
  } finally {
    await db.end()
  }
}

main().then(() => console.log('โ Done')).catch(console.error)
