import pg from 'pg'
import dotenv from 'dotenv'
import bytes from 'bytes'
import { Cluster } from '@nftstorage/ipfs-cluster'
import retry from 'p-retry'
import { fetch } from '@web-std/fetch'
import fs from 'fs/promises'
import path from 'path'
import toUri from 'multiaddr-to-uri'
import { IpfsClient } from './ipfs-client.js'
import { ElasticProvider } from './elastic-provider.js'
import CLUSTER_PEER_ADDRS from './cluster-peer-addrs.js'
import { Logger } from './logger.js'

global.fetch = fetch

dotenv.config()

const FETCH_CONTENT_SQL = 'SELECT cid, dag_size FROM content TABLESAMPLE SYSTEM(1) WHERE dag_size IS NOT NULL AND dag_size > $1 AND dag_size < $2 ORDER BY RANDOM() LIMIT $3'

const MiB = 1024 * 1024
const GiB = 1024 * MiB

const sizes = [
  [0, MiB],
  [MiB, 5 * MiB],
  [5 * MiB, 25 * MiB],
  [25 * MiB, 100 * MiB],
  [100 * MiB, 500 * MiB],
  [500 * MiB, GiB]
  // [GiB, 10 * GiB]
  // [10 * GiB, 32 * GiB]
]

const LIMIT = 250

async function main () {
  const db = new pg.Client({ connectionString: mustGetEnv('DATABASE_CONNECTION') })
  await db.connect()
  console.log('🐘 PostgreSQL connected')

  const ipfsPath = process.env.IPFS_PATH || './.ipfs'
  const ipfsApiUrl = toUri(await retry(() => fs.readFile(path.join(ipfsPath, 'api'), 'utf8')))
  const ipfs = new IpfsClient(ipfsApiUrl)
  const identity = await ipfs.id()
  console.log(`🪐 IPFS ready: ${identity.ID}`)

  const elastic = new ElasticProvider(mustGetEnv('ELASTIC_PROVIDER_ADDR'), {
    bucket: mustGetEnv('ELASTIC_PROVIDER_S3_BUCKET'),
    region: mustGetEnv('ELASTIC_PROVIDER_S3_REGION'),
    accessKeyId: mustGetEnv('ELASTIC_PROVIDER_S3_ACCESS_KEY_ID'),
    secretAccessKey: mustGetEnv('ELASTIC_PROVIDER_S3_SECRET_ACCESS_KEY')
  })
  console.log('🌩 Elastic Provider client configured')

  const cluster = new Cluster(mustGetEnv('CLUSTER_API_URL'), {
    headers: { Authorization: `Basic ${mustGetEnv('CLUSTER_BASIC_AUTH_TOKEN')}` }
  })
  console.log('🏘 IPFS Cluster client configured')

  const logger = new Logger()

  try {
    const limit = process.env.LIMIT ? parseInt(process.env.LIMIT) : LIMIT
    for (const [min, max] of sizes) {
      console.log(`🧪 Fetching samples between ${bytes(min)} and ${bytes(max)}`)
      const res = await db.query(FETCH_CONTENT_SQL, [min, max, limit * 2])
      if (!res.rows.length) throw new Error('no rows')

      for (const { cid, dag_size: dagSize } of res.rows) {
        if (!(await elastic.has(cid))) {
          console.log(`⏭ Skipping ${cid}: not available on Elastic Provider`)
          continue
        }

        const statusRes = await cluster.status(cid)
        const pinnedClusterPeers = Object.values(statusRes.peerMap).filter(s => s.status === 'pinned')

        if (!pinnedClusterPeers.length) {
          console.log(`⏭ Skipping ${cid}: not available on IPFS Cluster`)
          continue
        }

        const index = randomInt(0, pinnedClusterPeers.length)
        const clusterAddr = CLUSTER_PEER_ADDRS.find(a => a.endsWith(pinnedClusterPeers[index].ipfsPeerId))

        if (!clusterAddr) {
          throw new Error(`missing cluster peer address: ${pinnedClusterPeers[index].ipfsPeerId}`)
        }

        for (const addr of [clusterAddr, elastic.multiaddr]) {
          const peerId = addr.split('/p2p/')[1]

          console.log(`🔌 Connecting ${addr}`)
          try {
            await ipfs.swarmConnect(addr)
          } catch (err) {
            if (err.code === 'ERR_TIMEOUT') {
              console.error(`❌ failed to connect to ${addr}`, err)
              continue
            }
            throw err
          }

          console.log(`⤵️ Transferring ${cid} from ${peerId} (${bytes(parseInt(dagSize))})`)
          const start = Date.now()
          let receivedBytes = 0
          const logTransferRate = () => console.log(`${bytes(receivedBytes / ((Date.now() - start) / 1000))}/s`)
          const intervalId = setInterval(logTransferRate, 10000)
          try {
            for await (const chunk of ipfs.dagExport(cid, { timeout: 10000 })) {
              receivedBytes += chunk.length
            }
            logger.logResult(peerId, cid, receivedBytes, Date.now() - start)
          } catch (err) {
            console.error(`❌ failed to transfer ${cid} from ${peerId}`, err)
          } finally {
            clearInterval(intervalId)
            logTransferRate()
          }

          let n = 0
          for await (const res of ipfs.repoGc()) {
            if (!res.Error) n++
          }
          console.log(`🗑 Garbage collected ${n} CIDs`)

          const swarm = await ipfs.swarmPeers()
          for (const p of swarm.Peers || []) {
            const addr = `${p.Addr}/p2p/${p.Peer}`
            console.log(`🔌 Disconnecting ${addr}`)
            await ipfs.swarmDisconnect(addr)
          }
        }
      }
    }
  } finally {
    await db.end()
  }
}

/**
 * @param {string} name
 */
function mustGetEnv (name) {
  const value = process.env[name]
  if (!value) throw new Error(`missing ${name} environment variable`)
  return value
}

function randomInt (min, max) {
  return Math.floor(Math.random() * (max - min) + min)
}

main().then(() => console.log('✅ Done')).catch(console.error)
