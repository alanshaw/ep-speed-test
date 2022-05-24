import dotenv from 'dotenv'
import bytes from 'bytes'
import retry from 'p-retry'
import { fetch } from '@web-std/fetch'
import fs from 'fs/promises'
import path from 'path'
import toUri from 'multiaddr-to-uri'
import { CID } from 'multiformats'
import { IpfsClient } from './ipfs-client.js'
import { mustGetEnv } from './util.js'

global.fetch = fetch

dotenv.config()

async function main () {
  /** @type {CID} */
  let cid
  try {
    cid = CID.parse(process.argv[2])
  } catch (err) {
    throw new Error(`missing or invalid CID argument: ${process.argv[2]}`, { cause: err })
  }

  await new Promise(resolve => setTimeout(resolve, 1000))
  const ipfsPath = process.env.IPFS_PATH || './.ipfs'
  const ipfsApiUrl = toUri(await retry(() => fs.readFile(path.join(ipfsPath, 'api'), 'utf8')))
  const ipfs = new IpfsClient(ipfsApiUrl)
  const identity = await ipfs.id()
  console.log(`🪐 IPFS ready: ${identity.ID}`)

  const addr = mustGetEnv('ELASTIC_PROVIDER_ADDR')

  console.log(`🔌 Connecting ${addr}`)
  await ipfs.swarmConnect(addr)

  console.log(`⤵️ Transferring ${cid} from ${addr}`)
  const start = Date.now()
  let receivedBytes = 0
  const logTransferRate = () => console.log(`👉 ${bytes(receivedBytes)} @ ${bytes(receivedBytes / ((Date.now() - start) / 1000))}/s`)
  const intervalId = setInterval(logTransferRate, 10000)
  try {
    await retry(async () => {
      for await (const chunk of ipfs.dagExport(cid, { timeout: 1000 * 60 * 30 })) {
        receivedBytes += chunk.length
      }
    })
  } catch (err) {
    console.error(`❌ failed to transfer ${cid}`, err)
  }

  clearInterval(intervalId)
  logTransferRate()
}

main().then(() => console.log('✅ Done')).catch(console.error)
