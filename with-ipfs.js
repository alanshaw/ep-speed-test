#!/usr/bin/env node
import { fileURLToPath } from "url";
import process from "process";
import * as assert from "assert";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { dirname, resolve } from 'path';
import debug from "debug";
import { exec, spawn } from "node:child_process";
import { hideBin } from "yargs/helpers";
import { spawnSync } from "child_process";

const debugLog = debug('elastic-ipfs:with-ipfs')

/**
 * @typedef {(useIpfs: (ipfs: { path: string }) => Promise<void>} IpfsUser
 */

/**
 * @typedef {(
 *    options: {
 *      clean: bool,
 *      ipfsPath: string,
 *    },
 *    useIpfs: IpfsUser,
 *  ) => Promise<void>} WithIpfsDaemon
 */

function mkpTempDir(preferredPrefix='tmp-') {
  const osTmp = os.tmpdir();
  const sep = path.sep;
  const prefix = `${osTmp}${sep}${preferredPrefix}`;
  const tempDirpath = fs.mkdtempSync(prefix)
  return tempDirpath;
}

/**
 * @type {WithIpfsDaemon}
 * @param options
 * @param {bool} options.clean
 * @param {string} options.ipfsPath
 * @param {IpfsUser} use
 */
const withIpfsDaemon = async (
  {
    preClean=false,
    ipfsPath=mkpTempDir('ipfs-'),
  },
  use,
) => {
  debugLog({ ipfsPath })
  assert.ok(fs.existsSync(ipfsPath), 'expect ipfsPath to exist on fs')
  if (preClean) {
    const cleanCommand = `rm -rf ${ipfsPath}`
    debugLog('clean', cleanCommand)
    await execShellCommand(cleanCommand, {
      ...process.env,
      IPFS_PATH: ipfsPath,
    })
  }
  // init
  {
    const initCommand = `ipfs init --profile=test`
    debugLog('init', initCommand)
    await execShellCommand(initCommand, {
      ...process.env,
      IPFS_PATH: ipfsPath,
    })
  }
  // configure
  {
    const configureCommand = `ipfs config Routing.Type none`
    debugLog('configure', configureCommand)
    await execShellCommand(configureCommand, {
      ...process.env,
      IPFS_PATH: ipfsPath,
    })
  }
  debugLog({ ipfsPath })
  debugLog('spawning ipfs daemon')
  const daemonProcess = spawn('ipfs', ['daemon'], {
    env: { ...process.env, IPFS_PATH: ipfsPath },
  });
  daemonProcess.once('error', (error) => {
    console.error('daemon error', error);
  });
  daemonProcess.stdout.pipe(process.stdout, { end: false })
  daemonProcess.stderr.pipe(process.stderr, { end: false })
  await new Promise((resolve, reject) => {
    daemonProcess.once('error', reject);
    daemonProcess.once('spawn', resolve);
  })
  await new Promise((resolve) => setTimeout(resolve, 1000))
  await use({
    path: ipfsPath,
  });
  await new Promise((resolve, reject) => {
    daemonProcess.once('error', reject);
    daemonProcess.once('close', resolve);
    debugLog('killing ipfs daemon')
    daemonProcess.kill('SIGHUP')
  })
};

/**
 * Executes a shell command and return it as a Promise.
 * @param cmd {string}
 * @param env {Record<string,string>}
 * @return {Promise<string>}
 */
 function execShellCommand(cmd, env=process.env) {
  return new Promise((resolve, reject) => {
    try {
      exec(cmd, {
        shell: true,
        env,
        stderr: process.stderr,
      }, (error, stdout, stderr) => {
       if (error) {
        console.warn(error);
        return reject(error);
       }
       resolve(stdout? stdout : stderr);
      }); 
    } catch (error) {
      reject(error);
    }
  });
 }

/**
 * 
 * @param  {...string[]} argv 
 */
async function main(...argv) {
  const postBinArgs = hideBin(argv);
  await withIpfsDaemon({ clean: true }, async (ipfs) => {
    const [command, ...args] = postBinArgs
    debugLog('invoking remainingCommand', [command, ...args].join(' '))
    const remainingResult = spawnSync(command, args, {
      env: {
        ...process.env,
        IPFS_PATH: ipfs.path,
      }
    });
    console.log({ remainingResult });
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main(...process.argv).catch((error) => {
    throw error;
  });
}
