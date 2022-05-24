# Elastic Provider Speed Test

Speed test for Elastic Provider vs IPFS Cluster.

How does it work?

1. Starts a local IPFS node.
2. Selects a random CID from the database.
3. Ensures it is `pinned` on IPFS Cluster.
4. Ensures it is stored in the Elastic Provider S3 bucket.
5. Connects the local node to an IPFS Cluster node.
6. Transfers the data and measures the speed.
7. Disconnects from the IPFS Cluster node.
8. Connects the local node to the Elastic Provider node.
9. Transfers the data and measures the speed.
10. Disconnects from the Elastic Provider node.
11. Appends the result to the NDJSON log.
12. GOTO 2.

## Usage

Install project dependencies:

```sh
npm install
```

Create a `.env` file in the root of the project and populate with the following information:

```sh
DATABASE_CONNECTION=<postgres connection string>

ELASTIC_PROVIDER_ADDR=<multiaddr of EP node including peer ID>
ELASTIC_PROVIDER_S3_REGION=<EP S3 bucket region>
ELASTIC_PROVIDER_S3_BUCKET=<EP S3 bucket name>
ELASTIC_PROVIDER_S3_ACCESS_KEY_ID=<EP S3 bucket access key>
ELASTIC_PROVIDER_S3_SECRET_ACCESS_KEY=<EP S3 bucket secret>

CLUSTER_API_URL=<cluster API URL>
CLUSTER_BASIC_AUTH_TOKEN=<cluster basic auth token>
```
Start the speed test:

```sh
npm start
```

### Other scripts

#### Check if a DAG is complete in S3

1. Read all CAR files pertaining to the CID from S3
2. Add the blocks to an in memory blockstore
3. Walk the DAG to ensure completeness

```sh
node is-car-dag-complete.js bafybeih6qj6w3kfesrhy5gxo2lvph2p4uvpl7sq3u2rorvjaooqngqwa5y
```

#### Check if Elastic Provider can transfer a given CID

1. Start a local IPFS node
2. Connect to the Elastic Provider node
2. Transfer a given CID

```sh
npm run can-bitswap -- bafybeih6qj6w3kfesrhy5gxo2lvph2p4uvpl7sq3u2rorvjaooqngqwa5y
```

## Notes

A good and simple tool to convert NDJSON to JSON: https://observablehq.com/@iosonosempreio/ndjson-sorceress
