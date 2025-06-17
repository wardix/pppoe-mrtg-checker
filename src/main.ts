import { connect, JSONCodec, type JsMsg } from 'nats'
import mysql from 'mysql2/promise'
import pg from 'pg'
import path from 'path'
import fs from 'fs/promises'
import {
  INITIAL_BACKOFF_DELAY,
  MAX_BACKOFF_DELAY,
  NATS_CONSUMER,
  NATS_SERVERS,
  NATS_STREAM,
  NATS_TOKEN,
  MYSQL_HOST,
  MYSQL_USER,
  MYSQL_PASSWORD,
  MYSQL_DATABASE,
  PG_HOST,
  PG_USER,
  PG_PASSWORD,
  PG_DATABASE,
  PG_PORT,
  MAX_CONSECUTIVE_ERRORS,
  MAX_BATCH_SIZE,
  METRICS_FILE,
  MYSQL_PORT,
  TEMP_METRICS_FILE,
  NIS_GRAPHID_PREFIX,
} from './config'
import logger from './logger'

// Define interface for database results
interface CustomerNetworkRecord {
  net: string
  csid: string
}

interface GraphHostIfaceRecord {
  graphid: string
  host: string
  iface: string
}

interface GraphCsRecord {
  graphid: string
  csid: string
}
const jc = JSONCodec()

// Create MySQL connection pool
const mysqlPool = mysql.createPool({
  host: MYSQL_HOST,
  user: MYSQL_USER,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE,
  port: MYSQL_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
})

// Create PostgreSQL connection pool
const pgPool = new pg.Pool({
  host: PG_HOST,
  user: PG_USER,
  password: PG_PASSWORD,
  database: PG_DATABASE,
  port: PG_PORT,
  max: 10, // Maximum number of clients in the pool
  idleTimeoutMillis: 30000, // How long a client is allowed to remain idle before being closed
})

// Test PostgreSQL connection
pgPool.on('error', (err) => {
  logger.error('Unexpected error on idle PostgreSQL client', err)
})

// Collection of metrics to write
let metricsBuffer: string[] = []

// Function to add a metric to the buffer
function addMetric(metric: string): void {
  metricsBuffer.push(metric)
}

// Function to flush metrics to file
async function flushMetricsToFile(): Promise<void> {
  const metricsToWrite = [...metricsBuffer] // Create a copy
  metricsBuffer = [] // Clear the buffer immediately

  try {
    // Ensure directory exists
    const dir = path.dirname(TEMP_METRICS_FILE)
    await fs.mkdir(dir, { recursive: true })

    // Write all metrics to temporary file
    await fs.writeFile(TEMP_METRICS_FILE, metricsToWrite.join('\n') + '\n')

    // Rename temporary file to final destination (atomic operation)
    await fs.rename(TEMP_METRICS_FILE, METRICS_FILE)

    logger.info(`Wrote ${metricsToWrite.length} metrics to ${METRICS_FILE}`)
  } catch (err) {
    logger.error('Error writing metrics to file:', err)
    // Put the metrics back in the buffer to try again later
    metricsBuffer = [...metricsToWrite, ...metricsBuffer]
  }
}

async function main() {
  try {
    logger.info('Starting NATS message consumer')
    await consumeMessages()
  } catch (err) {
    logger.error('Fatal error:', err)
    process.exit(1)
  }
}

async function consumeMessages() {
  const nc = await connect({
    servers: NATS_SERVERS,
    token: NATS_TOKEN,
  })

  // Graceful shutdown on SIGINT and SIGTERM
  for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, async () => {
      logger.info(`Received ${signal}. Draining NATS connection...`)
      await nc.drain()
      await mysqlPool.end() // Close MySQL connections
      await pgPool.end() // Close PostgreSQL connections
      process.exit(0)
    })
  }

  const js = nc.jetstream()
  const consumer = await js.consumers.get(NATS_STREAM, NATS_CONSUMER)
  let backoffDelay = INITIAL_BACKOFF_DELAY
  let consecutiveErrors = 0

  while (true) {
    try {
      const messages = await consumer.fetch({
        max_messages: 1,
        expires: 1000,
      })
      let hasMessages = false
      for await (const message of messages) {
        hasMessages = true
        try {
          await processMessage(message)
          message.ack()
          // Reset backoff and error count on successful message processing
          backoffDelay = INITIAL_BACKOFF_DELAY
          consecutiveErrors = 0
        } catch (err) {
          logger.error('Error processing message:', err)
          message.nak() // Negative acknowledge to retry later
          consecutiveErrors++
          if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
            logger.error(
              `Reached ${MAX_CONSECUTIVE_ERRORS} consecutive errors, exiting...`,
            )
            await nc.drain()
            await mysqlPool.end()
            await pgPool.end()
            process.exit(1)
          }
        }
      }
      if (!hasMessages) {
        logger.info(`No message available. Backing off for ${backoffDelay} ms`)
        await sleep(backoffDelay)
        backoffDelay = Math.min(backoffDelay * 2, MAX_BACKOFF_DELAY)
      }
    } catch (connErr) {
      logger.error('NATS connection error:', connErr)
      await sleep(backoffDelay)
      backoffDelay = Math.min(backoffDelay * 2, MAX_BACKOFF_DELAY)
    }
  }
}

async function processMessage(message: JsMsg): Promise<void> {
  // Parse the message data
  const data: any = jc.decode(message.data)
  logger.info(`Processing message: ${message.seq}`)

  const networks: string[] = []
  const networkToHostIface = new Map<string, { host: string; iface: string }>()

  for (const host in data.servers) {
    for (const { network, iface } of data.servers[host]) {
      networkToHostIface.set(`${network}/32`, { host, iface })
      networks.push(`${network}/32`)
    }
  }

  if (networks.length === 0) {
    logger.warn('No networks found in message')
  }

  try {
    const netCsids = await queryNetworksInBatches(networks)
    const netToCsid = new Map<string, number>()
    const hostIfaceToCsid = new Map<string, number>()
    netCsids.forEach((e) => {
      netToCsid.set(e.net, Number(e.csid))
    })
    const hostIfaces: string[] = []
    networkToHostIface.forEach(({ host, iface }, net) => {
      const csid = netToCsid.get(net)
      hostIfaces.push(`${host} ${iface}`)
      hostIfaceToCsid.set(`${host} ${iface}`, csid as number)
    })

    const graphHostIfaces = await queryGraphHostIfacesInBatches(hostIfaces)
    const rightGraphidToCsid = new Map<string, number>()
    const graphids: string[] = []
    graphHostIfaces.forEach(({ graphid, host, iface }) => {
      const csid = hostIfaceToCsid.get(`${host} ${iface}`)
      if (!csid) return
      rightGraphidToCsid.set(`${NIS_GRAPHID_PREFIX}${graphid}`, csid)
      graphids.push(`${NIS_GRAPHID_PREFIX}${graphid}`)
    })

    const nisGraphidCsids = await queryGraphidCsidsInBatches(graphids)
    nisGraphidCsids.forEach(({ graphid, csid }) => {
      const rightCsid = rightGraphidToCsid.get(graphid)
      if (!rightCsid) return
      if (Number(csid) == rightCsid) return
      const metric = `invalid_linked_graph{graphid="${graphid}",csid="${csid}"} 1`
      addMetric(metric)
    })
    const insertedGraphLink: { graphid: string; csid: number }[] = []
    rightGraphidToCsid.forEach((csid, graphid) => {
      if (nisGraphidCsids.find((e) => e.graphid == graphid)) return
      insertedGraphLink.push({ graphid, csid })
    })

    const mysqlConnection = await mysqlPool.getConnection()
    try {
      insertedGraphLink.forEach(async ({ graphid, csid }) => {
        await mysqlConnection.execute(`
          UPDATE CustomerServicesZabbixGraph
          SET OrderNo = OrderNo + 1
          WHERE CustServId = ?
        `, [csid])

        const query = `
          INSERT INTO CustomerServicesZabbixGraph
          SET GraphId = ?, CustServId = ?, Title = 'Traffic PPPoE',
              OrderNo = 0, UpdatedTime = NOW(), UpdatedBy = '0200306'
        `
        await mysqlConnection.execute(query, [graphid, csid])
      })
    } catch (error) {
      throw error
    } finally {
      mysqlConnection.release()
    }
    await flushMetricsToFile()
  } catch (error) {
    logger.error('Error databases:', error)
    throw error
  }
}

async function queryGraphidCsidsInBatches(
  graphids: string[],
): Promise<GraphCsRecord[]> {
  const allResults: GraphCsRecord[] = []
  for (let i = 0; i < graphids.length; i += MAX_BATCH_SIZE) {
    const batch = graphids.slice(i, i + MAX_BATCH_SIZE)
    const batchResults = await queryGraphidCsidsBatch(batch)
    allResults.push(...batchResults)
  }
  return allResults
}

async function queryGraphidCsidsBatch(
  graphids: string[],
): Promise<GraphCsRecord[]> {
  if (graphids.length === 0) return []
  const placeHolders = graphids.map(() => '?').join(',')
  let connection
  try {
    connection = await mysqlPool.getConnection()

    const query = `
      SELECT TRIM(GraphId) graphid, CustServId csid
      FROM CustomerServicesZabbixGraph
      WHERE TRIM(GraphId) IN (${placeHolders})
    `

    const [rows] = await connection.execute(query, graphids)
    return rows as GraphCsRecord[]
  } finally {
    if (connection) connection.release()
  }
}

async function queryGraphHostIfacesInBatches(
  hostIfaces: string[],
): Promise<GraphHostIfaceRecord[]> {
  const allResults: GraphHostIfaceRecord[] = []
  for (let i = 0; i < hostIfaces.length; i += MAX_BATCH_SIZE) {
    const batch = hostIfaces.slice(i, i + MAX_BATCH_SIZE)
    const batchResults = await queryGraphHostIfacesBatch(batch)
    allResults.push(...batchResults)
  }
  return allResults
}

async function queryGraphHostIfacesBatch(
  hostIfaces: string[],
): Promise<GraphHostIfaceRecord[]> {
  if (hostIfaces.length === 0) return []

  const placeHolders = hostIfaces.map((_, index) => `$${index + 1}`).join(',')

  let connection
  try {
    connection = await pgPool.connect()
    const query = `
      SELECT DISTINCT 
          g.graphid, 
          h.host, 
          SUBSTRING(g.name, 9) AS iface
      FROM 
          graphs_items gi
      LEFT JOIN 
          graphs g ON g.graphid = gi.graphid
      LEFT JOIN 
          items i ON i.itemid = gi.itemid
      LEFT JOIN 
          hosts h ON i.hostid = h.hostid
      WHERE 
          CONCAT(h.host, ' ', SUBSTRING(g.name, 9)) IN (${placeHolders})
    `
    const rest = await connection.query(query, hostIfaces)
    return rest.rows as GraphHostIfaceRecord[]
  } finally {
    if (connection) connection.release()
  }
}

// Function to query networks in batches
async function queryNetworksInBatches(
  networks: string[],
): Promise<CustomerNetworkRecord[]> {
  const allResults: CustomerNetworkRecord[] = []

  // Process in batches to avoid query parameter limits
  for (let i = 0; i < networks.length; i += MAX_BATCH_SIZE) {
    const batch = networks.slice(i, i + MAX_BATCH_SIZE)
    const batchResults = await queryNetworkBatch(batch)
    allResults.push(...batchResults)
  }

  return allResults
}

// Function to query a batch of networks
async function queryNetworkBatch(
  networks: string[],
): Promise<CustomerNetworkRecord[]> {
  if (networks.length === 0) return []

  const placeHolders = networks.map(() => '?')
  let connection

  try {
    connection = await mysqlPool.getConnection()

    const query = `
      SELECT cst.Network net, cst.CustServId csid
      FROM CustomerServiceTechnical cst
      LEFT JOIN CustomerServices cs ON cs.CustServId = cst.CustServId
      LEFT JOIN Customer c ON c.CustId = cs.CustId
      WHERE c.BranchId = '020'
      AND NOT (cs.ServiceId IN ('IPP'))
      AND cst.Network IN (${placeHolders.join(',')})
      ORDER BY cst.Network
    `

    const [rows] = await connection.execute(query, networks)
    return rows as CustomerNetworkRecord[]
  } finally {
    if (connection) connection.release()
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

// Start the application
main()
