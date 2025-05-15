export const NATS_SERVERS = process.env.NATS_SERVERS || 'nats://localhost:4222'
export const NATS_TOKEN = process.env.NATS_TOKEN || ''
export const NATS_STREAM = process.env.NATS_STREAM || 'EVENTS'
export const NATS_CONSUMER =
  process.env.NATS_CONSUMER || 'online_pppoe_mrtg_checker'
export const NATS_SUBJECT =
  process.env.NATS_SUBJECT || 'events.online_pppoe_data_fetched'
export const INITIAL_BACKOFF_DELAY = Number(
  process.env.INITIAL_BACKOFF_DELAY || 1000,
)
export const MAX_BACKOFF_DELAY = Number(process.env.MAX_BACKOFF_DELAY || 32000)
export const MYSQL_HOST = process.env.MYSQL_HOST || 'localhost'
export const MYSQL_USER = process.env.MYSQL_USER || 'root'
export const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || ''
export const MYSQL_DATABASE = process.env.MYSQL_DATABASE || 'test'
export const MYSQL_PORT = Number(process.env.MYSQL_PORT || 3306)

export const PG_HOST = process.env.PG_HOST || 'localhost'
export const PG_USER = process.env.PG_USER || 'root'
export const PG_PASSWORD = process.env.PG_PASSWORD || ''
export const PG_DATABASE = process.env.PG_DATABASE || 'test'
export const PG_PORT = Number(process.env.PG_PORT || 5432)

export const MAX_CONSECUTIVE_ERRORS = Number(
  process.env.MAX_CONSECUTIVE_ERRORS || 8,
)
export const MAX_BATCH_SIZE = Number(process.env.MAX_BATCH_SIZE || 128)
export const METRICS_FILE = process.env.METRICS_FILE || './data/metrics.txt'
export const TEMP_METRICS_FILE =
  process.env.TEMP_METRICS_FILE || './data/metrics.txt.tmp'

export const NIS_GRAPHID_PREFIX = process.env.NIS_GRAPHID_PREFIX || 'g'
