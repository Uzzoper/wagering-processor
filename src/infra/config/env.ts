/**
 * Leitura de configuração a partir de variáveis de ambiente.
 * Bun carrega `.env` automaticamente; aqui só centralizamos os nomes e defaults.
 * Nenhum segredo é logado — apenas host/porta/database são não-sensíveis o suficiente para log.
 */

export interface DatabaseConfig {
  host: string;
  port: number;
  name: string;
  user: string;
  password: string;
}

/** Quais processos este serviço roda: `WORKERS_ENABLED=api,outbox,sqs`. */
export interface WorkersConfig {
  enabled: string[];
}

/** Endpoint/credenciais do SQS (LocalStack local: `AWS_ENDPOINT_URL`). */
export interface SqsConfig {
  region: string;
  endpoint?: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  /** Nome (não URL) da fila de eventos da outbox. */
  eventsQueueName: string;
  /** Nome (não URL) da fila de transações consumida pelo consumer. */
  wagerQueueName: string;
  /** Consumer SQS ligado (`sqs` ∈ `WORKERS_ENABLED`). */
  consumerEnabled: boolean;
}

/** Parâmetros do publisher da outbox (polling/claim com lease). */
export interface OutboxConfig {
  enabled: boolean;
  pollIntervalMs: number;
  batchSize: number;
  leaseMs: number;
}

export interface AppConfig {
  port: number;
  database: DatabaseConfig;
  workers: WorkersConfig;
  sqs: SqsConfig;
  outbox: OutboxConfig;
}

function parsePort(value: string | undefined, fallback: number): number {
  if (value === undefined || value.trim() === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Porta inválida em variável de ambiente: "${value}"`);
  }
  return parsed;
}

function parsePositiveMs(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined || value.trim() === '') {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Valor inválido (inteiro positivo) em "${name}": "${value}"`);
  }
  return parsed;
}

function parseWorkersEnabled(value: string | undefined): string[] {
  if (value === undefined || value.trim() === '') {
    return ['api', 'outbox', 'sqs'];
  }
  return value
    .split(',')
    .map((name) => name.trim())
    .filter((name) => name !== '');
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const enabled = parseWorkersEnabled(env.WORKERS_ENABLED);
  return {
    port: parsePort(env.PORT, 3000),
    database: {
      host: env.DB_HOST ?? 'localhost',
      port: parsePort(env.DB_PORT, 5433),
      name: env.DB_NAME ?? 'wagering',
      user: env.DB_USER ?? 'wagering',
      password: env.DB_PASSWORD ?? 'wagering',
    },
    workers: { enabled },
    sqs: {
      region: env.AWS_REGION ?? 'us-east-1',
      endpoint: env.AWS_ENDPOINT_URL,
      accessKeyId: env.AWS_ACCESS_KEY_ID,
      secretAccessKey: env.AWS_SECRET_ACCESS_KEY,
      eventsQueueName: env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo',
      wagerQueueName: env.SQS_WAGER_QUEUE ?? 'wager-transactions.fifo',
      consumerEnabled: enabled.includes('sqs'),
    },
    outbox: {
      enabled: enabled.includes('outbox'),
      pollIntervalMs: parsePositiveMs(env.OUTBOX_POLL_INTERVAL_MS, 500, 'OUTBOX_POLL_INTERVAL_MS'),
      batchSize: parsePositiveMs(env.OUTBOX_BATCH_SIZE, 50, 'OUTBOX_BATCH_SIZE'),
      leaseMs: parsePositiveMs(env.OUTBOX_LEASE_MS, 60_000, 'OUTBOX_LEASE_MS'),
    },
  };
}
