import { MikroORM } from '@mikro-orm/postgresql';
import { DeleteQueueCommand } from '@aws-sdk/client-sqs';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import { spawnApp, type SpawnedApp } from '../helpers/app-process';
import { createPrivateWagerQueues, makeSqsClient } from '../helpers/sqs-suite';

/**
 * Observabilidade com processo REAL (CHALLENGE §12): logger JSON em uma linha
 * com correlationId via ALS, readiness `/health/ready` com checks de Postgres
 * e SQS e `/metrics` em formato Prometheus — valores validados DEPOIS de uma
 * carga HTTP (PROCESSED + REJECTED + replay + PENDING_REFERENCE). Este arquivo
 * spawna com `LOG_LEVEL=info`: o preload força `silent` para as outras suítes.
 */

const BASE_MAIN = 'http://127.0.0.1:4370';
const BASE_NO_SQS = 'http://127.0.0.1:4371';
const SUITE_QUEUE = `logs-metrics-${crypto.randomUUID().slice(0, 8)}`;
const MISSING_QUEUE = `${SUITE_QUEUE}-missing.fifo`;

let app: SpawnedApp;
let appNoSqs: SpawnedApp | undefined;
let orm: MikroORM;
let sqs: ReturnType<typeof makeSqsClient>;
let wagerUrl = '';
let dlqUrl = '';
/** Carteira do load (100.00 → 90.00) usada no teste de lock. */
let loadedWalletId = '';
let loadedPlayerId = '';

interface WalletBody {
  id: string;
  playerId: string;
}

interface ErrorBody {
  failureCode?: string;
  message?: string;
}

type WagerResponse = {
  status: number;
  headers: Headers;
  body: Record<string, unknown> & ErrorBody;
};

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

async function postWallet(body: unknown): Promise<WalletBody> {
  const response = await fetch(`${BASE_MAIN}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  expect(response.status).toBe(201);
  return (await response.json()) as WalletBody;
}

async function postWager(
  body: unknown,
  key: string,
  correlationId?: string,
): Promise<WagerResponse> {
  const response = await fetch(`${BASE_MAIN}/wagering/transactions`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'idempotency-key': key,
      ...(correlationId !== undefined ? { 'x-correlation-id': correlationId } : {}),
    },
    body: JSON.stringify(body),
  });
  return {
    status: response.status,
    headers: response.headers,
    body: (await response.json()) as Record<string, unknown> & ErrorBody,
  };
}

function wagerInput(
  playerId: string,
  walletId: string,
  overrides: { amount: string; kind?: string; reference?: string },
): Record<string, unknown> {
  return {
    providerId: 'provider-logs-metrics',
    externalTransactionId: `tx-${crypto.randomUUID()}`,
    playerId,
    walletId,
    roundId: `round-${crypto.randomUUID()}`,
    gameId: 'game-1',
    kind: overrides.kind ?? 'BET',
    money: { amount: overrides.amount, currency: 'BRL' },
    ...(overrides.reference !== undefined
      ? { referenceExternalTransactionId: overrides.reference }
      : {}),
  };
}

beforeAll(async () => {
  sqs = makeSqsClient();
  ({ wagerUrl, dlqUrl } = await createPrivateWagerQueues(sqs, SUITE_QUEUE));
  app = await spawnApp({
    port: 4370,
    workers: 'api',
    silent: false,
    env: {
      // Filas privadas da suíte; sem consumer (api-only): readiness consulta a
      // fila e o gauge DLQ lê a DLQ — nada é consumido pelo app.
      SQS_WAGER_QUEUE: `${SUITE_QUEUE}.fifo`,
      SQS_EVENTS_QUEUE: `${SUITE_QUEUE}-events-not-used.fifo`,
      DB_LOCK_TIMEOUT_MS: '150',
      LOG_LEVEL: 'info',
    },
  });
  orm = await MikroORM.init(buildMikroOrmConfig());
});

afterAll(async () => {
  await app?.stop();
  if (appNoSqs !== undefined) {
    await appNoSqs.stop();
  }
  await orm?.close(true);
  for (const queueUrl of [wagerUrl, dlqUrl]) {
    if (queueUrl !== '') {
      await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
    }
  }
});

/** Linhas JSON do stdout acumulado do app1. */
async function appLines(): Promise<Record<string, unknown>[]> {
  const stdout = await app.stdoutText();
  const lines = stdout.split('\n').filter((line) => line.trim().length > 0);
  return lines.map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function linesOf(app: SpawnedApp): Promise<string> {
  return app.stdoutText();
}

describe('logs, health e métricas com processo real (fatia 8b)', () => {
  test(
    'carga HTTP: asserts I-IV (logs) e VI (contadores no /metrics)',
    { timeout: 20_000 },
    async () => {
      const playerIdA = crypto.randomUUID();
      const walletA = await postWallet({
        playerId: playerIdA,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      });
      loadedWalletId = walletA.id;
      loadedPlayerId = playerIdA;
      const walletB = await postWallet({
        playerId: crypto.randomUUID(),
        initialBalance: { amount: '9.00', currency: 'BRL' },
      });

      // Carga: PROCESSED (BET 100 → 90), replay da key, REJECTED (BET 9 → 422)
      // e REFUND cuja referência nunca existirá (PENDING_REFERENCE 202, sem
      // esperar resolução — nenhum worker pending roda nesta suíte).
      const bet = wagerInput(walletA.playerId, walletA.id, { amount: '10.00' });
      const processed = await postWager(bet, `lm-bet-${walletA.id}`, 'lm-corr-bet-1');
      expect(processed.status).toBe(201);
      expect(processed.body.status).toBe('PROCESSED');
      // (IV) Eco do header na resposta.
      expect(processed.headers.get('x-correlation-id')).toBe('lm-corr-bet-1');

      const replay = await postWager(bet, `lm-bet-${walletA.id}`, 'lm-corr-replay-2');
      expect(replay.status).toBe(200);
      expect(replay.body.idempotentReplay).toBe(true);

      const rejected = await postWager(
        wagerInput(walletB.playerId, walletB.id, { amount: '10.00' }),
        `lm-rejected-${walletB.id}`,
      );
      expect(rejected.status).toBe(422);
      expect(rejected.body.failureCode).toBe('INSUFFICIENT_BALANCE');

      const refund = await postWager(
        wagerInput(walletA.playerId, walletA.id, {
          amount: '10.00',
          kind: 'REFUND',
          reference: `never-${crypto.randomUUID()}`,
        }),
        `lm-refund-${walletA.id}`,
        'lm-corr-refund-3',
      );
      expect(refund.status).toBe(202);
      expect(refund.body.status as string).toBe('PENDING_REFERENCE');

      // ===== (I) Toda linha stdout é JSON válido com level e timestamp ISO. =====
      const lines = await appLines();
      expect(lines.length).toBeGreaterThan(10);
      for (const record of lines) {
        expect(['debug', 'info', 'warn', 'error']).toContain(String(record.level));
        expect(typeof record.timestamp === 'string').toBe(true);
        const parsedTime = new Date(String(record.timestamp)).getTime();
        expect(Number.isNaN(parsedTime)).toBe(false);
      }

      // ===== (II) Contexto completo nas linhas do submit. =====
      const submissions = lines.filter((record) => record.event === 'wager.submitted');
      expect(submissions).toHaveLength(4);
      for (const record of submissions) {
        expect(typeof record.transactionId).toBe('string');
        expect(typeof record.walletId).toBe('string');
        expect(record.providerId).toBe('provider-logs-metrics');
        expect(typeof record.correlationId).toBe('string');
      }
      expect(submissions.some((record) => record.correlationId === 'lm-corr-bet-1')).toBe(true);
      expect(submissions.some((record) => record.correlationId === 'lm-corr-replay-2')).toBe(true);

      // ===== (III) Nenhuma linha com payload financeiro. =====
      const stdout = await linesOf(app);
      expect(stdout).not.toContain('"amount"');
      expect(stdout).not.toContain('"10.00"');

      // ===== (VI) Contadores no /metrics logo após a carga. =====
      const metrics = await (await fetch(`${BASE_MAIN}/metrics`)).text();
      expect(
        metrics.includes('wager_transactions_total{kind="BET",status="PROCESSED"} 1'),
      ).toBe(true);
      expect(
        metrics.includes('wager_transactions_total{kind="BET",status="REJECTED"} 1'),
      ).toBe(true);
      expect(metrics.includes('wager_duplicates_total{source="idempotency"} 1')).toBe(true);
      const field = (name: string): RegExpExecArray | null =>
        new RegExp(`${name} ([-\\d.]+)`).exec(metrics);
      const countMatch = field('wager_processing_latency_ms_count');
      expect(countMatch).not.toBeNull();
      expect(Number(countMatch![1])).toBeGreaterThanOrEqual(1);
      const lagMatch = field('outbox_lag_seconds');
      expect(lagMatch).not.toBeNull();
      expect(Number(lagMatch![1])).toBeGreaterThanOrEqual(0);
      const dlqMatch = field('sqs_dlq_messages');
      expect(dlqMatch).not.toBeNull();
      expect(Number(dlqMatch![1])).toBeGreaterThanOrEqual(0);
    },
  );

  test(
    'readiness: 200 com ambos os checks up; 503 com fila SQS inexistente',
    { timeout: 30_000 },
    async () => {
      const ready = await fetch(`${BASE_MAIN}/health/ready`);
      expect(ready.status).toBe(200);
      const readyBody = (await ready.json()) as {
        status: string;
        checks: { postgres: string; sqs: string };
      };
      expect(readyBody.status).toBe('ok');
      expect(readyBody.checks).toEqual({ postgres: 'up', sqs: 'up' });

      appNoSqs = await spawnApp({
        port: 4371,
        workers: 'api',
        env: { SQS_WAGER_QUEUE: MISSING_QUEUE },
      });
      const unavailable = await fetch(`${BASE_NO_SQS}/health/ready`);
      expect(unavailable.status).toBe(503);
      const unavailableBody = (await unavailable.json()) as typeof readyBody;
      expect(unavailableBody.status).toBe('unavailable');
      expect(unavailableBody.checks).toEqual({ postgres: 'up', sqs: 'down' });
    },
  );

  test(
    'lock da carteira: 503 na borda e db_lock_conflicts_total (VII)',
    { timeout: 20_000 },
    async () => {
      expect(loadedWalletId !== '').toBe(true);
      // `DB_LOCK_TIMEOUT_MS=150` no app: o submit aguenta o lock e aborta
      // (55P03) — a UnitOfWork conta o conflito e rethrowa (borda mapeia 503).
      // Root em: só a transação do em raiz (orm.em.transactional) fixa a
      // conexão — fork perde o hold entre os queries.
      await orm.em.transactional(async (tx) => {
        await tx.execute(`SELECT id FROM wallet WHERE id = ? FOR UPDATE`, [loadedWalletId]);
        const response = await fetch(`${BASE_MAIN}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'idempotency-key': `lm-lock-${crypto.randomUUID()}`,
          },
          body: JSON.stringify(
            wagerInput(loadedPlayerId, loadedWalletId, { amount: '10.00' }),
          ),
          signal: AbortSignal.timeout(15_000),
        });
        expect(response.status).toBe(503);
        const body = (await response.json()) as ErrorBody;
        expect(body.failureCode).toBe('INFRA_TRANSIENT');
      });

      const metrics = await (await fetch(`${BASE_MAIN}/metrics`)).text();
      expect(metrics.includes('db_lock_conflicts_total 1')).toBe(true);
    },
  );
});
