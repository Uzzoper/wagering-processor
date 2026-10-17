import {
  CreateQueueCommand,
  DeleteMessageCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { AddressInfo } from 'node:net';

import { assertLedgerConsistency } from './ledger-consistency';
import { AppModule } from '../../src/app.module';
import { CreateWalletUseCase } from '../../src/application/create-wallet.use-case';
import type { UnitOfWork } from '../../src/application/ports';
import { NoopProviderIdentity } from '../../src/infra/auth/noop-provider-identity';
import { SubmitWagerTransactionUseCase } from '../../src/application/submit-wager.use-case';
import { MikroOrmUnitOfWork } from '../../src/infra/persistence/unit-of-work';
import {
  PendingReferenceWorker,
  type PendingReferenceWorkerOptions,
} from '../../src/infra/workers/pending-reference-worker';
import {
  SqsWagerConsumer,
  type SqsWagerConsumerOptions,
} from '../../src/infra/sqs/sqs-wager-consumer';

/**
 * Fatia 7c — worker de `PENDING_REFERENCE` (resume pelo MESMO use case).
 *
 * Nenhum worker/consumer roda em loop por padrão (preload fixa
 * `WORKERS_ENABLED=api`): os testes chamam `worker.processDueBatch(now)` e
 * `consumer.cycle()` manualmente, com clock fake mutável — zero sleep.
 */

/** Fila privada deste arquivo: o cenário SQS roda isolado — purga/drain aqui
 * não destrói mensagens das outras suítes da LocalStack (e vice-versa). */
const WAGER_QUEUE_NAME = 'test-local-pending-wager-transactions.fifo';
const CONSUMER_NAME = 'test-pending-ref-consumer';
const PROVIDER_ID = 'provider-pending-test';

interface CreateWalletCreated {
  outcome: 'CREATED';
  wallet: { id: string };
}

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;
let unitOfWork: UnitOfWork;
let sqs: SQSClient;
let queueUrl: string;
/** Clock fake mutável: cada teste avança para o instante que quer. */
let clockNow = new Date();

function makeWorker(
  overrides: Partial<PendingReferenceWorkerOptions> = {},
): PendingReferenceWorker {
  return new PendingReferenceWorker(
    unitOfWork,
    new SubmitWagerTransactionUseCase(unitOfWork, new NoopProviderIdentity()),
    {
      enabled: true,
      pollIntervalMs: 0,
      batchSize: 50,
      leaseMs: 60_000,
      ttlHours: 24,
      maxAttempts: 20,
      clock: { now: () => new Date(clockNow) },
      ...overrides,
    },
  );
}

function makeConsumer(
  overrides: Partial<SqsWagerConsumerOptions> = {},
): SqsWagerConsumer {
  return new SqsWagerConsumer(
    new SubmitWagerTransactionUseCase(unitOfWork, new NoopProviderIdentity()),
    sqs,
    WAGER_QUEUE_NAME,
    {
      enabled: true,
      waitTimeSeconds: 1,
      maxMessages: 10,
      visibilityTimeoutSeconds: 2,
      heartbeatMs: 1000,
      pollIntervalMs: 30,
      consumerName: CONSUMER_NAME,
      ...overrides,
    },
  );
}

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

async function createWallet(playerId: string, amount: string): Promise<string> {
  const result = await new CreateWalletUseCase(unitOfWork).execute({
    playerId,
    initialBalance: { amount, currency: 'BRL' },
  });
  expect(result.outcome).toBe('CREATED');
  return (result as unknown as CreateWalletCreated).wallet.id;
}

interface SubmitBody {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
}

function makeInput(
  playerId: string,
  walletId: string,
  overrides: Record<string, unknown> = {},
): { body: Record<string, unknown>; key: string } {
  const externalTransactionId = (overrides.externalTransactionId as string) ?? crypto.randomUUID();
  return {
    key: `${PROVIDER_ID}:${externalTransactionId}`,
    body: {
      providerId: PROVIDER_ID,
      externalTransactionId,
      playerId,
      walletId,
      roundId: 'pending-round',
      gameId: 'pending-game',
      kind: 'BET',
      money: { amount: '10.00', currency: 'BRL' },
      ...overrides,
    },
  };
}

async function postWager(
  body: Record<string, unknown>,
  key: string,
): Promise<{ status: number; body: SubmitBody }> {
  const response = await fetch(`${baseUrl}/wagering/transactions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as SubmitBody };
}

async function walletRow(walletId: string): Promise<{ balance_amount: string; version: number }> {
  const rows = await query<Array<{ balance_amount: string; version: number }>>(
    `SELECT balance_amount, version FROM wallet WHERE id = ?`,
    [walletId],
  );
  expect(rows).toHaveLength(1);
  return rows[0]!;
}

async function txById(id: string): Promise<{
  status: string;
  kind: string;
  failure_code: string | null;
  observed_balance_amount: string | null;
  retry_attempts: number;
  pending_since: Date | string | null;
  next_retry_at: Date | string | null;
}> {
  const rows = await query<
    Array<{
      status: string;
      kind: string;
      failure_code: string | null;
      observed_balance_amount: string | null;
      retry_attempts: number;
      pending_since: Date | string | null;
      next_retry_at: Date | string | null;
    }>
  >(
    `SELECT status, kind, failure_code, observed_balance_amount, retry_attempts,
            pending_since, next_retry_at
       FROM wager_transaction WHERE id = ?`,
    [id],
  );
  expect(rows).toHaveLength(1);
  return rows[0]!;
}

/** Eventos persistidos na outbox para um transactionId (em `data`). */
async function outboxEventsFor(walletId: string, transactionId: string): Promise<string[]> {
  const rows = await query<Array<{ event_type: string }>>(
    `SELECT event_type FROM outbox_message
      WHERE aggregate_id = ? AND payload->'data'->>'transactionId' = ?`,
    [walletId, transactionId],
  );
  return rows.map((row) => row.event_type);
}

async function ledgerDirectionCounts(
  walletId: string,
): Promise<{ debit: number; credit: number }> {
  // Exclui o lançamento de OPENING da criação da wallet (não é efeito da submissão).
  const rows = await query<Array<{ direction: string; count: number }>>(
    `SELECT l.direction, count(*)::int AS count
       FROM wallet_ledger_entry l
       JOIN wager_transaction t ON t.id = l.transaction_id
      WHERE l.wallet_id = ? AND t.kind <> 'OPENING'
      GROUP BY l.direction`,
    [walletId],
  );
  const debit = rows.find((row) => row.direction === 'DEBIT')?.count ?? 0;
  const credit = rows.find((row) => row.direction === 'CREDIT')?.count ?? 0;
  return { debit, credit };
}

/** `execute` bruto para o shared helper de consistência do ledger. */
async function executeShared(
  sql: string,
  params: unknown[] = [],
): Promise<Array<{ balance_amount: string; ledger_sum: string | null; last_after: string | null }>> {
  return query(sql, params);
}

interface FinancialSnapshot {
  balanceAmount: string;
  version: number;
  ledgerCount: number;
  ledgerSum: string;
  txCount: number;
}

async function snapshotFinancials(walletId: string): Promise<FinancialSnapshot> {
  const wallet = await walletRow(walletId);
  const ledger = await query<Array<{ sum: string; count: number }>>(
    `SELECT COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0) AS sum,
            count(*)::int AS count
       FROM wallet_ledger_entry WHERE wallet_id = ?`,
    [walletId],
  );
  const transactions = await query<Array<{ count: number }>>(
    `SELECT count(*)::int AS count FROM wager_transaction WHERE wallet_id = ?`,
    [walletId],
  );
  return {
    balanceAmount: wallet.balance_amount,
    version: wallet.version,
    ledgerCount: ledger[0]!.count,
    ledgerSum: ledger[0]!.sum,
    txCount: transactions[0]!.count,
  };
}

/** Submete uma reversão (`REFUND`/`ROLLBACK`) cuja referência ainda não existe. */
async function deferReversalPending(
  playerId: string,
  walletId: string,
  kind: 'REFUND' | 'ROLLBACK',
  referenceExternalTransactionId: string,
): Promise<{ transactionId: string; body: Record<string, unknown>; key: string }> {
  const { body, key } = makeInput(playerId, walletId, {
    kind,
    referenceExternalTransactionId,
  });
  const { status, body: result } = await postWager(body, key);
  expect(status).toBe(202);
  expect(result.status).toBe('PENDING_REFERENCE');
  return { transactionId: result.transactionId, body, key };
}

async function submitBet(
  playerId: string,
  walletId: string,
  externalTransactionId: string,
): Promise<SubmitBody> {
  const { body, key } = makeInput(playerId, walletId, { externalTransactionId });
  const { status, body: result } = await postWager(body, key);
  expect(status).toBe(201);
  expect(result.status).toBe('PROCESSED');
  return result;
}

interface EnvelopeData extends Record<string, unknown> {
  kind: string;
  money: { amount: string; currency: string };
  idempotencyKey: string;
}

function wagerEnvelope(
  messageId: string,
  data: EnvelopeData,
): { messageId: string; type: string; occurredAt: string; data: EnvelopeData } {
  return {
    messageId,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data,
  };
}

async function enqueue(envelope: unknown, walletId: string, dedupId: string): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: walletId,
      MessageDeduplicationId: dedupId,
    }),
  );
}

async function purgeQueue(): Promise<void> {
  await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
}

/** Drena a fila com DELETE (para asseverar "nada a mais" na prova do ack). */
async function drainQueue(timeoutMs = 8_000): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  let deleted = 0;
  while (Date.now() < deadline) {
    const response = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1 }),
    );
    for (const message of response.Messages ?? []) {
      await sqs.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }),
      );
      deleted += 1;
    }
    if ((response.Messages ?? []).length === 0) {
      break;
    }
  }
  return deleted;
}

beforeAll(async () => {
  app = await NestFactory.create(AppModule, { logger: false });
  await app.listen(0);

  const address = app.getHttpServer().address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${address.port}`;
  orm = app.get(MikroORM);
  unitOfWork = new MikroOrmUnitOfWork(orm);

  sqs = new SQSClient({
    region: process.env.AWS_REGION ?? 'us-east-1',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
  const created = await sqs.send(
    new CreateQueueCommand({
      QueueName: WAGER_QUEUE_NAME,
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'true' },
    }),
  );
  queueUrl = created.QueueUrl!;
  await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
}, 60_000);

afterAll(async () => {
  await app.close();
});

describe('REFUND antes da BET', () => {
  test('worker aplica pelo resume e o replay devolve PROCESSED idempotente', async () => {
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '100.00');
    const betExt = crypto.randomUUID();
    const refund = await deferReversalPending(playerId, walletId, 'REFUND', betExt);

    // Evento de pendência persistido na outbox.
    expect(await outboxEventsFor(walletId, refund.transactionId)).toEqual([
      'WagerTransactionPendingReference',
    ]);

    // A referência chega depois: BET processa e debita.
    const betResult = await submitBet(playerId, walletId, betExt);
    expect(betResult.balance).toEqual({ amount: '90.00', currency: 'BRL' });

    // Worker resolve o refund pendente.
    clockNow = new Date(clockNow.getTime() + 60_000);
    const worker = makeWorker();
    expect(await worker.processDueBatch()).toBeGreaterThanOrEqual(1);

    const refundRow = await txById(refund.transactionId);
    expect(refundRow.status).toBe('PROCESSED');
    expect(refundRow.observed_balance_amount).toBe('100.00');
    expect((await walletRow(walletId)).balance_amount).toBe('100.00');
    expect((await walletRow(walletId)).version).toBe(3); // OPENING, débito, crédito

    // Exatamente 1 lançamento CREDIT do refund na wallet.
    expect((await ledgerDirectionCounts(walletId)).credit).toBe(1);

    // Replay HTTP pela mesma Idempotency-Key: devolve o terminal original.
    const replay = await postWager(refund.body, refund.key);
    expect(replay.status).toBe(200);
    expect(replay.body.status).toBe('PROCESSED');
    expect(replay.body.idempotentReplay).toBe(true);

    // Worker rodando 2× não duplica efeito.
    const before = await snapshotFinancials(walletId);
    await worker.processDueBatch();
    await worker.processDueBatch();
    expect(await snapshotFinancials(walletId)).toEqual(before);

    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});

describe('ROLLBACK antes da BET', () => {
  test('mesmo fluxo: rollback credita a inversão quando a ref aparece', async () => {
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '100.00');
    const betExt = crypto.randomUUID();
    const rollback = await deferReversalPending(playerId, walletId, 'ROLLBACK', betExt);

    // BET chega depois (débito)...
    const betResult = await submitBet(playerId, walletId, betExt);
    expect(betResult.balance).toEqual({ amount: '90.00', currency: 'BRL' });

    // ...e o worker aplica o ROLLBACK (crédito da inversão).
    clockNow = new Date(clockNow.getTime() + 60_000);
    expect(await makeWorker().processDueBatch()).toBeGreaterThanOrEqual(1);

    const rollbackRow = await txById(rollback.transactionId);
    expect(rollbackRow.status).toBe('PROCESSED');
    expect(rollbackRow.observed_balance_amount).toBe('100.00');
    expect((await walletRow(walletId)).balance_amount).toBe('100.00');

    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});

describe('TTL vencido', () => {
  test('worker rejeita com REFERENCE_NOT_FOUND + evento Rejected', async () => {
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '50.00');
    const refund = await deferReversalPending(playerId, walletId, 'REFUND', crypto.randomUUID());

    // Clock fake avança +2h com ttlHours 1 → expirada pelo TTL.
    clockNow = new Date(clockNow.getTime() + 2 * 3_600_000);
    await makeWorker({ ttlHours: 1 }).processDueBatch();

    const refundRow = await txById(refund.transactionId);
    expect(refundRow.status).toBe('REJECTED');
    expect(refundRow.failure_code).toBe('REFERENCE_NOT_FOUND');

    // Nenhum lançamento para o refund.
    expect((await ledgerDirectionCounts(walletId)).credit).toBe(0);

    // Evento de rejeição persistido na outbox (o 1º defer já tinha publicado o pendência).
    const events = await outboxEventsFor(walletId, refund.transactionId);
    expect(events).toContain('WagerTransactionRejected');
  }, 60_000);
});

describe('tentativas esgotadas', () => {
  test('worker rejeita com REFERENCE_NOT_FOUND', async () => {
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '50.00');
    const refund = await deferReversalPending(playerId, walletId, 'REFUND', crypto.randomUUID());

    await query(
      `UPDATE wager_transaction SET retry_attempts = 2, next_retry_at = now() WHERE id = ?`,
      [refund.transactionId],
    );
    clockNow = new Date(clockNow.getTime() + 60_000);
    await makeWorker({ maxAttempts: 2 }).processDueBatch();

    const refundRow = await txById(refund.transactionId);
    expect(refundRow.status).toBe('REJECTED');
    expect(refundRow.failure_code).toBe('REFERENCE_NOT_FOUND');
  }, 60_000);
});

describe('referência ainda pendente', () => {
  test('ROLLBACK de REFUND pendente renova tentativa sem duplicar evento', async () => {
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '100.00');

    // REFUND pendente sem BET (referência inexistente).
    const refund = await deferReversalPending(playerId, walletId, 'REFUND', crypto.randomUUID());
    clockNow = new Date(clockNow.getTime() + 60_000);
    // ROLLBACK referenciando o REFUND (que também está pendente).
    const rollback = await deferReversalPending(
      playerId,
      walletId,
      'ROLLBACK',
      refund.body.externalTransactionId as string,
    );
    const beforeRenew = await txById(rollback.transactionId);
    expect(beforeRenew.retry_attempts).toBe(0);

    clockNow = new Date(clockNow.getTime() + 60_000);
    await makeWorker().processDueBatch();

    const afterRenew = await txById(rollback.transactionId);
    expect(afterRenew.status).toBe('PENDING_REFERENCE');
    expect(afterRenew.retry_attempts).toBe(1);

    // Renovação: pending_since intacto; next_retry_at no futuro.
    const pendingSince = new Date(String(afterRenew.pending_since));
    expect(pendingSince.getTime()).toBeLessThan(clockNow.getTime());
    expect(new Date(String(afterRenew.next_retry_at)).getTime()).toBeGreaterThan(clockNow.getTime());

    // EXATAMENTE 1 evento de pendência para o ROLLBACK (renovação não emite nada).
    expect(await outboxEventsFor(walletId, rollback.transactionId)).toEqual([
      'WagerTransactionPendingReference',
    ]);

    // A BET da REFUND chega: tudo resolve em cadeia em dois ciclos de worker.
    await submitBet(
      playerId,
      walletId,
      refund.body.referenceExternalTransactionId as string,
    );
    clockNow = new Date(clockNow.getTime() + 5 * 60_000);
    await makeWorker().processDueBatch();

    expect((await txById(refund.transactionId)).status).toBe('PROCESSED');
    expect((await txById(rollback.transactionId)).status).toBe('PROCESSED');
    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});

describe('referência REJECTED', () => {
  test('worker rejeita o refund com INVALID_REFERENCE', async () => {
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '1.00');

    // REFUND pendente apontando para uma BET que ainda não existe...
    const betExt = crypto.randomUUID();
    const refund = await deferReversalPending(playerId, walletId, 'REFUND', betExt);

    // ...e a BET chega REJEITADA (saldo 1.00 insuficiente para 10.00).
    const bet = makeInput(playerId, walletId, { externalTransactionId: betExt });
    const betResult = await postWager(bet.body, bet.key);
    expect(betResult.status).toBe(422);

    clockNow = new Date(clockNow.getTime() + 60_000);
    await makeWorker().processDueBatch();

    const refundRow = await txById(refund.transactionId);
    expect(refundRow.status).toBe('REJECTED');
    expect(refundRow.failure_code).toBe('INVALID_REFERENCE');
  }, 60_000);
});

describe('dois workers concurrentes, 50 pendências', () => {
  test('cada refund aplicado exatamente uma vez (claim SKIP LOCKED)', async () => {
    const walletCount = 10;
    const rounds = 5;
    const wallets: string[] = [];
    const bets: Array<{ playerId: string; walletId: string; ext: string }> = [];
    const refunds: Array<{ playerId: string; walletId: string; body: Record<string, unknown> }> = [];

    for (let wallet = 0; wallet < walletCount; wallet += 1) {
      const playerId = crypto.randomUUID();
      const walletId = await createWallet(playerId, '200.00');
      wallets.push(walletId);
      for (let round = 0; round < rounds; round += 1) {
        const betExt = crypto.randomUUID();
        // Refunds primeiro: pendências já criadas via HTTP (202).
        const refund = await deferReversalPending(playerId, walletId, 'REFUND', betExt);
        refunds.push({ playerId, walletId, body: refund.body });
        // Depois o BET (referência) processa.
        await submitBet(playerId, walletId, betExt);
        bets.push({ playerId, walletId, ext: betExt });
      }
    }
    expect(bets).toHaveLength(walletCount * rounds);
    expect(refunds).toHaveLength(walletCount * rounds);

    // Claim com dois workers concorrentes de lote 25; repete até esvaziar
    // (renovações de backoff devolvem linhas com o avanço do clock fake).
    const workers = [
      makeWorker({ batchSize: 25 }),
      makeWorker({ batchSize: 25 }),
    ];
    for (let iteration = 0; iteration < 10; iteration += 1) {
      clockNow = new Date(clockNow.getTime() + 60_000);
      const claimed = await Promise.all(workers.map((worker) => worker.processDueBatch()));
      if (claimed.every((count) => count === 0)) {
        break;
      }
    }

    for (const { walletId } of refunds) {
      // Todos os refunds desta wallet terminaram PROCESSED (exatamente uma vez).
      const processed = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM wager_transaction
          WHERE kind = 'REFUND' AND wallet_id = ? AND status = 'PROCESSED'`,
        [walletId],
      );
      expect(processed[0]!.count).toBe(rounds);

      const counts = await ledgerDirectionCounts(walletId);
      expect(counts.debit).toBe(rounds);
      expect(counts.credit).toBe(rounds);
      expect((await walletRow(walletId)).balance_amount).toBe('200.00');
      await assertLedgerConsistency(executeShared, walletId);
    }
  }, 120_000);
});

describe('refund chegou pelo SQS e worker resolve', () => {
  test('consumer defere; worker aplica depois da referência', async () => {
    await purgeQueue();
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '70.00');
    const betExt = crypto.randomUUID();

    const externalTransactionId = crypto.randomUUID();
    const idempotencyKey = `${PROVIDER_ID}:${externalTransactionId}`;
    const messageId = `msg-${crypto.randomUUID()}`;
    await enqueue(
      wagerEnvelope(messageId, {
        idempotencyKey,
        providerId: PROVIDER_ID,
        externalTransactionId,
        playerId,
        walletId,
        roundId: 'pending-round',
        gameId: 'pending-game',
        kind: 'REFUND',
        money: { amount: '10.00', currency: 'BRL' },
        referenceExternalTransactionId: betExt,
      }),
      walletId,
      messageId,
    );

    // Consumer defere: PENDING_REFERENCE pela mesma ordem do use case.
    const consumer = makeConsumer();
    expect(await consumer.cycle()).toBe(1);
    const deferred = await query<Array<{ status: string }>>(
      `SELECT status FROM wager_transaction WHERE idempotency_key = ?`,
      [idempotencyKey],
    );
    expect(deferred[0]!.status).toBe('PENDING_REFERENCE');

    // A referência chega via HTTP (BET) e worker resolve o refund.
    await submitBet(playerId, walletId, betExt);
    clockNow = new Date(clockNow.getTime() + 60_000);
    await makeWorker().processDueBatch();

    const refundRow = await query<Array<{ status: string; observed_balance_amount: string }>>(
      `SELECT status, observed_balance_amount FROM wager_transaction WHERE idempotency_key = ?`,
      [idempotencyKey],
    );
    expect(refundRow[0]!.status).toBe('PROCESSED');
    expect(refundRow[0]!.observed_balance_amount).toBe('70.00');

    // Envelope POST-202 via SQS foi acked no commit do consumer (PENDING_REFERENCE
    // é resultado da submissão — a mensagem foi deletada na hora certa).
    expect(await drainQueue()).toBe(0);
    expect((await walletRow(walletId)).balance_amount).toBe('70.00');
    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});
