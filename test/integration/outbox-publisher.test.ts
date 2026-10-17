import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  type QueueAttributeName,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { AddressInfo } from 'node:net';

import type { PublishRequest } from '../../src/application/ports';
import { AppModule } from '../../src/app.module';
import type { IntegrationEventProps } from '../../src/domain/events';
import { IntegrationEvent } from '../../src/domain/events';
import { assertLedgerConsistency as sharedAssertLedgerConsistency } from './ledger-consistency';
import type { OutboxMessage } from '../../src/domain/outbox-message';
import { MikroOrmUnitOfWork } from '../../src/infra/persistence/unit-of-work';
import { SqsEventPublisher } from '../../src/infra/sqs/sqs-event-publisher';
import {
  OutboxPublisherWorker,
  type OutboxPublisherOptions,
} from '../../src/infra/workers/outbox-publisher';

/**
 * Fatia 7a — publisher da outbox contra PostgreSQL e LocalStack reais.
 *
 * Nenhum worker roda em loop por padrão (preload fixa `WORKERS_ENABLED=api`);
 * os testes instanciam `OutboxPublisherWorker` com configuração explícita e
 * filtram as mensagens recebidas da fila pelos `aggregateId` do teste.
 */

const EVENTS_QUEUE_NAME = process.env.SQS_EVENTS_QUEUE ?? 'wager-events.fifo';

interface WalletBody {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

interface SubmitBody {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  failureCode?: string;
}

interface FinancialSnapshot {
  balanceAmount: string;
  balanceCurrency: string;
  version: number;
  ledgerCount: number;
  ledgerSum: string;
  txCount: number;
}

interface Envelope {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;
  version: number;
  data: Record<string, unknown>;
}

interface ReceivedMessage extends Partial<Envelope> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  messageGroupId?: string;
  parsed: Envelope;
}

interface SeedEvent {
  eventId: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  occurredAt: Date;
}

// Evento de teste (subclasse concreta do envelope abstrato).
class TestEvent extends IntegrationEvent<Record<string, unknown>> {
  override readonly eventType = 'TestIntegrationEvent';
  override readonly version = 1;

  constructor(props: IntegrationEventProps<Record<string, unknown>>) {
    super(props);
  }
}

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;
let unitOfWork: MikroOrmUnitOfWork;
let sqs: SQSClient;
let publisher: SqsEventPublisher;
let queueUrl: string;

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

function workerOptions(overrides: Partial<OutboxPublisherOptions> = {}): OutboxPublisherOptions {
  return {
    enabled: true,
    pollIntervalMs: 100,
    batchSize: 50,
    leaseMs: 60_000,
    ...overrides,
  };
}

/** Linha nova da outbox com envelope estável, única por aggregate/event. */
function seedEvent(occurredAt: Date): SeedEvent {
  const eventId = crypto.randomUUID();
  const aggregateId = crypto.randomUUID();
  return {
    eventId,
    aggregateId,
    eventType: 'WalletBalanceChanged',
    occurredAt,
    payload: {
      eventId,
      eventType: 'WalletBalanceChanged',
      aggregateId,
      correlationId: `corr-${eventId}`,
      occurredAt: occurredAt.toISOString(),
      version: 1,
      data: {
        walletId: aggregateId,
        transactionId: crypto.randomUUID(),
        direction: 'DEBIT',
        money: { amount: '1.00', currency: 'BRL' },
        balanceBefore: { amount: '0.00', currency: 'BRL' },
        balanceAfter: { amount: '1.00', currency: 'BRL' },
        walletVersion: 1,
      },
    },
  };
}

async function seedOutbox(events: SeedEvent[]): Promise<void> {
  for (const event of events) {
    await query(
      `INSERT INTO outbox_message (id, event_id, aggregate_id, event_type, payload, occurred_at)
       VALUES (?, ?, ?, ?, ?::jsonb, ?)`,
      [
        crypto.randomUUID(),
        event.eventId,
        event.aggregateId,
        event.eventType,
        JSON.stringify(event.payload),
        event.occurredAt,
      ],
    );
  }
}

/** Campos Money dentro de `data` precisam chegar como MoneyProps (strings). */
function moneyPropsCount(data: Record<string, unknown>): number {
  let count = 0;
  for (const value of Object.values(data)) {
    if (value !== null && typeof value === 'object') {
      const record = value as Record<string, unknown>;
      if (typeof record.amount === 'string' && typeof record.currency === 'string') {
        count += 1;
      }
    }
  }
  return count;
}

/**
 * Lê a fila LocalStack, filtra pelos aggregateIds do teste e apaga o que
 * recebeu (mensagens de outros testes/rodadas são descartadas com segurança:
 * os asserts sempre olham só para os UUIDs próprios).
 */
async function drainQueue(
  aggregates: Set<string>,
  expected: number,
  timeoutMs = 30_000,
): Promise<ReceivedMessage[]> {
  const received: ReceivedMessage[] = [];
  const deadline = Date.now() + (expected === 0 ? 2_500 : timeoutMs);
  while (Date.now() < deadline && received.length < expected) {
    const response = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        // MessageGroupId só existe em filas FIFO; o enum do SDK não a lista.
        AttributeNames: ['All', 'MessageGroupId' as QueueAttributeName],
      }),
    );
    for (const message of response.Messages ?? []) {
      const parsed = JSON.parse(message.Body ?? '{}') as Envelope;
      await sqs.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }),
      );
      if (aggregates.has(parsed.aggregateId)) {
        received.push({
          ...parsed,
          messageGroupId: message.Attributes?.MessageGroupId,
          parsed,
        });
      }
    }
  }
  return received;
}

/** Wrapper fino: delega ao helper compartilhado (versão forte, com last_after). */
async function assertLedgerConsistency(walletId: string): Promise<void> {
  await sharedAssertLedgerConsistency((sql, params) => query(sql, params), walletId);
}

async function snapshotFinancials(walletId: string): Promise<FinancialSnapshot> {
  const wallets = await query<
    Array<{ balance_amount: string; balance_currency: string; version: number }>
  >(`SELECT balance_amount, balance_currency, version FROM wallet WHERE id = ?`, [walletId]);
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
  const wallet = wallets[0]!;
  return {
    balanceAmount: wallet.balance_amount,
    balanceCurrency: wallet.balance_currency,
    version: wallet.version,
    ledgerCount: ledger[0]!.count,
    ledgerSum: ledger[0]!.sum,
    txCount: transactions[0]!.count,
  };
}

async function outboxEventIds(aggregateId: string): Promise<string[]> {
  const rows = await query<Array<{ event_id: string }>>(
    `SELECT event_id FROM outbox_message WHERE aggregate_id = ?`,
    [aggregateId],
  );
  return rows.map((row) => row.event_id);
}

/** Nenhuma linha desses aggregates pode seguir pendente. */
async function assertAllPublished(aggregateIds: string[]): Promise<void> {
  const placeholders = aggregateIds.map(() => '?').join(', ');
  const rows = await query<Array<{ pending_count: number; total: number }>>(
    `SELECT count(*) FILTER (WHERE published_at IS NULL)::int AS pending_count,
            count(*)::int AS total
       FROM outbox_message WHERE aggregate_id IN (${placeholders})`,
    aggregateIds,
  );
  if (aggregateIds.length === 1) {
    // fluxo e morte testam aggregates próprios: deve existir pelo menos 1 evento
    expect(rows[0]!.total).toBeGreaterThan(0);
  }
  expect(rows[0]!.pending_count).toBe(0);
}

/** `next_attempt_at` pode virar string no execute cru — normaliza para Date. */
function asDate(value: Date | string): Date {
  if (value instanceof Date) {
    return value;
  }
  return new Date(String(value).replace(' ', 'T').replace('+00', 'Z').replace('+0000', 'Z'));
}

async function pendingCount(aggregateIds: string[]): Promise<number> {
  const placeholders = aggregateIds.map(() => '?').join(', ');
  const rows = await query<Array<{ pending_count: number }>>(
    `SELECT count(*) FILTER (WHERE published_at IS NULL)::int AS pending_count
       FROM outbox_message WHERE aggregate_id IN (${placeholders})`,
    aggregateIds,
  );
  return rows[0]!.pending_count;
}

/**
 * Roda o worker até as linhas dos aggregates dados estarem todas publicadas.
 * Resíduos pendentes de outros testes ocupam os primeiros lotes: cicla com
 * deadline em vez de um único `processDueBatch`.
 */
async function flushViaWorker(
  worker: OutboxPublisherWorker,
  aggregateIds: string[],
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if ((await pendingCount(aggregateIds)) === 0) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`timeout: outbox de ${aggregateIds.length} aggregates não esvaziou`);
    }
    await worker.processDueBatch();
    await Bun.sleep(30);
  }
}

/**
 * Faz claims em ciclos até ter todas as linhas dos eventIds dados (com lease
 * definido). Devolve apenas as linhas pedidas — resíduos claimados pelo caminho
 * ficam com o lease vigente e voltam a ficar vencidas depois.
 */
async function claimOurs(
  eventIds: Set<string>,
  leaseMs: number,
  timeoutMs = 30_000,
): Promise<OutboxMessage[]> {
  const claimed: OutboxMessage[] = [];
  const deadline = Date.now() + timeoutMs;
  while (claimed.length < eventIds.size && Date.now() < deadline) {
    const claims = await unitOfWork.run((repositories) =>
      repositories.outbox.claimDue(50, leaseMs),
    );
    for (const message of claims) {
      if (eventIds.has(message.eventId)) {
        claimed.push(message);
      }
    }
  }
  if (claimed.length < eventIds.size) {
    throw new Error(`timeout: claim não achou as ${eventIds.size} linhas pedidas`);
  }
  return claimed;
}

async function postWallet(playerId: string, amount: string): Promise<WalletBody> {
  const response = await fetch(`${baseUrl}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ playerId, initialBalance: { amount, currency: 'BRL' } }),
  });
  expect(response.status).toBe(201);
  return (await response.json()) as WalletBody;
}

async function postBet(
  playerId: string,
  walletId: string,
  amount: string,
): Promise<SubmitBody> {
  const externalTransactionId = crypto.randomUUID();
  const response = await fetch(`${baseUrl}/wagering/transactions`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'idempotency-key': `provider-a:${externalTransactionId}`,
    },
    body: JSON.stringify({
      providerId: 'provider-a',
      externalTransactionId,
      playerId,
      walletId,
      roundId: 'round-1',
      gameId: 'game-1',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    }),
  });
  expect(response.status).toBe(201);
  return (await response.json()) as SubmitBody;
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
  publisher = new SqsEventPublisher(sqs, EVENTS_QUEUE_NAME);
  const url = await sqs.send(new GetQueueUrlCommand({ QueueName: EVENTS_QUEUE_NAME }));
  queueUrl = url.QueueUrl!;

  // Ambiente determinístico entre rodadas: limpa a fila (só este arquivo consome)
  // e marca resíduos pendentes de rodadas antigas como publicados — os testes
  // desta suíte nunca dependem de linhas de outros tests.
  await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
  await query(`UPDATE outbox_message SET published_at = now() WHERE published_at IS NULL`);
}, 60_000);

afterAll(async () => {
  await app.close();
});

describe('atomicidade: outbox só sobrevive ao commit', () => {
  test('transação com erro transitório → rollback não deixa linha na outbox nem mensagem na fila', async () => {
    const aggregateId = crypto.randomUUID();
    const eventId = crypto.randomUUID();

    await expect(
      unitOfWork.run(async (repositories) => {
        await repositories.outbox.insert([
          new TestEvent({
            eventId,
            aggregateId,
            correlationId: 'corr-rollback',
            occurredAt: new Date(),
            data: { money: { amount: '1.00', currency: 'BRL' } },
          }),
        ]);
        // erro transitório forçado → rollback da tx inteira
        throw new Error('erro transitório forçado');
      }),
    ).rejects.toThrow('erro transitório forçado');

    const rows = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM outbox_message WHERE event_id = ?`,
      [eventId],
    );
    expect(rows[0]!.count).toBe(0);

    const queue = await drainQueue(new Set([aggregateId]), 0);
    expect(queue).toHaveLength(0);
  }, 30_000);
});

describe('fluxo end-to-end: POST /wallets + BET publicados', () => {
  test('publisher publica os 4 eventos e a fila recebe envelopes com MoneyProps string', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '1000.00');
    const walletId = wallet.id;

    const bet = await postBet(playerId, walletId, '10.00');
    expect(bet.balance).toEqual({ amount: '990.00', currency: 'BRL' });
    await assertLedgerConsistency(walletId);

    // publisher: claim (tx curta) → publicação fora da tx → markPublished
    const worker = new OutboxPublisherWorker(unitOfWork, publisher, workerOptions());
    await flushViaWorker(worker, [walletId]);

    await assertAllPublished([walletId]);

    const queue = await drainQueue(new Set([walletId]), 4);
    expect(new Set(queue.map((message) => message.eventId))).toEqual(
      new Set(await outboxEventIds(walletId)),
    );
    for (const message of queue) {
      expect(message.messageGroupId).toBe(walletId);
      expect(message.version).toBe(1);
      expect(message.aggregateId).toBe(walletId);
      expect(message.correlationId).toBeTruthy();
      expect(message.occurredAt).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/);
      // data com MoneyProps (strings), nunca numbers
      expect(moneyPropsCount(message.parsed.data)).toBeGreaterThanOrEqual(1);
    }

    const betProcessed = queue.find(
      (message) =>
        message.eventType === 'WagerTransactionProcessed' && message.parsed.data.kind === 'BET',
    );
    expect(betProcessed).toBeDefined();
    expect(betProcessed!.parsed.data.money).toEqual({ amount: '10.00', currency: 'BRL' });
    expect(betProcessed!.parsed.data.observedBalance).toEqual({
      amount: '990.00',
      currency: 'BRL',
    });

    const openingProcessed = queue.find(
      (message) =>
        message.eventType === 'WagerTransactionProcessed' && message.parsed.data.kind === 'OPENING',
    );
    expect(openingProcessed).toBeDefined();
    expect(openingProcessed!.parsed.data.observedBalance).toEqual({
      amount: '1000.00',
      currency: 'BRL',
    });

    const balanceEvents = queue.filter(
      (message) => message.eventType === 'WalletBalanceChanged',
    );
    expect(balanceEvents).toHaveLength(2);

    await assertLedgerConsistency(walletId);
  }, 60_000);
});

describe('dois publishers concorrentes sobre 200 linhas', () => {
  test('claims disjuntos, 200 publicadas e 200 eventIds na fila', async () => {
    // ocorred_at determinístico e anterior a qualquer linha de outro teste
    const firstOccurrence = Date.now() - 30_000;
    const events = Array.from({ length: 200 }, (_, index) =>
      seedEvent(new Date(firstOccurrence + index)),
    );
    const aggregates = events.map((event) => event.aggregateId);
    await seedOutbox(events);

    const pending = new Set(events.map((event) => event.eventId));
    const deadline = Date.now() + 90_000;

    while (pending.size > 0 && Date.now() < deadline) {
      // dois claims em paralelo: SKIP LOCKED → conjuntos disjuntos
      const [claimA, claimB] = await Promise.all([
        unitOfWork.run((repositories) => repositories.outbox.claimDue(100, 60_000)),
        unitOfWork.run((repositories) => repositories.outbox.claimDue(100, 60_000)),
      ]);

      const idsA = new Set(claimA.map((message) => message.eventId));
      const idsB = new Set(claimB.map((message) => message.eventId));
      for (const id of idsA) {
        expect(idsB.has(id)).toBe(false);
      }

      for (const message of [...claimA, ...claimB]) {
        await publisher.publish({
          eventId: message.eventId,
          aggregateId: message.aggregateId,
          payload: JSON.stringify(message.payload),
        });
        await unitOfWork.run((repositories) => repositories.outbox.markPublished(message.id));
        pending.delete(message.eventId);
      }
    }

    expect(pending.size).toBe(0);
    await assertAllPublished(aggregates);

    // LocalStack sob a carga de processos da 7d atrasa entregas: margem 90s
    // (a quiescência no DB já foi provada acima; aqui é só lag de fila).
    const queue = await drainQueue(new Set(aggregates), 200, 90_000);
    const queuedIds = new Set(queue.map((message) => message.eventId));
    expect(queuedIds.size).toBe(200);
    for (const event of events) {
      expect(queuedIds.has(event.eventId)).toBe(true);
    }
    for (const message of queue) {
      expect(message.messageGroupId).toBe(message.aggregateId);
    }
  }, 120_000);
});

describe('morte entre o claim e a publicação', () => {
  test('lease expira e outro publisher assume sem perder evento', async () => {
    const now = Date.now();
    const events = [seedEvent(new Date(now)), seedEvent(new Date(now + 1)), seedEvent(new Date(now + 2))];
    await seedOutbox(events);
    const eventIds = events.map((event) => event.eventId);
    const aggregateIds = events.map((event) => event.aggregateId);
    const ours = (claimed: { eventId: string }[]) =>
      claimed.filter((message) => eventIds.includes(message.eventId));

    // "processo morre": claim com lease curto (mesma SQL do worker) e nunca publica
    const dead = await claimOurs(new Set(eventIds), 1_500);
    expect(dead).toHaveLength(3);

    // lease ainda válido: novo claim não pega as três linhas
    const early = await unitOfWork.run((repositories) => repositories.outbox.claimDue(60, 100));
    expect(ours(early)).toHaveLength(0);

    // lease expira → outro publisher assume e publica
    await Bun.sleep(1_800);
    const worker = new OutboxPublisherWorker(unitOfWork, publisher, workerOptions());
    await flushViaWorker(worker, aggregateIds);

    await assertAllPublished(aggregateIds);

    const queue = await drainQueue(new Set(aggregateIds), 3);
    expect(new Set(queue.map((message) => message.eventId))).toEqual(new Set(eventIds));
  }, 60_000);
});

describe('falha de publicação: backoff do domínio', () => {
  const failingPublisher = {
    publish: async () => {
      throw new Error('SQS indisponível');
    },
  };

  test('attempts incrementa, next_attempt_at avança conforme o backoff e a linha segue pendente', async () => {
    const event = seedEvent(new Date());
    await seedOutbox([event]);

    const worker = new OutboxPublisherWorker(unitOfWork, failingPublisher, workerOptions());

    // cicla até a linha do teste ser claimada (resíduos podem ocupar o lote);
    // firstFailureAt é medido logo antes do ciclo que vai gerar a falha
    let firstFailureAt = 0;
    for (;;) {
      if (await attemptsOf(event.eventId).then((a) => a >= 1)) {
        break;
      }
      firstFailureAt = Date.now();
      await worker.processDueBatch();
      await Bun.sleep(30);
    }

    const row = await readOutboxRow(event.eventId);
    expect(row!.attempts).toBe(1);
    expect(row!.published_at).toBeNull();
    // backoff da tentativa 1: 2s (vindo de OutboxMessage.scheduleRetry)
    const firstDelay = asDate(row!.next_attempt_at).getTime() - firstFailureAt;
    expect(firstDelay).toBeGreaterThan(1_500);
    expect(firstDelay).toBeLessThan(4_000);

    // ainda não vencida → claim imediato não pega
    const early = await unitOfWork.run((repositories) => repositories.outbox.claimDue(60, 10_000));
    expect(new Set(early.map((message) => message.eventId)).has(event.eventId)).toBe(false);

    // vencida (2s + folga) → segunda falha: attempts=2, backoff 4s
    await Bun.sleep(2_300);
    let secondFailureAt = 0;
    for (;;) {
      if (await attemptsOf(event.eventId).then((a) => a >= 2)) {
        break;
      }
      secondFailureAt = Date.now();
      await worker.processDueBatch();
      await Bun.sleep(30);
    }

    const retryRow = await readOutboxRow(event.eventId);
    expect(retryRow!.attempts).toBe(2);
    expect(retryRow!.published_at).toBeNull();
    // backoff da tentativa 2: 4s
    const secondDelay = asDate(retryRow!.next_attempt_at).getTime() - secondFailureAt;
    expect(secondDelay).toBeGreaterThan(3_000);
    expect(secondDelay).toBeLessThan(6_000);

    // limpeza: não deixar a linha falha como resíduo para as próximas rodadas
    await query(`UPDATE outbox_message SET published_at = now() WHERE event_id = ?`, [
      event.eventId,
    ]);
  }, 60_000);
});

async function attemptsOf(eventId: string): Promise<number> {
  const rows = await query<Array<{ attempts: number }>>(
    `SELECT attempts FROM outbox_message WHERE event_id = ?`,
    [eventId],
  );
  return rows[0]?.attempts ?? 0;
}

interface OutboxRow {
  attempts: number;
  next_attempt_at: Date | string;
  published_at: Date | string | null;
}

async function readOutboxRow(eventId: string): Promise<OutboxRow | undefined> {
  const rows = await query<Array<OutboxRow>>(
    `SELECT attempts, next_attempt_at, published_at FROM outbox_message WHERE event_id = ?`,
    [eventId],
  );
  return rows[0];
}

describe('publicação duplicada é segura', () => {
  test('mesmo eventId publicado duas vezes não muda os dados financeiros', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '200.00');
    await postBet(playerId, wallet.id, '30.00');
    const before = await snapshotFinancials(wallet.id);

    const payloadRows = await query<Array<{ event_id: string; payload: Record<string, unknown> }>>(
      `SELECT event_id, payload FROM outbox_message WHERE aggregate_id = ? LIMIT 1`,
      [wallet.id],
    );
    expect(payloadRows).toHaveLength(1);
    const request: PublishRequest = {
      eventId: payloadRows[0]!.event_id,
      aggregateId: wallet.id,
      payload: JSON.stringify(payloadRows[0]!.payload),
    };

    // dupla publicação com o mesmo eventId (MessageDeduplicationId igual na fila)
    await publisher.publish(request);
    await publisher.publish(request);
    await Bun.sleep(500);

    const after = await snapshotFinancials(wallet.id);
    expect(after).toEqual(before);
    await assertLedgerConsistency(wallet.id);
  }, 60_000);
});
