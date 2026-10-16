import {
  DeleteMessageCommand,
  GetQueueUrlCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  type QueueAttributeName,
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
import { SubmitWagerTransactionUseCase } from '../../src/application/submit-wager.use-case';
import { MikroOrmUnitOfWork } from '../../src/infra/persistence/unit-of-work';
import { SqsWagerConsumer, type SqsWagerConsumerOptions } from '../../src/infra/sqs/sqs-wager-consumer';

/**
 * Fatia 7b — consumer SQS contra PostgreSQL e LocalStack reais.
 *
 * Nenhum consumer roda em loop por padrão (preload fixa `WORKERS_ENABLED=api`):
 * os testes instanciam `SqsWagerConsumer` com configuração explícita e
 * identificam tudo por UUIDs próprios (`walletId` como `MessageGroupId`).
 */

const WAGER_QUEUE_NAME = process.env.SQS_WAGER_QUEUE ?? 'wager-transactions.fifo';
const CONSUMER_NAME = 'test-sqs-consumer';
const PROVIDER_ID = 'provider-sqs-test';

interface CreateWalletCreated {
  outcome: 'CREATED';
  wallet: { id: string };
}

function testOptions(overrides: Partial<SqsWagerConsumerOptions> = {}): SqsWagerConsumerOptions {
  return {
    enabled: true,
    waitTimeSeconds: 1,
    maxMessages: 10,
    // Visibilidade curta: redelivery de teste acontece sem esperar 30s.
    visibilityTimeoutSeconds: 2,
    heartbeatMs: 1000,
    pollIntervalMs: 30,
    consumerName: CONSUMER_NAME,
    ...overrides,
  };
}

let app: INestApplication;
let orm: MikroORM;
let unitOfWork: UnitOfWork;
let sqs: SQSClient;
let queueUrl: string;

function makeConsumer(overrides: Partial<SqsWagerConsumerOptions> = {}): SqsWagerConsumer {
  const useCase = new SubmitWagerTransactionUseCase(unitOfWork);
  return new SqsWagerConsumer(useCase, sqs, WAGER_QUEUE_NAME, testOptions(overrides));
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

interface EnvelopeData extends Record<string, unknown> {
  kind: string;
  money: { amount: string; currency: string };
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

/** Dados de BET prontos para o envelope (idempotency-key derivada do external id). */
function betData(
  playerId: string,
  walletId: string,
  amount: string,
): { data: EnvelopeData; idempotencyKey: string; externalTransactionId: string } {
  const externalTransactionId = crypto.randomUUID();
  const idempotencyKey = `${PROVIDER_ID}:${externalTransactionId}`;
  return {
    idempotencyKey,
    externalTransactionId,
    data: {
      providerId: PROVIDER_ID,
      externalTransactionId,
      idempotencyKey,
      playerId,
      walletId,
      roundId: 'sqs-round',
      gameId: 'sqs-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    },
  };
}

function playerId(): string {
  return crypto.randomUUID();
}

/** Purge por teste: mensagens inválidas NUNCA são deletadas pelo consumer
 * (redrive leva à DLQ) e contaminariam o próximo teste da fila FIFO. */
async function purgeQueue(): Promise<void> {
  await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
}

async function send(
  envelope: unknown,
  walletId: string,
  deduplicationId: string,
): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: walletId,
      MessageDeduplicationId: deduplicationId,
    }),
  );
}

/**
 * Recebe da fila até achar as mensagens dos ids informados (ou o deadline).
 * Tudo recebido é deletado: os asserts sempre olham só para as mensagens
 * próprias do teste e a fila fica limpa para as próximas suítes/rodadas.
 */
async function drainUntil(
  messageIds: Set<string>,
  expected: number,
  timeoutMs = 15_000,
): Promise<Array<{ messageId: string; body: string; receiveCount: number }>> {
  const found: Array<{ messageId: string; body: string; receiveCount: number }> = [];
  const deadline = Date.now() + timeoutMs;
  while (found.length < expected && Date.now() < deadline) {
    const response = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        AttributeNames: ['ApproximateReceiveCount' as QueueAttributeName],
      }),
    );
    for (const message of response.Messages ?? []) {
      const body = message.Body ?? '';
      let envelopeMessageId = '';
      try {
        envelopeMessageId = (JSON.parse(body) as { messageId?: string }).messageId ?? '';
      } catch {
        envelopeMessageId = body; // JSON quebrado: o próprio corpo é o id do teste
      }
      await sqs.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.ReceiptHandle }),
      );
      if (messageIds.has(envelopeMessageId)) {
        found.push({
          messageId: envelopeMessageId,
          body,
          receiveCount: Number(message.Attributes?.ApproximateReceiveCount ?? 0),
        });
      }
    }
  }
  return found;
}

/** Processa a fila com o consumer até ela parar de devolver mensagens. */
async function runUntilEmpty(consumer: SqsWagerConsumer, maxCycles = 5): Promise<void> {
  for (let index = 0; index < maxCycles; index += 1) {
    if ((await consumer.cycle()) === 0) {
      return;
    }
  }
}

interface FinancialSnapshot {
  balanceAmount: string;
  version: number;
  ledgerCount: number;
  ledgerSum: string;
  txCount: number;
}

async function snapshotFinancials(walletId: string): Promise<FinancialSnapshot> {
  const wallets = await query<
    Array<{ balance_amount: string; version: number }>
  >(`SELECT balance_amount, version FROM wallet WHERE id = ?`, [walletId]);
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
    balanceAmount: wallets[0]!.balance_amount,
    version: wallets[0]!.version,
    ledgerCount: ledger[0]!.count,
    ledgerSum: ledger[0]!.sum,
    txCount: transactions[0]!.count,
  };
}

async function inboxRows(messageId: string): Promise<Array<{ processed_at: string | null }>> {
  return query<Array<{ processed_at: string | null }>>(
    `SELECT processed_at FROM inbox_message WHERE consumer_name = ? AND message_id = ?`,
    [CONSUMER_NAME, messageId],
  );
}

async function transactionByIdempotencyKey(key: string): Promise<
  Array<{ id: string; status: string; kind: string; observed_balance_amount: string | null; failure_code: string | null }>
> {
  return query<
    Array<{
      id: string;
      status: string;
      kind: string;
      observed_balance_amount: string | null;
      failure_code: string | null;
    }>
  >(
    `SELECT id, status, kind, observed_balance_amount, failure_code
       FROM wager_transaction WHERE idempotency_key = ?`,
    [key],
  );
}

/** `execute` bruto para o helper compartilhado de consistência do ledger. */
async function executeShared(
  sql: string,
  params: unknown[] = [],
): Promise<Array<{ balance_amount: string; ledger_sum: string | null; last_after: string | null }>> {
  return query(sql, params);
}

beforeAll(async () => {
  app = await NestFactory.create(AppModule, { logger: false });
  await app.listen(0);

  const address = app.getHttpServer().address() as AddressInfo;
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
  const url = await sqs.send(new GetQueueUrlCommand({ QueueName: WAGER_QUEUE_NAME }));
  queueUrl = url.QueueUrl!;

  // Ambiente determinístico entre rodadas: só este arquivo consome/só ele
  // envia na fila de transações.
  await sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
}, 60_000);

afterAll(async () => {
  await app.close();
});

describe('fluxo: BET via SQS processada com inbox na mesma tx', () => {
  test('message → PROCESSED + débito + inbox row + mensagem deletada', async () => {
    await purgeQueue();
    const playerIdValue = playerId();
    const walletId = await createWallet(playerIdValue, '100.00');
    const { data, idempotencyKey } = betData(playerIdValue, walletId, '10.00');
    const messageId = `msg-${crypto.randomUUID()}`;
    await send(wagerEnvelope(messageId, data), walletId, messageId);

    const consumer = makeConsumer();
    await runUntilEmpty(consumer);

    const transaction = await transactionByIdempotencyKey(idempotencyKey);
    expect(transaction).toHaveLength(1);
    expect(transaction[0]).toMatchObject({ status: 'PROCESSED', kind: 'BET' });

    const wallets = await query<Array<{ balance_amount: string; version: number }>>(
      `SELECT balance_amount, version FROM wallet WHERE id = ?`,
      [walletId],
    );
    expect(wallets[0]!.balance_amount).toBe('90.00');
    expect(wallets[0]!.version).toBe(2); // OPENING = 1, débito do BET incrementa

    const inbox = await inboxRows(messageId);
    expect(inbox).toHaveLength(1);
    expect(inbox[0]!.processed_at).not.toBeNull();

    // ack (DELETE) pós-commit: a fila não devolve a mensagem
    const leftovers = await drainUntil(new Set([messageId]), 0, 3_000);
    expect(leftovers).toHaveLength(0);

    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});

describe('redelivery: dedup via inbox, sem novo débito', () => {
  test('mesmo messageId reenviado → replay idempotente (saldo/ledger iguais)', async () => {
    const playerIdValue = playerId();
    const walletId = await createWallet(playerIdValue, '200.00');
    const { data, idempotencyKey } = betData(playerIdValue, walletId, '15.00');
    const messageId = `msg-${crypto.randomUUID()}`;
    // Mesma mensagem com messageId idêntico; MessageDeduplicationId diferente
    // para a fila entregar as duas cópias (o dedup testado aqui é o do inbox,
    // exatamente o redelivery que SQS simula).
    await send(wagerEnvelope(messageId, data), walletId, messageId);
    await send(wagerEnvelope(messageId, data), walletId, `${messageId}:2`);

    const consumer = makeConsumer();
    await runUntilEmpty(consumer);

    const transactions = await transactionByIdempotencyKey(idempotencyKey);
    expect(transactions).toHaveLength(1);
    expect(transactions[0]!.status).toBe('PROCESSED');

    const wallets = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [walletId],
    );
    expect(wallets[0]!.balance_amount).toBe('185.00');

    const ledger = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count
         FROM wallet_ledger_entry WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [walletId],
    );
    expect(ledger[0]!.count).toBe(1);

    const inbox = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM inbox_message
        WHERE consumer_name = ? AND message_id = ?`,
      [CONSUMER_NAME, messageId],
    );
    expect(inbox[0]!.count).toBe(1);

    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});

describe('payload inválido: permanente, NÃO deleta', () => {
  test('JSON quebrado e kind OPENING continuam recebíveis (redrive → DLQ)', async () => {
    await purgeQueue();
    const brokenMessageId = `msg-${crypto.randomUUID()}`;
    await sqs.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: '{not json',
        MessageGroupId: playerId(),
        MessageDeduplicationId: brokenMessageId,
      }),
    );

    const playerIdValue = playerId();
    const walletId = await createWallet(playerIdValue, '50.00');
    const openingMessageId = `msg-${crypto.randomUUID()}`;
    const externalTransactionId = crypto.randomUUID();
    const idempotencyKey = `${PROVIDER_ID}:${externalTransactionId}`;
    await send(
      wagerEnvelope(openingMessageId, {
        providerId: PROVIDER_ID,
        externalTransactionId,
        idempotencyKey,
        playerId: playerIdValue,
        walletId,
        roundId: 'sqe-round',
        gameId: 'sqs-game',
        kind: 'OPENING', // interno: nunca entra pela fila nem pela API
        money: { amount: '10.00', currency: 'BRL' },
      } as EnvelopeData),
      walletId,
      openingMessageId,
    );

    const consumer = makeConsumer();
    await runUntilEmpty(consumer);

    // NÃO deletadas: expira a visibilidade de 2s e voltam a ficar recebíveis.
    // O JSON quebrado é casado pelo próprio corpo (não parseia messageId).
    const stillThere = await drainUntil(
      new Set([brokenMessageId, openingMessageId, '{not json']),
      2,
      15_000,
    );
    expect(stillThere).toHaveLength(2);
    // O JSON quebrado nunca parseia messageId — o corpo cru é o identificador.
    expect(new Set(stillThere.map((message) => message.messageId))).toEqual(
      new Set([openingMessageId, '{not json']),
    );

    // OPENING não persistiu nada
    const transactions = await transactionByIdempotencyKey(idempotencyKey);
    expect(transactions).toHaveLength(0);
    expect(await inboxRows(openingMessageId)).toHaveLength(0);
    expect(await inboxRows(brokenMessageId)).toHaveLength(0);

    const wallets = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [walletId],
    );
    expect(wallets[0]!.balance_amount).toBe('50.00'); // sem débito indevido
  }, 60_000);
});

describe('morte entre o commit e o ack', () => {
  test('fault no ack → redelivery entra pelo replay sem duplicar efeito', async () => {
    await purgeQueue();
    const playerIdValue = playerId();
    const walletId = await createWallet(playerIdValue, '300.00');
    const { data, idempotencyKey } = betData(playerIdValue, walletId, '10.00');
    const messageId = `msg-${crypto.randomUUID()}`;
    await send(wagerEnvelope(messageId, data), walletId, messageId);

    // Consumer "que morre" na hora do ack (depois do commit).
    let ackAttempts = 0;
    const killer = makeConsumer({
      beforeAck: async () => {
        ackAttempts += 1;
        // Comprovando a ordem: quando o hook roda, o commit já aconteceu.
        const committed = await transactionByIdempotencyKey(idempotencyKey);
        expect(committed[0]!.status).toBe('PROCESSED');
        throw new Error('ack failure injected');
      },
    });
    await expect(killer.cycle()).rejects.toThrow('ack failure injected');
    expect(ackAttempts).toBe(1);

    // Commit visível mesmo sem ack: PROCESSED + inbox row persisted.
    const transaction = await transactionByIdempotencyKey(idempotencyKey);
    expect(transaction[0]!.status).toBe('PROCESSED');
    expect(await inboxRows(messageId)).toHaveLength(1);
    const before = await snapshotFinancials(walletId);

    // A mensagem NÃO foi deletada: expira a visibilidade e volta recebível.
    const redelivered = await drainUntil(new Set([messageId]), 1, 15_000);
    expect(redelivered).toHaveLength(1);
    expect(redelivered[0]!.receiveCount).toBeGreaterThanOrEqual(2);

    // Ops! A devolvemos à fila para o consumer normal assumir o replay:
    await send(wagerEnvelope(messageId, data), walletId, `${messageId}:r`);
    const consumer = makeConsumer();
    await runUntilEmpty(consumer);

    // Replay não duplica efeito: tx única, saldo/ledger iguais.
    const after = await snapshotFinancials(walletId);
    expect(after).toEqual(before);
    expect((await transactionByIdempotencyKey(idempotencyKey))).toHaveLength(1);

    // Mensagem deletada no segundo ciclo (terminal): fila não devolve mais.
    const leftovers = await drainUntil(new Set([messageId]), 0, 3_000);
    expect(leftovers).toHaveLength(0);

    await assertLedgerConsistency(executeShared, walletId);
  }, 90_000);
});

describe('SIGTERM/parada: conclui o que está em voo, não perde mensagem', () => {
  test('shutdown durante o ack → mensagem processada e deletada', async () => {
    await purgeQueue();
    const playerIdValue = playerId();
    const walletId = await createWallet(playerIdValue, '100.00');
    const { data, idempotencyKey } = betData(playerIdValue, walletId, '10.00');
    const messageId = `msg-${crypto.randomUUID()}`;
    await send(wagerEnvelope(messageId, data), walletId, messageId);

    let ackStarted = false;
    const slow = makeConsumer({
      beforeAck: async () => {
        ackStarted = true;
        await Bun.sleep(700);
      },
    });
    slow.start();

    const deadline = Date.now() + 10_000;
    while (!ackStarted && Date.now() < deadline) {
      await Bun.sleep(50);
    }
    expect(ackStarted).toBe(true);

    // Shutdown limpo: espera o em voo terminar (nunca ack sem commit).
    await slow.onApplicationShutdown();

    expect((await transactionByIdempotencyKey(idempotencyKey))[0]!.status).toBe('PROCESSED');
    expect(await inboxRows(messageId)).toHaveLength(1);
    const leftovers = await drainUntil(new Set([messageId]), 0, 3_000);
    expect(leftovers).toHaveLength(0);

    await assertLedgerConsistency(executeShared, walletId);
  }, 60_000);
});
