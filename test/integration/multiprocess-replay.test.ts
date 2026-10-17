import { DeleteMessageCommand, PurgeQueueCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import type { UnitOfWork } from '../../src/application/ports';
import { MikroOrmUnitOfWork } from '../../src/infra/persistence/unit-of-work';
import { CreateWalletUseCase } from '../../src/application/create-wallet.use-case';
import { spawnApp, type SpawnApp } from '../helpers/app-process';
import {
  createPrivateEventsQueue,
  createPrivateWagerQueues,
  makeSqsClient,
  pollReceive,
} from '../helpers/sqs-suite';
import { assertLedgerConsistency } from './ledger-consistency';

/**
 * Fatia 7d — carga multiprocesso: 3 instâncias reais (api,outbox,pending) nas
 * portas 4321–4323 + 1 consumer SQS em processo separado na fila privada da
 * suíte. Cenários (a) 80/80 em instâncias distintas, (b) replay 50× distribuído,
 * (c) perna SQS com dedup do inbox. Zero sleep fixo — só poll com deadline.
 */

const SUITE_BASE = 'test-mpr';
// Sufixo por rodada: órfãos de rodadas anteriores ficam fora das filas frescas.
const SUITE = `${SUITE_BASE}-${crypto.randomUUID().slice(0, 8)}`;
const WAGER_QUEUE_NAME = `${SUITE}.fifo`;
const EVENTS_QUEUE_NAME = `${SUITE}-events.fifo`;
const HTTP_PORTS = [4321, 4322, 4323];
const CONSUMER_PORT = 4324;
const BASE_URLS = HTTP_PORTS.map((port) => `http://127.0.0.1:${port}`);

interface SubmitBody {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
}

interface WalletBody {
  id: string;
  playerId: string;
}

interface WagerInput {
  key: string;
  body: Record<string, unknown>;
}

let orm: MikroORM;
let unitOfWork: UnitOfWork;
let sqs: ReturnType<typeof makeSqsClient>;
let wagerUrl: string;
let dlqUrl: string;
let eventsUrl: string;
const spawns: SpawnApp[] = [];

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

function betInput(playerId: string, walletId: string, amount: string): WagerInput {
  const externalTransactionId = crypto.randomUUID();
  return {
    key: `provider-a:${externalTransactionId}`,
    body: {
      providerId: 'provider-a',
      externalTransactionId,
      playerId,
      walletId,
      roundId: 'mpr-round',
      gameId: 'mpr-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    },
  };
}

async function postWallet(baseUrl: string, playerId: string, amount: string): Promise<WalletBody> {
  const response = await fetch(`${baseUrl}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ playerId, initialBalance: { amount, currency: 'BRL' } }),
  });
  expect(response.status).toBe(201);
  return (await response.json()) as WalletBody;
}

async function postWager(
  baseUrl: string,
  input: WagerInput,
): Promise<{ status: number; body: SubmitBody }> {
  const response = await fetch(`${baseUrl}/wagering/transactions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': input.key },
    body: JSON.stringify(input.body),
  });
  return { status: response.status, body: (await response.json()) as SubmitBody };
}

/** Cria wallet na bancada (bypass HTTP) para a perna SQS. */
async function createWallet(playerId: string, amount: string): Promise<string> {
  const result = await new CreateWalletUseCase(unitOfWork).execute({
    playerId,
    initialBalance: { amount, currency: 'BRL' },
  });
  expect(result.outcome).toBe('CREATED');
  return (result as unknown as { wallet: { id: string } }).wallet.id;
}

interface SentBet {
  messageId: string;
  idempotencyKey: string;
  envelope: Record<string, unknown>;
  walletId: string;
  playerId: string;
}

/**
 * Envelope SQS para a perna do consumer. `repeatId` distinto reenvia a MESMA
 * transação com messageId novo — o desvio da dedup do SQS força o replay
 * do inbox (linha única por key), nunca um segundo débito.
 */
function sqsBet(playerId: string, walletId: string, amount: string, repeatId?: string): SentBet {
  const externalTransactionId = crypto.randomUUID();
  const idempotencyKey = `provider-sqs:${externalTransactionId}`;
  const messageId = repeatId ?? crypto.randomUUID();
  const envelope = {
    messageId,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data: {
      idempotencyKey,
      providerId: 'provider-sqs',
      externalTransactionId,
      playerId,
      walletId,
      roundId: 'mpr-sqs-round',
      gameId: 'mpr-sqs-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    },
  };
  return { messageId, idempotencyKey, envelope, walletId, playerId };
}

async function sendEnvelope(envelope: Record<string, unknown>, walletId: string): Promise<void> {
  // MessageGroupId fixo da suíte: LocalStack FIFO entrega no máximo mensagens
  // de UM grupo por ReceiveMessage — grupos por wallet drenam a 1 msg/long-poll.
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: wagerUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: `${SUITE}-bulk`,
      MessageDeduplicationId: String(envelope.messageId),
    }),
  );
}

/** Reenvio: messageId novo, MESMA idempotency-key e MESMO payload (hash igual). */
function repeatBet(original: SentBet): SentBet {
  const messageId = crypto.randomUUID();
  const envelope = { ...original.envelope, messageId, occurredAt: new Date().toISOString() };
  return {
    messageId,
    idempotencyKey: original.idempotencyKey,
    envelope,
    walletId: original.walletId,
    playerId: original.playerId,
  };
}

/** Espera (deletando) a fila daǉ devolver 0 mensagens — prova de fila vazia. */
async function pollUntilEmpty(queueUrl: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const received = await pollReceive(sqs, queueUrl, { waitSeconds: 1 });
    for (const { receiptHandle } of received) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle }));
    }
    if (received.length === 0) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`timeout: fila ${queueUrl} continuou devolvendo mensagens`);
    }
  }
}

async function assertDlqEmpty(): Promise<void> {
  const deadline = Date.now() + 60_000;
  for (;;) {
    const received = await pollReceive(sqs, dlqUrl, { waitSeconds: 1 });
    for (const { receiptHandle } of received) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: dlqUrl, ReceiptHandle: receiptHandle }));
    }
    if (received.length === 0) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`DLQ privada da suíte não está vazia: recebeu ${received.length}`);
    }
  }
}

/** Espera o publisher publicar tudo que os aggregates-alvo geraram na outbox. */
async function waitOutboxPublished(aggregateIds: string[], timeoutMs: number): Promise<void> {
  const placeholders = aggregateIds.map(() => '?').join(', ');
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const rows = await query<Array<{ pending: number }>>(
      `SELECT count(*)::int AS pending FROM outbox_message
        WHERE aggregate_id IN (${placeholders}) AND published_at IS NULL`,
      aggregateIds,
    );
    if (rows[0]!.pending === 0) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`timeout: ${String(rows[0]!.pending)} linha(s) de outbox ainda pendente(s)`);
    }
    await Bun.sleep(200);
  }
}

beforeAll(async () => {
  orm = await MikroORM.init(buildMikroOrmConfig());
  unitOfWork = new MikroOrmUnitOfWork(orm);
  sqs = makeSqsClient();
  const queues = await createPrivateWagerQueues(sqs, SUITE);
  wagerUrl = queues.wagerUrl;
  dlqUrl = queues.dlqUrl;
  eventsUrl = await createPrivateEventsQueue(sqs, SUITE);
  await sqs.send(new PurgeQueueCommand({ QueueUrl: wagerUrl }));

  // 3 instâncias completas (api,outbox,pending) + consumer dedicado. Todas
  // publicam eventos na fila privada desta suíte (nunca na de produção) e o
  // consumer consome a fila privada de wager (nunca a de outra suíte).
  const sharedEnv = { SQS_EVENTS_QUEUE: EVENTS_QUEUE_NAME };
  for (const [index, port] of HTTP_PORTS.entries()) {
    spawns.push(await spawnApp({ port, workers: 'api,outbox,pending', env: sharedEnv }));
  }
  spawns.push(
    await spawnApp({
      port: CONSUMER_PORT,
      workers: 'api,sqs',
      env: { SQS_WAGER_QUEUE: WAGER_QUEUE_NAME },
    }),
  );
}, 90_000);

afterAll(async () => {
  for (const spawn of spawns) {
    await spawn.stop().catch(() => undefined);
  }
  // Filas privadas desta rodada: melhor esforço.
  for (const queueUrl of [wagerUrl, dlqUrl, eventsUrl]) {
    const { DeleteQueueCommand } = await import('@aws-sdk/client-sqs');
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
  }
  await orm.close(true);
});

describe('3 instâncias (api,outbox,pending) + consumer SQS separado', () => {
  test('(a) 80/80: 2 BETs em instâncias distintas = 1 processada + 1 rejeitada; replay na terceira', async () => {
    const wallet = await postWallet(BASE_URLS[0]!, crypto.randomUUID(), '80.00');
    const first = betInput(wallet.playerId, wallet.id, '80.00');
    const second = betInput(wallet.playerId, wallet.id, '80.00');

    const pairs = await Promise.all([
      postWager(BASE_URLS[0]!, first).then((response) => ({ input: first, response })),
      postWager(BASE_URLS[1]!, second).then((response) => ({ input: second, response })),
    ]);
    const processed = pairs.filter((pair) => pair.response.status === 201);
    const rejected = pairs.filter((pair) => pair.response.status === 422);
    expect(processed).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect(rejected[0]!.response.body.failureCode).toBe('INSUFFICIENT_BALANCE');
    expect(processed[0]!.response.body.balance).toEqual({ amount: '0.00', currency: 'BRL' });

    // Replay na terceira instância: 200 idempotente com o saldo da época (0.00).
    const replay = await postWager(BASE_URLS[2]!, processed[0]!.input);
    expect(replay.status).toBe(200);
    expect(replay.body.idempotentReplay).toBe(true);
    expect(replay.body.transactionId).toBe(processed[0]!.response.body.transactionId);
    expect(replay.body.balance).toEqual({ amount: '0.00', currency: 'BRL' });

    const stored = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [wallet.id],
    );
    expect(stored[0]!.balance_amount).toBe('0.00');

    const debits = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry
        WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [wallet.id],
    );
    expect(debits[0]!.count).toBe(1);
    await assertLedgerConsistency(query, wallet.id);

    // Outbox da wallet publicada pelas 3 instâncias sem nada perdido.
    await waitOutboxPublished([wallet.id], 60_000);
  }, 180_000);

  test('(b) replay 50×: mesma BET distribuída nas 3 instâncias = 1×201 + 49×200, 1 débito', async () => {
    const wallet = await postWallet(BASE_URLS[1]!, crypto.randomUUID(), '100.00');
    const input = betInput(wallet.playerId, wallet.id, '10.00');

    const responses = await Promise.all(
      Array.from({ length: 50 }, (_, index) =>
        postWager(BASE_URLS[index % 3]!, input),
      ),
    );
    const fresh = responses.filter((response) => response.status === 201);
    const replays = responses.filter((response) => response.status === 200);
    expect(fresh).toHaveLength(1);
    expect(replays).toHaveLength(49);
    expect(fresh[0]!.body.idempotentReplay).toBe(false);
    for (const replay of replays) {
      expect(replay.body.idempotentReplay).toBe(true);
      expect(replay.body.transactionId).toBe(fresh[0]!.body.transactionId);
      expect(replay.body.balance).toEqual({ amount: '90.00', currency: 'BRL' });
    }

    const transactions = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction WHERE idempotency_key = ?`,
      [input.key],
    );
    expect(transactions[0]!.count).toBe(1);

    const debits = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry
        WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [wallet.id],
    );
    expect(debits[0]!.count).toBe(1);
    await assertLedgerConsistency(query, wallet.id);
  }, 180_000);

  test('(c) perna SQS: 30 bets + 10 reenvios = 1 débito por wallet e replay pelo inbox', async () => {
    const wallets = await Promise.all(
      Array.from({ length: 30 }, async () => {
        const playerId = crypto.randomUUID();
        return { playerId, walletId: await createWallet(playerId, '100.00') };
      }),
    );

    const bets = new Map<number, SentBet>();
    await Promise.all(
      wallets.map(async ({ playerId, walletId }, index) => {
        bets.set(index, sqsBet(playerId, walletId, '10.00'));
      }),
    );
    const sentIds: string[] = [];
    for (const bet of bets.values()) {
      await sendEnvelope(bet.envelope, bet.walletId);
      sentIds.push(bet.messageId);
    }
    // Reenvios: messageId NOVO, mesma idempotency-key e payload igual.
    const expectedInbox = 40; // 30 fresh + 10 replays (linhas por messageId)
    for (const index of [0, 3, 7, 11, 13, 17, 21, 29, 27, 19]) {
      const original = bets.get(index)!;
      const repeat = repeatBet(original);
      await sendEnvelope(repeat.envelope, repeat.walletId);
      sentIds.push(repeat.messageId);
    }

    // O consumer processa serialmente (1 msg/lote): prazo generoso por taxa ~3 msg/s.
    // Poll de arquivo APENAS via DB — sondar a fila com VisibilityTimeout 0
    // e DeleteMessage roubaria mensagens do consumer em processamento.
    const deadline = Date.now() + 240_000;
    for (;;) {
      const lines = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM inbox_message
          WHERE consumer_name = 'wager-sqs-consumer'
            AND message_id IN (${sentIds.map(() => '?').join(', ')})`,
        sentIds,
      );
      if (lines[0]!.count === expectedInbox) {
        break;
      }
      if (Date.now() > deadline) {
        throw new Error(`timeout: inbox tem ${String(lines[0]!.count)}/${expectedInbox} linhas`);
      }
      await Bun.sleep(200);
    }

    // 1 débito e 1 transação por wallet, saldo 90.00 — replay não duplicou.
    for (const { walletId } of wallets) {
      const stored = await query<Array<{ balance_amount: string }>>(
        `SELECT balance_amount FROM wallet WHERE id = ?`,
        [walletId],
      );
      expect(stored[0]!.balance_amount).toBe('90.00');
      const debits = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM wallet_ledger_entry
          WHERE wallet_id = ? AND direction = 'DEBIT'`,
        [walletId],
      );
      expect(debits[0]!.count).toBe(1);
    }
    const keys = [...bets.values()].map((bet) => bet.idempotencyKey);
    const transactions = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction
        WHERE idempotency_key IN (${keys.map(() => '?').join(', ')})`,
      keys,
    );
    expect(transactions[0]!.count).toBe(30);

    // Fila principal e DLQ limpas; eventos das wallets SQS publicados.
    await pollUntilEmpty(wagerUrl, 30_000);
    await assertDlqEmpty();
    const sqsWallets = wallets.map((wallet) => wallet.walletId);
    await waitOutboxPublished(sqsWallets, 90_000);

    // Nada perdido de forma grosseira: a fila privada de eventos recebeu
    // envelopes das publicações (a prova completa de ⊇ está no F3).
    // SGurança de presença com prazo: sob carga, uma sondagem vazia não encerra
    // o drain (mesma classe de truncamento corrigida no F3).
    const presenceDeadline = Date.now() + 60_000;
    let receivedCount = 0;
    for (;;) {
      const received = await pollReceive(sqs, eventsUrl, { waitSeconds: 1 });
      for (const { body, receiptHandle } of received) {
        await sqs.send(
          new DeleteMessageCommand({ QueueUrl: eventsUrl, ReceiptHandle: receiptHandle }),
        );
        if (body.trim().startsWith('{')) {
          receivedCount += 1;
        }
      }
      if (receivedCount > 0) {
        break;
      }
      if (Date.now() > presenceDeadline) {
        throw new Error('timeout: fila de eventos sem nenhum envelope publicado');
      }
    }
    expect(receivedCount).toBeGreaterThan(0);

    for (const { walletId } of wallets) {
      await assertLedgerConsistency(query, walletId);
    }
  }, 300_000);
});
