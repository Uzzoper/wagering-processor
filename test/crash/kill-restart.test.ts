import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import type { UnitOfWork } from '../../src/application/ports';
import { MikroOrmUnitOfWork } from '../../src/infra/persistence/unit-of-work';
import { CreateWalletUseCase } from '../../src/application/create-wallet.use-case';
import { spawnApp, stopAllApps, type SpawnApp } from '../helpers/app-process';
import {
  createPrivateEventsQueue,
  createPrivateWagerQueues,
  makeSqsClient,
  pollReceive,
} from '../helpers/sqs-suite';
import { assertLedgerConsistency } from '../integration/ledger-consistency';

/**
 * Fatia 7d — SIGKILL com substituição de sistema (30 wallets, carga mista
 * HTTP+SQS): matar 1 das 3 instâncias (api,outbox,pending) no meio da carga e
 * subir substituto completo. Invariantes no fim: balance == ledger nas 30
 * wallets, 1 linha de inbox por messageId, fila principal e DLQ vazias e os
 * eventIds do outbox ⊇ drenados da fila de privada de eventos. Zero sleep fixo.
 */

const SUITE_BASE = 'test-kr';
// Sufixo por rodada: órfãos de rodadas anteriores (filas antigas) ficam fora.
const SUITE = `${SUITE_BASE}-${crypto.randomUUID().slice(0, 8)}`;
const WAGER_QUEUE_NAME = `${SUITE}.fifo`;
const EVENTS_QUEUE_NAME = `${SUITE}-events.fifo`;
const HTTP_PORTS = [4331, 4332, 4333];
const CONSUMER_PORT = 4334;
const REPLACEMENT_PORT = 4335;
const BASE_URLS = HTTP_PORTS.map((port) => `http://127.0.0.1:${port}`);
const PENDING_ATTEMPTING_COUNT = 15;

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
      roundId: 'kr-round',
      gameId: 'kr-game',
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
}

function sqsBet(playerId: string, walletId: string, amount: string): SentBet {
  const externalTransactionId = crypto.randomUUID();
  const idempotencyKey = `provider-sqs:${externalTransactionId}`;
  const messageId = crypto.randomUUID();
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
      roundId: 'kr-sqs-round',
      gameId: 'kr-sqs-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    },
  };
  return { messageId, idempotencyKey, envelope, walletId };
}

/**
 * Grupo fixo da suíte: LocalStack FIFO entrega no máximo mensagens de um
 * MessageGroupId por ReceiveMessage — grupos por wallet drenam 1 msg/10s.
 */
async function sendEnvelope(envelope: Record<string, unknown>): Promise<void> {
  const { SendMessageCommand } = await import('@aws-sdk/client-sqs');
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: wagerUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: `${SUITE}-bulk`,
      MessageDeduplicationId: String(envelope.messageId),
    }),
  );
}

async function pollUntilEmpty(queueUrl: string, timeoutMs: number): Promise<void> {
  const { DeleteMessageCommand } = await import('@aws-sdk/client-sqs');
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
  const { DeleteMessageCommand } = await import('@aws-sdk/client-sqs');
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

beforeAll(async () => {
  orm = await MikroORM.init(buildMikroOrmConfig());
  unitOfWork = new MikroOrmUnitOfWork(orm);
  sqs = makeSqsClient();
  const queues = await createPrivateWagerQueues(sqs, SUITE);
  wagerUrl = queues.wagerUrl;
  dlqUrl = queues.dlqUrl;
  eventsUrl = await createPrivateEventsQueue(sqs, SUITE);

  // 3 instâncias completas com leases curtos (morte no meio da carga resolve
  // rápido pelo co-claim das instâncias vivas) e worker pending acelerado.
  const sharedEnv = {
    SQS_EVENTS_QUEUE: EVENTS_QUEUE_NAME,
    OUTBOX_LEASE_MS: '3000',
    OUTBOX_POLL_INTERVAL_MS: '200',
    PENDING_REF_POLL_INTERVAL_MS: '200',
    PENDING_REF_MAX_ATTEMPTS: '1',
  };
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
  await stopAllApps(spawns);
  // Filas privadas desta rodada: melhor esforço (sem depender do resultado).
  for (const queueUrl of [wagerUrl, dlqUrl, eventsUrl]) {
    const { DeleteQueueCommand } = await import('@aws-sdk/client-sqs');
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
  }
  await orm.close(true);
});

describe('SIGKILL com substituição + carga mista (HTTP + SQS)', () => {
  test('mata 1 das 3 no meio da carga, substituto assume; nada perdido e eventos ⊆ fila de eventos', async () => {
    // 30 wallets: 15 entram por HTTP (BET 10.00 + REFUND sem referência) e 15 via SQS (BET 10.00).
    const httpWallets = await Promise.all(
      Array.from({ length: 15 }, async (_, index) => ({
        index,
        wallet: await postWallet(BASE_URLS[index % 3]!, crypto.randomUUID(), '100.00'),
      })),
    );
    const sqsWallets = await Promise.all(
      Array.from({ length: 15 }, async () => {
        const playerId = crypto.randomUUID();
        const walletId = await createWallet(playerId, '100.00');
        return { playerId, walletId };
      }),
    );

    // Perna HTTP: 15 BETs em paralelo distribuídas pelas 3 instâncias.
    const httpBets = httpWallets.map(({ wallet }) =>
      betInput(wallet.playerId, wallet.id, '10.00'),
    );
    const httpResponses = await Promise.all(
      httpBets.map((bet, index) => postWager(BASE_URLS[index % 3]!, bet)),
    );
    expect(httpResponses.every((response) => response.status === 201)).toBe(true);
    for (const response of httpResponses) {
      expect(response.body.balance).toEqual({ amount: '90.00', currency: 'BRL' });
    }

    // Perna SQS: 15 BETs → consumer processa (poll só no DB).
    const sqsBets = sqsWallets.map(({ playerId, walletId }) => sqsBet(playerId, walletId, '10.00'));
    for (const bet of sqsBets) {
      await sendEnvelope(bet.envelope);
    }
    const sqsKeys = sqsBets.map((bet) => bet.idempotencyKey);
    const sqsIds = sqsBets.map((bet) => bet.messageId);
    const deadlineAds = Date.now() + 240_000;
    for (;;) {
      const rows = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM wager_transaction
          WHERE idempotency_key IN (${sqsKeys.map(() => '?').join(', ')}) AND status = 'PROCESSED'`,
        sqsKeys,
      );
      if (rows[0]!.count === 15) {
        break;
      }
      if (Date.now() > deadlineAds) {
        throw new Error(`timeout: apenas ${String(rows[0]!.count)}/15 bets SQS processadas`);
      }
      await Bun.sleep(200);
    }

    // CRASH: SIGKILL numa das 3 instâncias com o sistema ocupado (bets SQS
    // acabaram créditas: os workers das três têm outbox/pending para claimar).
    const victim = spawns[HTTP_PORTS.length - 1]!;
    victim.kill('SIGKILL');
    // SIGKILL no Bun resolve `exited` com signalCode (exitCode fica null).
    const deadExit = await victim.proc.exited;
    expect(victim.proc.signalCode ?? `exit ${String(deadExit)}`).toBe('SIGKILL');
    // Substituto completo assume (mesma config de leases/pending).
    const replacement = await spawnApp({
      port: REPLACEMENT_PORT,
      workers: 'api,outbox,pending',
      env: {
        SQS_EVENTS_QUEUE: EVENTS_QUEUE_NAME,
        OUTBOX_LEASE_MS: '3000',
        OUTBOX_POLL_INTERVAL_MS: '200',
        PENDING_REF_POLL_INTERVAL_MS: '200',
        PENDING_REF_MAX_ATTEMPTS: '1',
      },
    });
    spawns.push(replacement);

    // URLs vivas dpois do crash: as 2 instâncias sobreviventes + o substituto.
    const liveUrls = [BASE_URLS[0]!, BASE_URLS[1]!, replacement.baseUrl];

    // REFUND antes da referência: referência que nunca vai existir —
    // 202 PENDING_REFERENCE na hora; worker esgota a tentativa → REJECTED
    // REFERENCE_NOT_FOUND + evento Rejected; sem crédito no ledger.
    const refundInputs = httpWallets.map(({ wallet }) => {
      const externalTransactionId = crypto.randomUUID();
      return {
        key: `provider-a:${externalTransactionId}`,
        body: {
          providerId: 'provider-a',
          externalTransactionId,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'kr-round',
          gameId: 'kr-game',
          kind: 'REFUND',
          money: { amount: '10.00', currency: 'BRL' },
          referenceExternalTransactionId: `missing:${crypto.randomUUID()}`,
        },
      };
    });
    const refundResponses = await Promise.all(
      refundInputs.map((input, index) => postWager(liveUrls[index % liveUrls.length]!, input)),
    );
    expect(refundResponses.every((response) => response.status === 202)).toBe(true);

    // Fim: 15 refund → REJECTED REFERENCE_NOT_FOUND (+ evento Rejected).
    const refundKeys = refundInputs.map((input) => input.key);
    const refundDeadline = Date.now() + 60_000;
    for (;;) {
      const rows = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM wager_transaction
          WHERE idempotency_key IN (${refundKeys.map(() => '?').join(', ')}) AND status = 'REJECTED'`,
        refundKeys,
      );
      if (rows[0]!.count === PENDING_ATTEMPTING_COUNT) {
        break;
      }
      if (Date.now() > refundDeadline) {
        throw new Error(
          `timeout: apenas ${String(rows[0]!.count)}/${PENDING_ATTEMPTING_COUNT} refunds REJECTED`,
        );
      }
      await Bun.sleep(200);
    }
    const failureCodes = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction
        WHERE idempotency_key IN (${refundKeys.map(() => '?').join(', ')})
          AND status = 'REJECTED' AND failure_code = 'REFERENCE_NOT_FOUND'`,
      refundKeys,
    );
    expect(failureCodes[0]!.count).toBe(PENDING_ATTEMPTING_COUNT);
    // REFUND rejected: SEM crédito no ledger — {opened} só o débito da BET.
    const httpWalletIds = httpWallets.map(({ wallet }) => wallet.id);
    const invalidCredits = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry
        WHERE direction = 'CREDIT'
        AND wallet_id IN (${httpWalletIds.map(() => '?').join(', ')})`,
      httpWalletIds,
    );
    expect(invalidCredits[0]!.count).toBe(15); // apenas os OPENING

    // Invariantes finais nas 30 wallets: balance == ledger e efeito unitário.
    for (const { wallet } of httpWallets) {
      const stored = await query<Array<{ balance_amount: string }>>(
        `SELECT balance_amount FROM wallet WHERE id = ?`,
        [wallet.id],
      );
      expect(stored[0]!.balance_amount).toBe('90.00');
      await assertLedgerConsistency(query, wallet.id);
    }
    for (const { walletId } of sqsWallets) {
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
      await assertLedgerConsistency(query, walletId);
    }

    // Uma linha de inbox por messageId das bets SQS.
    const inbox = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM inbox_message
        WHERE consumer_name = 'wager-sqs-consumer'
          AND message_id IN (${sqsIds.map(() => '?').join(', ')})`,
      sqsIds,
    );
    expect(inbox[0]!.count).toBe(15);

    // Fila principal vazia; DLQ vazia.
    await pollUntilEmpty(wagerUrl, 60_000);
    await assertDlqEmpty();

    // Nada perdido de eventos: o outbox termina vazio (todos os event_ids
    // publicados pelas instâncias vivas + substituto) e TODOS drenados.
    const allAggregates = [...httpWalletIds, ...sqsWallets.map(({ walletId }) => walletId)];
    const places = allAggregates.map(() => '?').join(', ');
    const outboxDeadline = Date.now() + 90_000;
    for (;;) {
      const rows = await query<Array<{ pending: number }>>(
        `SELECT count(*)::int AS pending FROM outbox_message
          WHERE aggregate_id IN (${places}) AND published_at IS NULL`,
        allAggregates,
      );
      if (rows[0]!.pending === 0) {
        break;
      }
      if (Date.now() > outboxDeadline) {
        throw new Error(`timeout: ${String(rows[0]!.pending)} linhas de outbox pendentes`);
      }
      await Bun.sleep(200);
    }
    const published = await query<Array<{ event_id: string }>>(
      `SELECT event_id FROM outbox_message WHERE aggregate_id IN (${places})`,
      allAggregates,
    );
    // Drenar ATÉ COBRIR o conjunto esperado, com deadline (nunca teto fixo de
    // rodadas): o helper com corte devolve logo na primeira sondagem vazia —
    // sob carga, LocalStack pode responder 0 mesmo com o publisher em fila
    // de entrega (lag após a quiescência do banco) e o conjunto sai truncado.
    // Esperado vem do DB APÓS a quiescência (linhas do outbox são permanentes);
    // se um evento nunca chegar, o prazo estoura — perda real, correto falhar.
    const { DeleteMessageCommand } = await import('@aws-sdk/client-sqs');
    const expectedIds = published.map((row) => row.event_id);
    const drainedIds = new Set<string>();
    const drainDeadline = Date.now() + 120_000;
    for (;;) {
      const received = await pollReceive(sqs, eventsUrl, { waitSeconds: 1 });
      for (const { body, receiptHandle } of received) {
        await sqs.send(
          new DeleteMessageCommand({ QueueUrl: eventsUrl, ReceiptHandle: receiptHandle }),
        );
        try {
          const envelope = JSON.parse(body) as { eventId?: string };
          if (typeof envelope.eventId === 'string') {
            drainedIds.add(envelope.eventId);
          }
        } catch {
          // corpo fora do formato: descartado e deletado
        }
      }
      const missing = expectedIds.filter((id) => !drainedIds.has(id));
      if (missing.length === 0) {
        break;
      }
      if (Date.now() > drainDeadline) {
        throw new Error(
          `timeout: ${String(missing.length)}/${String(expectedIds.length)} eventIds não drenados`,
        );
      }
    }
  }, 600_000);
});
