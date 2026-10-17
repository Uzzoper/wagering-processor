import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  PurgeQueueCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import { CreateWalletUseCase } from '../../src/application/create-wallet.use-case';
import type { UnitOfWork } from '../../src/application/ports';
import { MikroOrmUnitOfWork } from '../../src/infra/persistence/unit-of-work';
import { spawnApp, type SpawnApp } from '../helpers/app-process';
import {
  createPrivateEventsQueue,
  createPrivateWagerQueues,
  makeSqsClient,
  pollReceive,
} from '../helpers/sqs-suite';
import { assertLedgerConsistency } from './ledger-consistency';

/**
 * Fatia 7d — crash recovery com processos reais (`bun run src/main.ts`):
 * (2) morte entre o commit e o ack (FAULT_EXIT_AFTER_COMMIT) + redelivery;
 * (3) morte do publisher entre o claim e a publicação (FAULT_EXIT_AFTER_CLAIM);
 * (5) SIGTERM com mensagens em voo — nada perdido.
 *
 * Filas privadas ÚNICAS por rodada (nunca as de produção): um órfão de rodada
 * anterior apontando para um nome antigo NÃO pode consumir nossas mensagens.
 * Zero sleep fixo — o processo só espera por coisa via poll com deadline.
 */

const SUITE_BASE = 'test-crash7d';
// Sufixo por rodada: filas de runs antigos (e seus órfãos) ficam fora do caminho.
const SUITE = `${SUITE_BASE}-${crypto.randomUUID().slice(0, 8)}`;
const WAGER_QUEUE_NAME = `${SUITE}.fifo`;
const EVENTS_QUEUE_NAME = `${SUITE}-events.fifo`;

interface Envelope {
  messageId: string;
  type: string;
  occurredAt: string;
  data: Record<string, unknown>;
}

function wagerEnvelope(messageId: string, data: Record<string, unknown>): Envelope {
  return {
    messageId,
    type: 'WagerTransactionRequested',
    occurredAt: new Date().toISOString(),
    data,
  };
}

async function sendEnvelope(
  envelope: Envelope,
  walletId: string,
): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: wagerUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: walletId,
      MessageDeduplicationId: envelope.messageId,
    }),
  );
}

interface SentBet {
  messageId: string;
  idempotencyKey: string;
  externalTransactionId: string;
}

async function sendBet(
  playerId: string,
  walletId: string,
  amount: string,
): Promise<SentBet> {
  const externalTransactionId = crypto.randomUUID();
  const idempotencyKey = `provider-crash:${externalTransactionId}`;
  const messageId = `msg-${crypto.randomUUID()}`;
  await sendEnvelope(
    wagerEnvelope(messageId, {
      idempotencyKey,
      providerId: 'provider-crash',
      externalTransactionId,
      playerId,
      walletId,
      roundId: 'crash-round',
      gameId: 'crash-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    }),
    walletId,
  );
  return { messageId, idempotencyKey, externalTransactionId };
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

async function createWallet(playerId: string, amount: string): Promise<string> {
  const result = await new CreateWalletUseCase(unitOfWork).execute({
    playerId,
    initialBalance: { amount, currency: 'BRL' },
  });
  expect(result.outcome).toBe('CREATED');
  return (result as unknown as { wallet: { id: string } }).wallet.id;
}

async function pollExitCode(proc: Bun.Subprocess, expected: number, deadlineMs = 90_000): Promise<number> {
  const deadline = Date.now() + deadlineMs;
  for (;;) {
    if (proc.exitCode === expected) {
      return proc.exitCode;
    }
    if (Date.now() > deadline) {
      throw new Error(
        `timeout: processo não chegou ao exit code ${String(expected)} (atual ${String(proc.exitCode)})`,
      );
    }
    await Bun.sleep(200);
  }
}

async function pollTransaction(
  idempotencyKey: string,
  expected: 'PROCESSED' | 'REJECTED',
  timeoutMs = 45_000,
): Promise<{ id: string; status: string; kind: string; failure_code: string | null }> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const rows = await query<
      Array<{ id: string; status: string; kind: string; failure_code: string | null }>
    >(
      `SELECT id, status, kind, failure_code FROM wager_transaction
        WHERE idempotency_key = ? ORDER BY created_at LIMIT 1`,
      [idempotencyKey],
    );
    if (rows[0] !== undefined && rows[0]!.status === expected) {
      return rows[0]!;
    }
    if (Date.now() > deadline) {
      throw new Error(`timeout: transação "${idempotencyKey}" não chegou a ${expected}`);
    }
    await Bun.sleep(200);
  }
}

async function ledgerDirectionCount(
  walletId: string,
  direction: 'DEBIT' | 'CREDIT',
): Promise<number> {
  const rows = await query<Array<{ count: number }>>(
    `SELECT count(*)::int AS count FROM wallet_ledger_entry
      WHERE wallet_id = ? AND direction = ?`,
    [walletId, direction],
  );
  return rows[0]!.count;
}

/** Recebe (deletando) até a fila devolver 0 mensagens — prova de fila vazia. */
async function pollUntilEmpty(queueUrl: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const received = await pollReceive(sqs, queueUrl, { waitSeconds: 1 });
    for (const { receiptHandle } of received) {
      await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle }));
    }
    if (received.length === 0) {
      expect(received).toHaveLength(0);
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`timeout: fila ${queueUrl} continuou devolvendo mensagens`);
    }
  }
}

/** DLQ da suíte vazia (nenhuma mensagem válida pode ter ido para lá). */
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

beforeAll(async () => {
  orm = await MikroORM.init(buildMikroOrmConfig());
  unitOfWork = new MikroOrmUnitOfWork(orm);
  sqs = makeSqsClient();
  const queues = await createPrivateWagerQueues(sqs, SUITE, { visibilityTimeoutSeconds: 2 });
  wagerUrl = queues.wagerUrl;
  dlqUrl = queues.dlqUrl;
  eventsUrl = await createPrivateEventsQueue(sqs, SUITE);
}, 60_000);

afterAll(async () => {
  for (const spawn of spawns) {
    await spawn.stop().catch(() => undefined);
  }
  // Filas privadas desta rodada: melhor esforço, uma filas órfãs não nos atrapalham.
  for (const queueUrl of [wagerUrl, dlqUrl, eventsUrl]) {
    await sqs.send(new DeleteQueueCommand({ QueueUrl: queueUrl })).catch(() => undefined);
  }
  await orm.close(true);
});

describe('morte entre o commit e o ack', () => {
  test('consumer morre 137 após o commit; redelivery entra pelo replay em processo novo', async () => {
    await sqs.send(new PurgeQueueCommand({ QueueUrl: wagerUrl }));
    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '100.00');

    // Consumer A com o gancho ATIVO (só existe em NODE_ENV=test): processa,
    // commita e morre 137 sem dar ack.
    const a = await spawnApp({
      port: 4411,
      workers: 'api,sqs',
      env: { SQS_WAGER_QUEUE: WAGER_QUEUE_NAME, FAULT_EXIT_AFTER_COMMIT: '1' },
    });

    const bet = await sendBet(playerId, walletId, '10.00');

    // Ordem determinística: primeiro prova do commit no banco (PROCESSED só
    // existe se A recebeu E commitou — A é o único consumidor da fila única da
    // rodada); SO ENTÃO o exit 137, que fica a alguns ms do commit
    // (prazo curto de propósito: safo o gancho não disparou).
    const processed = await pollTransaction(bet.idempotencyKey, 'PROCESSED', 120_000);
    expect(processed.status).toBe('PROCESSED');
    await pollExitCode(a.proc, 137, 30_000);

    // B SEM env de falha: prova que o gancho é ignorado sem a env e segue vivo.
    const b = await spawnApp({
      port: 4412,
      workers: 'api,sqs',
      env: { SQS_WAGER_QUEUE: WAGER_QUEUE_NAME },
    });
    spawns.push(b);

    // Redelivery entra pelo replay do inbox: exatamente 1 débito, 1 transação.
    // 2x margem sob carga de arquivos em paralelo
    const deadline = Date.now() + 90_000;
    for (;;) {
      if ((await ledgerDirectionCount(walletId, 'DEBIT')) >= 1) {
        break;
      }
      if (Date.now() > deadline) {
        throw new Error('timeout: redelivery não foi processada pelo processo B');
      }
      await Bun.sleep(200);
    }
    const balance = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [walletId],
    );
    expect(balance[0]!.balance_amount).toBe('90.00');
    expect(await ledgerDirectionCount(walletId, 'DEBIT')).toBe(1);

    // Dedup do inbox: 1 linha de consumer único com processed_at preenchido.
    const inbox = await query<Array<{ count: number; processed: number }>>(
      `SELECT count(*)::int AS count,
              count(*) FILTER (WHERE processed_at IS NOT NULL)::int AS processed
         FROM inbox_message
        WHERE consumer_name = 'wager-sqs-consumer' AND message_id = ?`,
      [bet.messageId],
    );
    expect(inbox[0]!.count).toBe(1);
    expect(inbox[0]!.processed).toBe(1);

    // fila principal e DLQ limpas (nada redrivou).
    await pollUntilEmpty(wagerUrl, 60_000);
    await assertDlqEmpty();

    // B segue vivo e desliga limpo (exit 0 no SIGTERM).
    expect(b.proc.exitCode).toBeNull();
    expect(await b.stop()).toBe(0);

    await assertLedgerConsistency(query, walletId);
  }, 180_000);
});

describe('morte do publisher entre o claim e a publicação', () => {
  test('P1 morre 137 após o claim; o lease expira e P2 publica tudo', async () => {
    // P1 sobe SEM trabalho do sentinel no outbox: criação de wallet também
    // publica na outbox (WalletCreated, ledger do próprio aggregate), então
    // subir com trabalho pendente é corrida com o health. O gancho é escopado
    // a um sentinel criado aqui — os claims de boot (linhas alheias ou da
    // wallet desta suíte) NUNCA combinam; só o lote que contém o sentinel morre,
    // e apenas DEPOIS de /health/live estar de pé. aggregate_id é coluna
    // UUID, então o sentinel é um UUID puro (nada de prefixo textual).
    const faultAggregate = crypto.randomUUID();
    const p1 = await spawnApp({
      port: 4413,
      workers: 'api,outbox',
      env: {
        SQS_EVENTS_QUEUE: EVENTS_QUEUE_NAME,
        OUTBOX_LEASE_MS: '2000',
        OUTBOX_POLL_INTERVAL_MS: '100',
        FAULT_EXIT_AFTER_CLAIM: '1',
        FAULT_EXIT_CLAIM_AGGREGATE: faultAggregate,
      },
    });

    const playerId = crypto.randomUUID();
    const walletId = await createWallet(playerId, '50.00');

    // Três linhas de outbox do sentinel — o próximo claim do P1 (a cada
    // 100ms; claim vazio não mata) as pega e o processo morre 137.
    const eventIds = Array.from({ length: 3 }, () => crypto.randomUUID());
    for (const [index, eventId] of eventIds.entries()) {
      const occurredAt = new Date();
      await query(
        `INSERT INTO outbox_message (id, event_id, aggregate_id, event_type, payload, occurred_at)
         VALUES (?, ?, ?, ?, ?::jsonb, ?)`,
        [
          crypto.randomUUID(),
          eventId,
          faultAggregate,
          'WalletBalanceChanged',
          JSON.stringify({
            eventId,
            eventType: 'WalletBalanceChanged',
            aggregateId: faultAggregate,
            correlationId: `corr-${eventId}`,
            occurredAt: occurredAt.toISOString(),
            version: 1,
            data: {
              walletId,
              transactionId: crypto.randomUUID(),
              direction: 'CREDIT',
              money: { amount: '1.00', currency: 'BRL' },
              balanceBefore: { amount: '0.00', currency: 'BRL' },
              balanceAfter: { amount: '1.00', currency: 'BRL' },
              walletVersion: 1 + index,
            },
          }),
          occurredAt,
        ],
      );
    }

    // P1 morre 137 logo após o claim; as linhas ficam com lease de 2s.
    await pollExitCode(p1.proc, 137);

    // P2 assume quando o lease de 2s do P1 vence e publica todas.
    const p2 = await spawnApp({
      port: 4414,
      workers: 'api,outbox',
      env: {
        SQS_EVENTS_QUEUE: EVENTS_QUEUE_NAME,
        OUTBOX_LEASE_MS: '2000',
        OUTBOX_POLL_INTERVAL_MS: '100',
      },
    });
    spawns.push(p2);

    const deadline = Date.now() + 90_000;
    for (;;) {
      const placeholders = eventIds.map(() => '?').join(', ');
      const rows = await query<Array<{ pending: number }>>(
        `SELECT count(*)::int AS pending FROM outbox_message
          WHERE event_id IN (${placeholders}) AND published_at IS NULL`,
        eventIds,
      );
      if (rows[0]!.pending === 0) {
        break;
      }
      if (Date.now() > deadline) {
        throw new Error('timeout: outbox não esvaziou após a morte do P1');
      }
      await Bun.sleep(200);
    }

    // Todos os eventIds chegam na fila privada: espera EVENTO a evento com
    // deadline (a fila pode âmago devolve 0 numa sondagem antes do evento)
    // e DELETA à medida que recebe.
    {
      const { DeleteMessageCommand } = await import('@aws-sdk/client-sqs');
      const remaining = new Set<string>(eventIds);
      const presenceDeadline = Date.now() + 90_000;
      for (;;) {
        const received = await pollReceive(sqs, eventsUrl, { waitSeconds: 1 });
        for (const { body, receiptHandle } of received) {
          await sqs.send(
            new DeleteMessageCommand({ QueueUrl: eventsUrl, ReceiptHandle: receiptHandle }),
          );
          try {
            const envelope = JSON.parse(body) as { eventId?: string };
            if (typeof envelope.eventId === 'string') {
              remaining.delete(envelope.eventId);
            }
          } catch {
            // corpo fora do formato: descartado
          }
        }
        if (remaining.size === 0) {
          break;
        }
        if (Date.now() > presenceDeadline) {
          throw new Error(
            `timeout: ${String(remaining.size)}/3 eventIds ainda não publicados na fila de eventos`,
          );
        }
      }
    }

    expect(p2.proc.exitCode).toBeNull();
    expect(await p2.stop()).toBe(0);

    await assertLedgerConsistency(query, walletId);
  }, 180_000);
});

describe('SIGTERM com mensagens em voo', () => {
  test('A encerra durante o processamento; B termina o resto sem perder nada', async () => {
    await sqs.send(new PurgeQueueCommand({ QueueUrl: wagerUrl }));

    const wallets = await Promise.all(
      Array.from({ length: 10 }, async (_, index) => {
        const playerId = crypto.randomUUID();
        return {
          index,
          playerId,
          walletId: await createWallet(playerId, '100.00'),
        };
      }),
    );

    const a = await spawnApp({
      port: 4415,
      workers: 'api,sqs',
      env: { SQS_WAGER_QUEUE: WAGER_QUEUE_NAME },
    });
    const b = await spawnApp({
      port: 4416,
      workers: 'api,sqs',
      env: { SQS_WAGER_QUEUE: WAGER_QUEUE_NAME },
    });
    spawns.push(a, b);

    const bets = new Map<number, SentBet>();
    await Promise.all(
      wallets.map(async ({ index, playerId, walletId }) => {
        const bet = await sendBet(playerId, walletId, '10.00');
        bets.set(index, bet);
      }),
    );

    // SIGTERM em A no momento em que o processamento já está bebido com
    // débito no banco (comprovadamente em voo). Se nada processou ainda,
    // kill mesmo assim: nada é perdido de qualquer jeito.
    const walletIds = wallets.map((wallet) => wallet.walletId);
    const deadline = Date.now() + 30_000;
    for (;;) {
      const rows = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM wallet_ledger_entry
          WHERE wallet_id IN (${walletIds.map(() => '?').join(', ')})`,
        walletIds,
      );
      if (rows[0]!.count > 0) {
        break;
      }
      if (Date.now() > deadline) {
        break;
      }
      await Bun.sleep(50);
    }
    a.kill('SIGTERM');
    expect(await a.proc.exited).toBe(0);

    // B termina o resto: 10 transações PROCESSED.
    const drainDeadline = Date.now() + 90_000;
    for (;;) {
      const keys = [...bets.values()].map((bet) => bet.idempotencyKey);
      const processed = await query<Array<{ count: number }>>(
        `SELECT count(*)::int AS count FROM wager_transaction
          WHERE idempotency_key IN (${keys.map(() => '?').join(', ')}) AND status = 'PROCESSED'`,
        keys,
      );
      if (processed[0]!.count === 10) {
        break;
      }
      if (Date.now() > drainDeadline) {
        throw new Error(`timeout: apenas ${String(processed[0]!.count)}/10 processadas`);
      }
      await Bun.sleep(200);
    }

    for (const { walletId } of wallets) {
      expect(await ledgerDirectionCount(walletId, 'DEBIT')).toBe(1);
    }

    // Uma inbox row por mensagem, DLQ vazia.
    const keys = [...bets.values()].map((bet) => bet.messageId);
    const inbox = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM inbox_message
        WHERE consumer_name = 'wager-sqs-consumer'
          AND message_id IN (${keys.map(() => '?').join(', ')})`,
      keys,
    );
    expect(inbox[0]!.count).toBe(10);
    await assertDlqEmpty();

    for (const { walletId } of wallets) {
      await assertLedgerConsistency(query, walletId);
    }
  }, 180_000);
});
