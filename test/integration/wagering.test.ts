import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { AddressInfo } from 'node:net';

import { AppModule } from '../../src/app.module';
import { assertLedgerConsistency as sharedAssertLedgerConsistency } from './ledger-consistency';

/**
 * `POST /wagering/transactions` + GETs contra PostgreSQL real, com a app
 * ligada (HTTP de verdade). O preload já criou `wagering_test` e migrou;
 * cada teste usa UUIDs próprios, sem `down()`/`TRUNCATE`.
 */

interface SubmitBody {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
  message?: string;
}

interface WalletBody {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
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

async function postWager(body: unknown, key?: string): Promise<{ status: number; body: SubmitBody }> {
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (key !== undefined) {
    headers['idempotency-key'] = key;
  }
  const response = await fetch(`${baseUrl}/wagering/transactions`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as SubmitBody };
}

function wagerInput(playerId: string, walletId: string, overrides: Record<string, unknown> = {}) {
  const externalTransactionId = crypto.randomUUID();
  return {
    providerId: 'provider-a',
    externalTransactionId,
    playerId,
    walletId,
    roundId: 'round-1',
    gameId: 'game-1',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
    ...overrides,
    key: `provider-a:${overrides.externalTransactionId ?? externalTransactionId}` as string,
  };
}

/** Separa a key do corpo (a key vai no header, nunca no JSON). */
function splitInput(input: ReturnType<typeof wagerInput>) {
  const { key, ...body } = input;
  return { key, body };
}

/** Wrapper fino: delega ao helper compartilhado usando o `query` deste arquivo. */
async function assertLedgerConsistency(walletId: string): Promise<void> {
  await sharedAssertLedgerConsistency((sql, params) => query(sql, params), walletId);
}

beforeAll(async () => {
  app = await NestFactory.create(AppModule, { logger: false });
  await app.listen(0);

  const address = app.getHttpServer().address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${address.port}`;
  orm = app.get(MikroORM);
});

afterAll(async () => {
  await app.close();
});

describe('POST /wagering/transactions: fluxos felizes', () => {
  test('BET debita, persiste PROCESSED e publica 2 eventos', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '30.00', currency: 'BRL' } }),
    );

    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(201);
    expect(result.status).toBe('PROCESSED');
    expect(result.balance).toEqual({ amount: '70.00', currency: 'BRL' });
    expect(result.idempotentReplay).toBe(false);

    const outbox = await query<Array<{ event_type: string }>>(
      `SELECT event_type FROM outbox_message WHERE aggregate_id = ? ORDER BY event_type`,
      [wallet.id],
    );
    // OPENING (2) + BET (2).
    expect(outbox.map((row) => row.event_type)).toEqual([
      'WagerTransactionProcessed',
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
      'WalletBalanceChanged',
    ]);
    await assertLedgerConsistency(wallet.id);
  });

  test('WIN credita e vincula a referência informada', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bet = splitInput(wagerInput(playerId, wallet.id, { money: { amount: '30.00', currency: 'BRL' } }));
    const betResult = await postWager(bet.body, bet.key);
    expect(betResult.status).toBe(201);

    const win = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'WIN',
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: (bet.body.externalTransactionId as string),
      }),
    );
    const { status, body: result } = await postWager(win.body, win.key);

    expect(status).toBe(201);
    expect(result.status).toBe('PROCESSED');
    expect(result.balance).toEqual({ amount: '120.00', currency: 'BRL' });

    const rows = await query<Array<{ reference_transaction_id: string | null }>>(
      `SELECT reference_transaction_id FROM wager_transaction WHERE id = ?`,
      [result.transactionId],
    );
    expect(rows[0]?.reference_transaction_id).toBe(betResult.body.transactionId);
    await assertLedgerConsistency(wallet.id);
  });

  test('WIN sem referência processa sem vínculo', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { kind: 'WIN', money: { amount: '10.00', currency: 'BRL' } }),
    );

    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(201);
    expect(result.balance).toEqual({ amount: '110.00', currency: 'BRL' });
    await assertLedgerConsistency(wallet.id);
  });

  test('LOSS registra sem ledger e sem mexer no saldo', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { kind: 'LOSS', money: { amount: '10.00', currency: 'BRL' } }),
    );

    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(201);
    expect(result.status).toBe('PROCESSED');
    expect(result.balance).toEqual({ amount: '100.00', currency: 'BRL' });

    const ledger = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry WHERE transaction_id = ?`,
      [result.transactionId],
    );
    expect(ledger[0]?.count).toBe(0);

    const stored = await query<Array<{ version: number }>>(
      `SELECT version FROM wallet WHERE id = ?`,
      [wallet.id],
    );
    expect(stored[0]?.version).toBe(1);
    await assertLedgerConsistency(wallet.id);
  });

  test('LOSS de 0.00 é aceito (zero só passa em LOSS)', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { kind: 'LOSS', money: { amount: '0.00', currency: 'BRL' } }),
    );

    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(201);
    expect(result.status).toBe('PROCESSED');
    await assertLedgerConsistency(wallet.id);
  });

  test('REFUND reverte a BET com crédito', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bet = splitInput(wagerInput(playerId, wallet.id, { money: { amount: '30.00', currency: 'BRL' } }));
    await postWager(bet.body, bet.key);

    const refund = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'REFUND',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: bet.body.externalTransactionId as string,
      }),
    );
    const { status, body: result } = await postWager(refund.body, refund.key);

    expect(status).toBe(201);
    expect(result.balance).toEqual({ amount: '100.00', currency: 'BRL' });

    const ledger = await query<Array<{ direction: string }>>(
      `SELECT direction FROM wallet_ledger_entry WHERE transaction_id = ?`,
      [result.transactionId],
    );
    expect(ledger).toHaveLength(1);
    expect(ledger[0]?.direction).toBe('CREDIT');
    await assertLedgerConsistency(wallet.id);
  });

  test('ROLLBACK reverte o WIN com lançamento invertido', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bet = splitInput(wagerInput(playerId, wallet.id, { money: { amount: '20.00', currency: 'BRL' } }));
    await postWager(bet.body, bet.key);
    const win = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'WIN',
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: bet.body.externalTransactionId as string,
      }),
    );
    const winResult = await postWager(win.body, win.key);
    expect(winResult.body.balance).toEqual({ amount: '130.00', currency: 'BRL' });

    const rollback = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'ROLLBACK',
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: win.body.externalTransactionId as string,
      }),
    );
    const { status, body: result } = await postWager(rollback.body, rollback.key);

    expect(status).toBe(201);
    expect(result.balance).toEqual({ amount: '80.00', currency: 'BRL' });

    const ledger = await query<Array<{ direction: string }>>(
      `SELECT direction FROM wallet_ledger_entry WHERE transaction_id = ?`,
      [result.transactionId],
    );
    expect(ledger).toHaveLength(1);
    expect(ledger[0]?.direction).toBe('DEBIT');
    await assertLedgerConsistency(wallet.id);
  });
});

describe('POST /wagering/transactions: replay e conflito', () => {
  test('replay devolve o saldo da época, mesmo depois do saldo mudar', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '30.00', currency: 'BRL' } }),
    );

    const first = await postWager(body, key);
    expect(first.status).toBe(201);
    expect(first.body.idempotentReplay).toBe(false);

    const win = splitInput(
      wagerInput(playerId, wallet.id, { kind: 'WIN', money: { amount: '10.00', currency: 'BRL' } }),
    );
    await postWager(win.body, win.key);

    const replay = await postWager(body, key);
    expect(replay.status).toBe(200);
    expect(replay.body.transactionId).toBe(first.body.transactionId);
    expect(replay.body.balance).toEqual({ amount: '70.00', currency: 'BRL' });
    expect(replay.body.idempotentReplay).toBe(true);

    const response = await fetch(`${baseUrl}/wagering/transactions/${first.body.transactionId}`);
    expect(response.status).toBe(200);
    expect(((await response.json()) as SubmitBody).balance).toEqual({
      amount: '70.00',
      currency: 'BRL',
    });
    await assertLedgerConsistency(wallet.id);
  });

  test('mesma key com payload diferente = 409; mesma external id com outra key = 409', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '10.00', currency: 'BRL' } }),
    );
    await postWager(body, key);

    const conflictPayload = await postWager(
      { ...body, money: { amount: '99.00', currency: 'BRL' } },
      key,
    );
    expect(conflictPayload.status).toBe(409);
    expect(conflictPayload.body.failureCode).toBe('IDEMPOTENCY_CONFLICT');

    const conflictKey = await postWager(body, `other-key:${crypto.randomUUID()}`);
    expect(conflictKey.status).toBe(409);
    expect(conflictKey.body.failureCode).toBe('IDEMPOTENCY_CONFLICT');
    await assertLedgerConsistency(wallet.id);
  });

  test('sem Idempotency-Key = 400; kind OPENING = 400', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { body } = splitInput(wagerInput(playerId, wallet.id));

    const missing = await postWager(body, undefined);
    expect(missing.status).toBe(400);
    expect(missing.body.failureCode).toBe('INVALID_PAYLOAD');

    const opening = await postWager(
      { ...body, kind: 'OPENING' },
      `provider-a:${crypto.randomUUID()}`,
    );
    expect(opening.status).toBe(400);
    expect(opening.body.failureCode).toBe('INVALID_PAYLOAD');
  });

  test('wallet inexistente = 404 sem persistir nada', async () => {
    const walletId = crypto.randomUUID();
    const { key, body } = splitInput(wagerInput(crypto.randomUUID(), walletId));

    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(404);
    expect(result.failureCode).toBe('WALLET_NOT_FOUND');

    const rows = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction WHERE wallet_id = ?`,
      [walletId],
    );
    expect(rows[0]?.count).toBe(0);
  });
});

describe('POST /wagering/transactions: rejeições de negócio', () => {
  test('saldo insuficiente = 422 com REJECTED persistida, sem ledger; replay mantém 422', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const first = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '80.00', currency: 'BRL' } }),
    );
    await postWager(first.body, first.key);

    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '80.00', currency: 'BRL' } }),
    );
    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(422);
    expect(result.status).toBe('REJECTED');
    expect(result.failureCode).toBe('INSUFFICIENT_BALANCE');
    expect(result.balance).toEqual({ amount: '20.00', currency: 'BRL' });
    expect(result.idempotentReplay).toBe(false);

    const ledger = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry WHERE transaction_id = ?`,
      [result.transactionId],
    );
    expect(ledger[0]?.count).toBe(0);

    const events = await query<Array<{ event_type: string }>>(
      `SELECT event_type FROM outbox_message WHERE aggregate_id = ? AND payload->'data'->>'transactionId' = ?`,
      [wallet.id, result.transactionId],
    );
    expect(events.map((row) => row.event_type)).toEqual(['WagerTransactionRejected']);

    const replay = await postWager(body, key);
    expect(replay.status).toBe(422);
    expect(replay.body.idempotentReplay).toBe(true);
    expect(replay.body.failureCode).toBe('INSUFFICIENT_BALANCE');
    expect(replay.body.balance).toEqual({ amount: '20.00', currency: 'BRL' });
    await assertLedgerConsistency(wallet.id);
  });

  test('playerId de outra wallet = 422 WALLET_OWNER_MISMATCH; moeda diferente = 422 CURRENCY_MISMATCH', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');

    const other = splitInput(wagerInput(crypto.randomUUID(), wallet.id));
    const owner = await postWager(other.body, other.key);
    expect(owner.status).toBe(422);
    expect(owner.body.failureCode).toBe('WALLET_OWNER_MISMATCH');

    const currency = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '10.00', currency: 'USD' } }),
    );
    const mismatch = await postWager(currency.body, currency.key);
    expect(mismatch.status).toBe(422);
    expect(mismatch.body.failureCode).toBe('CURRENCY_MISMATCH');
    await assertLedgerConsistency(wallet.id);
  });
});

describe('POST /wagering/transactions: referências', () => {
  test('REFUND antes da BET = 202 com linha e evento', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const missingRef = `missing-${crypto.randomUUID()}`;
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'REFUND',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: missingRef,
      }),
    );

    const { status, body: result } = await postWager(body, key);

    expect(status).toBe(202);
    expect(result.status).toBe('PENDING_REFERENCE');
    expect(result.idempotentReplay).toBe(false);

    const rows = await query<
      Array<{ status: string; retry_attempts: number; pending_since: Date | null; next_retry_at: Date | null }>
    >(
      `SELECT status, retry_attempts, pending_since, next_retry_at
         FROM wager_transaction WHERE id = ?`,
      [result.transactionId],
    );
    expect(rows[0]?.status).toBe('PENDING_REFERENCE');
    expect(rows[0]?.retry_attempts).toBe(0);
    expect(rows[0]?.pending_since).not.toBeNull();
    expect(rows[0]?.next_retry_at).not.toBeNull();

    const events = await query<Array<{ event_type: string }>>(
      `SELECT event_type FROM outbox_message WHERE aggregate_id = ? AND payload->'data'->>'transactionId' = ?`,
      [wallet.id, result.transactionId],
    );
    expect(events.map((row) => row.event_type)).toEqual(['WagerTransactionPendingReference']);

    const replay = await postWager(body, key);
    expect(replay.status).toBe(202);
    expect(replay.body.idempotentReplay).toBe(true);
    await assertLedgerConsistency(wallet.id);
  });

  test('referência REJECTED = 422 INVALID_REFERENCE', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bad = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '200.00', currency: 'BRL' } }),
    );
    const badResult = await postWager(bad.body, bad.key);
    expect(badResult.body.failureCode).toBe('INSUFFICIENT_BALANCE');

    const refund = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'REFUND',
        money: { amount: '200.00', currency: 'BRL' },
        referenceExternalTransactionId: bad.body.externalTransactionId as string,
      }),
    );
    const { status, body: result } = await postWager(refund.body, refund.key);

    expect(status).toBe(422);
    expect(result.failureCode).toBe('INVALID_REFERENCE');
    await assertLedgerConsistency(wallet.id);
  });

  test('segunda reversão = 422 ALREADY_REVERSED', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bet = splitInput(wagerInput(playerId, wallet.id, { money: { amount: '30.00', currency: 'BRL' } }));
    await postWager(bet.body, bet.key);
    const refundInput = {
      kind: 'REFUND',
      money: { amount: '30.00', currency: 'BRL' },
      referenceExternalTransactionId: bet.body.externalTransactionId as string,
    };
    const first = splitInput(wagerInput(playerId, wallet.id, refundInput));
    expect((await postWager(first.body, first.key)).status).toBe(201);
    const second = splitInput(wagerInput(playerId, wallet.id, refundInput));

    const { status, body: result } = await postWager(second.body, second.key);

    expect(status).toBe(422);
    expect(result.failureCode).toBe('ALREADY_REVERSED');
    await assertLedgerConsistency(wallet.id);
  });

  test('reversão que negativaria = 422 REVERSAL_WOULD_OVERDRAW', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bet = splitInput(wagerInput(playerId, wallet.id, { money: { amount: '80.00', currency: 'BRL' } }));
    await postWager(bet.body, bet.key);
    const win = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'WIN',
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: bet.body.externalTransactionId as string,
      }),
    );
    await postWager(win.body, win.key);
    const spend = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '60.00', currency: 'BRL' } }),
    );
    await postWager(spend.body, spend.key);

    const rollback = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'ROLLBACK',
        money: { amount: '50.00', currency: 'BRL' },
        referenceExternalTransactionId: win.body.externalTransactionId as string,
      }),
    );
    const { status, body: result } = await postWager(rollback.body, rollback.key);

    expect(status).toBe(422);
    expect(result.failureCode).toBe('REVERSAL_WOULD_OVERDRAW');
    await assertLedgerConsistency(wallet.id);
  });
});

describe('POST /wagering/transactions: concorrência obrigatória', () => {
  test('100.00 com 2 BET de 80.00 = 1 PROCESSED + 1 REJECTED, saldo 20.00, 1 DEBIT', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const first = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '80.00', currency: 'BRL' } }),
    );
    const second = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '80.00', currency: 'BRL' } }),
    );

    const [one, two] = await Promise.all([
      postWager(first.body, first.key),
      postWager(second.body, second.key),
    ]);

    const processed = [one, two].filter((response) => response.status === 201);
    const rejected = [one, two].filter((response) => response.status === 422);
    expect(processed).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect(rejected[0]?.body.failureCode).toBe('INSUFFICIENT_BALANCE');
    expect(processed[0]?.body.balance).toEqual({ amount: '20.00', currency: 'BRL' });

    const stored = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [wallet.id],
    );
    expect(stored[0]?.balance_amount).toBe('20.00');

    const debits = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [wallet.id],
    );
    expect(debits[0]?.count).toBe(1);
    await assertLedgerConsistency(wallet.id);
  });

  test('mesma BET 50x em paralelo = 1 débito, 1 linha, 1x 201/false + 49x 200/true', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '10.00', currency: 'BRL' } }),
    );

    const responses = await Promise.all(Array.from({ length: 50 }, () => postWager(body, key)));

    const fresh = responses.filter((response) => response.status === 201);
    const replays = responses.filter((response) => response.status === 200);
    expect(fresh).toHaveLength(1);
    expect(replays).toHaveLength(49);
    expect(fresh[0]?.body.idempotentReplay).toBe(false);
    for (const replay of replays) {
      expect(replay.body.idempotentReplay).toBe(true);
      expect(replay.body.transactionId).toBe(fresh[0]!.body.transactionId);
      expect(replay.body.balance).toEqual({ amount: '90.00', currency: 'BRL' });
    }

    const transactions = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction WHERE idempotency_key = ?`,
      [key],
    );
    expect(transactions[0]?.count).toBe(1);

    const debits = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [wallet.id],
    );
    expect(debits[0]?.count).toBe(1);
    await assertLedgerConsistency(wallet.id);
  });

  test('2 REFUNDs concorrentes da mesma BET = 1 PROCESSED + 1 ALREADY_REVERSED', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const bet = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '30.00', currency: 'BRL' } }),
    );
    await postWager(bet.body, bet.key);
    const ref = bet.body.externalTransactionId as string;
    const first = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'REFUND',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: ref,
      }),
    );
    const second = splitInput(
      wagerInput(playerId, wallet.id, {
        kind: 'REFUND',
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: ref,
      }),
    );

    const firstRound = await Promise.all([
      postWager(first.body, first.key),
      postWager(second.body, second.key),
    ]);
    // Provedor-realista: transiente (ex.: deadlock 40P01 → 503) merece um retry
    // com a mesma key — idempotente, converge ao estado terminal (ARCHITECTURE §2).
    const inputs = [first, second];
    const responses = await Promise.all(
      firstRound.map((response, index) =>
        response.status === 201 || response.status === 422
          ? response
          : postWager(inputs[index]!.body, inputs[index]!.key),
      ),
    );

    const processed = responses.filter((response) => response.status === 201);
    const rejected = responses.filter((response) => response.status === 422);
    expect(processed).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect(rejected[0]?.body.failureCode).toBe('ALREADY_REVERSED');
    expect(processed[0]?.body.balance).toEqual({ amount: '100.00', currency: 'BRL' });

    const reversals = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction
        WHERE reference_external_transaction_id = ? AND kind = 'REFUND' AND status = 'PROCESSED'`,
      [ref],
    );
    expect(reversals[0]?.count).toBe(1);

    const stored = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [wallet.id],
    );
    expect(stored[0]?.balance_amount).toBe('100.00');
    await assertLedgerConsistency(wallet.id);
  });

  test('wallets distintas em paralelo não se bloqueiam', async () => {
    const first = await postWallet(crypto.randomUUID(), '100.00');
    const second = await postWallet(crypto.randomUUID(), '100.00');

    const [one, two] = await Promise.all([
      postWager(
        splitInput(wagerInput(first.playerId, first.id)).body,
        `provider-a:${crypto.randomUUID()}`,
      ),
      postWager(
        splitInput(wagerInput(second.playerId, second.id)).body,
        `provider-a:${crypto.randomUUID()}`,
      ),
    ]);

    expect(one.status).toBe(201);
    expect(two.status).toBe(201);
    await assertLedgerConsistency(first.id);
    await assertLedgerConsistency(second.id);
  });
});

describe('GETs de transação', () => {
  test('por id e por provider+external, com envelope e failureCode em REJECTED', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(playerId, '100.00');
    const { key, body } = splitInput(
      wagerInput(playerId, wallet.id, { money: { amount: '80.00', currency: 'BRL' } }),
    );
    const created = await postWager(body, key);
    expect(created.status).toBe(201);

    const byId = await fetch(`${baseUrl}/wagering/transactions/${created.body.transactionId}`);
    expect(byId.status).toBe(200);
    expect(await byId.json()).toEqual({
      transactionId: created.body.transactionId,
      status: 'PROCESSED',
      balance: { amount: '20.00', currency: 'BRL' },
    });

    const byExternal = await fetch(
      `${baseUrl}/providers/provider-a/wagering/transactions/${body.externalTransactionId as string}`,
    );
    expect(byExternal.status).toBe(200);
    expect(await byExternal.json()).toEqual({
      transactionId: created.body.transactionId,
      status: 'PROCESSED',
      balance: { amount: '20.00', currency: 'BRL' },
    });
    await assertLedgerConsistency(wallet.id);
  });

  test('inexistente = 404 nos dois GETs', async () => {
    const missing = crypto.randomUUID();
    const byId = await fetch(`${baseUrl}/wagering/transactions/${missing}`);
    expect(byId.status).toBe(404);

    const byExternal = await fetch(`${baseUrl}/providers/provider-a/wagering/transactions/${missing}`);
    expect(byExternal.status).toBe(404);
  });
});
