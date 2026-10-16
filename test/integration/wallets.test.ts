import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { AddressInfo } from 'node:net';

import { AppModule } from '../../src/app.module';

/**
 * `POST /wallets` + `GET /wallets/:walletId` contra PostgreSQL real, com a app
 * ligada (HTTP de verdade). O preload já criou `wagering_test` e migrou; cada
 * teste usa UUIDs próprios, sem `down()`/`TRUNCATE`.
 *
 * Auth é deferida globalmente (ARCHITECTURE §6): os endpoints estão abertos e
 * os testes não enviam credencial alguma.
 */

interface WalletBody {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

interface ErrorBody {
  failureCode: string;
  message: string;
}

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

async function postWallet(body: unknown): Promise<{ status: number; body: WalletBody & ErrorBody }> {
  const response = await fetch(`${baseUrl}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as WalletBody & ErrorBody };
}

function balanceInput(amount: string, currency = 'BRL') {
  return { playerId: crypto.randomUUID(), initialBalance: { amount, currency } };
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

describe('POST /wallets: saldo inicial positivo', () => {
  test('cria wallet com OPENING PROCESSED + 1 CREDIT + 2 eventos, e balance == ledger', async () => {
    const playerId = crypto.randomUUID();
    const { status, body } = await postWallet({
      playerId,
      initialBalance: { amount: '1000.00', currency: 'BRL' },
    });

    expect(status).toBe(201);
    expect(body.playerId).toBe(playerId);
    expect(body.balance).toEqual({ amount: '1000.00', currency: 'BRL' });
    expect(body.version).toBe(1);

    const transactions = await query<
      Array<{
        kind: string;
        status: string;
        provider_id: string;
        external_transaction_id: string;
        idempotency_key: string;
        round_id: string;
        game_id: string;
        money_amount: string;
        observed_balance_amount: string;
        processed_at: Date | null;
      }>
    >(
      `SELECT kind, status, provider_id, external_transaction_id, idempotency_key,
              round_id, game_id, money_amount, observed_balance_amount, processed_at
         FROM wager_transaction WHERE wallet_id = ?`,
      [body.id],
    );

    expect(transactions).toHaveLength(1);
    expect(transactions[0]).toMatchObject({
      kind: 'OPENING',
      status: 'PROCESSED',
      provider_id: 'system',
      external_transaction_id: `opening:${body.id}`,
      idempotency_key: `opening:${body.id}`,
      round_id: 'opening',
      game_id: 'opening',
      money_amount: '1000.00',
      observed_balance_amount: '1000.00',
    });
    expect(transactions[0]?.processed_at).not.toBeNull();

    const ledger = await query<
      Array<{
        direction: string;
        amount: string;
        balance_before_amount: string;
        balance_after_amount: string;
      }>
    >(
      `SELECT direction, amount, balance_before_amount, balance_after_amount
         FROM wallet_ledger_entry WHERE wallet_id = ?`,
      [body.id],
    );

    expect(ledger).toHaveLength(1);
    expect(ledger[0]).toEqual({
      direction: 'CREDIT',
      amount: '1000.00',
      balance_before_amount: '0.00',
      balance_after_amount: '1000.00',
    });

    const outbox = await query<Array<{ event_type: string; aggregate_id: string }>>(
      `SELECT event_type, aggregate_id FROM outbox_message WHERE aggregate_id = ? ORDER BY event_type`,
      [body.id],
    );
    expect(outbox.map((row) => row.event_type)).toEqual([
      'WagerTransactionProcessed',
      'WalletBalanceChanged',
    ]);
    for (const row of outbox) {
      expect(row.aggregate_id).toBe(body.id);
    }

    const consistency = await query<Array<{ balance_amount: string; ledger_sum: string }>>(
      `SELECT w.balance_amount,
              COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount ELSE -l.amount END), 0) AS ledger_sum
         FROM wallet w
         LEFT JOIN wallet_ledger_entry l ON l.wallet_id = w.id
        WHERE w.id = ?
        GROUP BY w.balance_amount`,
      [body.id],
    );
    expect(consistency).toHaveLength(1);
    expect(Number(consistency[0]?.balance_amount)).toBe(Number(consistency[0]?.ledger_sum));
  });
});

describe('POST /wallets: saldo inicial zero', () => {
  test('cria só a wallet, sem OPENING/ledger/eventos', async () => {
    const { status, body } = await postWallet(balanceInput('0.00'));

    expect(status).toBe(201);
    expect(body.balance).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(body.version).toBe(1);

    const transactions = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction WHERE wallet_id = ?`,
      [body.id],
    );
    const ledger = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry WHERE wallet_id = ?`,
      [body.id],
    );
    const outbox = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM outbox_message WHERE aggregate_id = ?`,
      [body.id],
    );

    expect(transactions[0]?.count).toBe(0);
    expect(ledger[0]?.count).toBe(0);
    expect(outbox[0]?.count).toBe(0);
  });
});

describe('POST /wallets: conflito de playerId+currency', () => {
  test('duplicata sequencial retorna 409 WALLET_CONFLICT', async () => {
    const playerId = crypto.randomUUID();
    const payload = { playerId, initialBalance: { amount: '10.00', currency: 'BRL' } };

    const first = await postWallet(payload);
    const second = await postWallet(payload);

    expect(first.status).toBe(201);
    expect(second.status).toBe(409);
    expect(second.body.failureCode).toBe('WALLET_CONFLICT');
  });

  test('50 POSTs em paralelo: exatamente 1×201 e 49×409, 1 lançamento no ledger', async () => {
    const playerId = crypto.randomUUID();
    const payload = { playerId, initialBalance: { amount: '10.00', currency: 'BRL' } };

    const responses = await Promise.all(Array.from({ length: 50 }, () => postWallet(payload)));

    const created = responses.filter((response) => response.status === 201);
    const conflicts = responses.filter((response) => response.status === 409);

    expect(created).toHaveLength(1);
    expect(conflicts).toHaveLength(49);
    for (const conflict of conflicts) {
      expect(conflict.body.failureCode).toBe('WALLET_CONFLICT');
    }

    const walletId = created[0]!.body.id;

    const wallets = await query<Array<{ id: string }>>(
      `SELECT id FROM wallet WHERE player_id = ?`,
      [playerId],
    );
    expect(wallets).toHaveLength(1);

    const ledger = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry WHERE wallet_id = ?`,
      [walletId],
    );
    expect(ledger[0]?.count).toBe(1);

    const transactions = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wager_transaction WHERE wallet_id = ?`,
      [walletId],
    );
    expect(transactions[0]?.count).toBe(1);
  });
});

describe('POST /wallets: payload inválido', () => {
  test('amount fora do contrato (25.1, 1e3, negativo) e currency vazia → 400 INVALID_PAYLOAD', async () => {
    const cases = [
      balanceInput('25.1'),
      balanceInput('1e3'),
      balanceInput('-5.00'),
      balanceInput('10.00', ''),
      { playerId: 'not-a-uuid', initialBalance: { amount: '10.00', currency: 'BRL' } },
    ];

    for (const payload of cases) {
      const { status, body } = await postWallet(payload);
      expect(status).toBe(400);
      expect(body.failureCode).toBe('INVALID_PAYLOAD');
    }
  });
});

describe('GET /wallets/:walletId', () => {
  test('retorna o mesmo shape do POST', async () => {
    const created = await postWallet(balanceInput('25.00'));

    const response = await fetch(`${baseUrl}/wallets/${created.body.id}`);
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual(created.body);
  });

  test('wallet inexistente → 404 WALLET_NOT_FOUND, sem persistir nada', async () => {
    const missing = crypto.randomUUID();
    const response = await fetch(`${baseUrl}/wallets/${missing}`);

    expect(response.status).toBe(404);
    const body = (await response.json()) as ErrorBody;
    expect(body.failureCode).toBe('WALLET_NOT_FOUND');

    const wallets = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet WHERE id = ?`,
      [missing],
    );
    expect(wallets[0]?.count).toBe(0);
  });
});
