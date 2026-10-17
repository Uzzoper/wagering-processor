import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, test } from 'bun:test';

import { buildMikroOrmConfig } from '../../src/infra/mikro-orm.config';
import { spawnApp, stopAllApps, type SpawnApp } from '../helpers/app-process';
import { assertLedgerConsistency } from '../integration/ledger-consistency';

/**
 * Fatia 6 — ≥3 instâncias reais em processos separados (HTTP de verdade),
 * compartilhando o mesmo PostgreSQL: unidade de concorrência é `walletId`
 * (ARCHITECTURE §11), não o processo.
 *
 * Cada instância via `bun run src/main.ts` com PORT própria e apenas
 * `WORKERS_ENABLED=api` (nenhum worker de fila interfere). O preload já migrou
 * `wagering_test`; aqui só fazemos queries via MikroORM.init direto.
 */

const PORTS = [4311, 4312, 4313];
const BASE_URLS = PORTS.map((port) => `http://127.0.0.1:${port}`);

interface WalletBody {
  id: string;
  playerId: string;
}

interface SubmitBody {
  transactionId: string;
  status: string;
  balance: { amount: string; currency: string };
  idempotentReplay: boolean;
  failureCode?: string;
}

let orm: MikroORM;
const instances: SpawnApp[] = [];

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

interface WagerInput {
  key: string;
  body: Record<string, unknown>;
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
      roundId: 'instances-round',
      gameId: 'instances-game',
      kind: 'BET',
      money: { amount, currency: 'BRL' },
    },
  };
}

async function postWallet(
  baseUrl: string,
  playerId: string,
  amount: string,
): Promise<WalletBody> {
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

beforeAll(async () => {
  // Bancada: as instâncias herdam o banco do preload (`wagering_test`)
  // via `process.env`, usado no spawn.
  orm = await MikroORM.init(buildMikroOrmConfig());
  instances.push(
    await spawnApp({ port: PORTS[0]!, workers: 'api' }),
    await spawnApp({ port: PORTS[1]!, workers: 'api' }),
    await spawnApp({ port: PORTS[2]!, workers: 'api' }),
  );
}, 60_000);

afterAll(async () => {
  // Shutdown com SIGKILL de cauda — mesmo se os asserts falharem.
  await stopAllApps(instances);
  await orm.close(true);
});

describe('3 instâncias compartilhando o mesmo PostgreSQL', () => {
  test('100.00 com 2 BETs de 80.00 em instâncias distintas + replay na terceira', async () => {
    const playerId = crypto.randomUUID();
    const wallet = await postWallet(BASE_URLS[0]!, playerId, '100.00');

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

    // Replay da BET processada, na terceira instância: 200 com o saldo da época.
    const replay = await postWager(BASE_URLS[2]!, processed[0]!.input);
    expect(replay.status).toBe(200);
    expect(replay.body.idempotentReplay).toBe(true);
    expect(replay.body.transactionId).toBe(processed[0]!.response.body.transactionId);
    expect(replay.body.balance).toEqual({ amount: '20.00', currency: 'BRL' });

    const stored = await query<Array<{ balance_amount: string }>>(
      `SELECT balance_amount FROM wallet WHERE id = ?`,
      [wallet.id],
    );
    expect(stored[0]?.balance_amount).toBe('20.00');

    const debits = await query<Array<{ count: number }>>(
      `SELECT count(*)::int AS count FROM wallet_ledger_entry
        WHERE wallet_id = ? AND direction = 'DEBIT'`,
      [wallet.id],
    );
    expect(debits[0]?.count).toBe(1);

    await assertLedgerConsistency(query, wallet.id);
  }, 60_000);
});
