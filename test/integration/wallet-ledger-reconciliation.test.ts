import { MikroORM } from '@mikro-orm/postgresql';
import { afterAll, beforeAll, describe, expect, spyOn, test } from 'bun:test';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import type { AddressInfo } from 'node:net';

import { AppModule } from '../../src/app.module';
import { METRICS } from '../../src/application/ports';
import { MemoryMetrics } from '../../src/infra/observability/metrics';
import { assertLedgerConsistency } from './ledger-consistency';

/**
 * `GET /wallets/:walletId/ledger` e `POST /wallets/:walletId/reconciliation`
 * contra PostgreSQL real, com a app ligada (HTTP de verdade). O preload criou
 * `wagering_test` e migrou (incl. a migration 20261017000000 do índice novo);
 * seed direto por SQL é rápido e respeita triggers/constraints do schema.
 * Auth deferida (§6): endpoints abertos.
 */

interface WalletBody {
  id: string;
  playerId: string;
  balance: { amount: string; currency: string };
  version: number;
}

interface LedgerItemBody {
  id: string;
  transactionId: string;
  direction: 'DEBIT' | 'CREDIT';
  money: { amount: string; currency: string };
  balanceBefore: { amount: string; currency: string };
  balanceAfter: { amount: string; currency: string };
  createdAt: string;
}

interface LedgerBody {
  items: LedgerItemBody[];
  nextCursor: string | null;
}

interface ReconciliationBody {
  walletId: string;
  storedBalance: { amount: string; currency: string };
  calculatedBalance: { amount: string; currency: string };
  difference: string;
  consistent: boolean;
  checkedEntries: number;
}

interface ErrorBody {
  failureCode: string;
  message: string;
}

type Response<T> = { status: number; body: T & ErrorBody };

let app: INestApplication;
let baseUrl: string;
let orm: MikroORM;
let metrics: MemoryMetrics;

const RECONCILIATION_METRIC = 'reconciliation_mismatches_total';

function query<T>(sql: string, params: unknown[] = []): Promise<T> {
  return orm.em.getConnection().execute(sql, params) as Promise<T>;
}

async function postWallet(body: unknown): Promise<Response<WalletBody>> {
  const response = await fetch(`${baseUrl}/wallets`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: (await response.json()) as WalletBody & ErrorBody };
}

async function getWallet(walletId: string): Promise<Response<WalletBody>> {
  const response = await fetch(`${baseUrl}/wallets/${walletId}`);
  return { status: response.status, body: (await response.json()) as WalletBody & ErrorBody };
}

async function postWager(
  body: unknown,
  key: string,
): Promise<Response<Record<string, unknown>>> {
  const response = await fetch(`${baseUrl}/wagering/transactions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'idempotency-key': key },
    body: JSON.stringify(body),
  });
  return {
    status: response.status,
    body: (await response.json()) as Record<string, unknown> & ErrorBody,
  };
}

async function getLedger(
  walletId: string,
  params: Record<string, string | undefined> = {},
): Promise<Response<LedgerBody>> {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      search.set(key, value);
    }
  }
  const suffix = search.size > 0 ? `?${search.toString()}` : '';
  const response = await fetch(`${baseUrl}/wallets/${walletId}/ledger${suffix}`);
  return { status: response.status, body: (await response.json()) as LedgerBody & ErrorBody };
}

async function postReconcile(walletId: string): Promise<Response<ReconciliationBody>> {
  const response = await fetch(`${baseUrl}/wallets/${walletId}/reconciliation`, {
    method: 'POST',
  });
  return {
    status: response.status,
    body: (await response.json()) as ReconciliationBody & ErrorBody,
  };
}

/** Input de transação HTTP; a key vai no header (nunca no JSON). */
function wagerInput(
  playerId: string,
  walletId: string,
  overrides: Partial<{
    kind: string;
    money: { amount: string; currency: string };
    referenceExternalTransactionId: string;
  }> = {},
): { key: string; body: Record<string, unknown> } {
  const external = crypto.randomUUID();
  const body: Record<string, unknown> = {
    providerId: 'provider-a',
    externalTransactionId: external,
    playerId,
    walletId,
    roundId: 'round-1',
    gameId: 'game-1',
    kind: 'BET',
    money: { amount: '10.00', currency: 'BRL' },
    ...overrides,
  };
  return { key: `provider-a:${external}`, body };
}

function centsToAmount(cents: number): string {
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, '0')}`;
}

interface SeededRow {
  id: string;
  transactionId: string;
  direction: 'DEBIT' | 'CREDIT';
  amount: string;
  balanceBefore: string;
  balanceAfter: string;
  createdAt: Date;
}

/** Base no passado: entradas de app (HTTP) sempre têm `created_at` depois. */
const SEED_BASE_MS = new Date('2026-10-17T00:00:00.000Z').getTime();

/**
 * Seed direto por SQL: `count` lançamentos alternando CREDIT 1.00 / DEBIT 0.50
 * a partir de 0.00, cadeia before/after consistente com os CHECKs do schema.
 * `firstOffsetMs` desloca no tempo para não colidir com lotes anteriores.
 */
async function insertSeededLedger(
  walletId: string,
  playerId: string,
  count: number,
  firstOffsetMs = 0,
): Promise<Map<string, SeededRow>> {
  const connection = orm.em.getConnection();
  const seeded = new Map<string, SeededRow>();
  let balanceCents = 0;
  for (let index = 0; index < count; index += 1) {
    const isCredit = index % 2 === 0;
    const amountCents = isCredit ? 100 : 50;
    const afterCents = isCredit ? balanceCents + amountCents : balanceCents - amountCents;
    const createdAt = new Date(SEED_BASE_MS + firstOffsetMs + index);
    const id = crypto.randomUUID();
    const transactionId = crypto.randomUUID();
    const direction = isCredit ? 'CREDIT' : 'DEBIT';
    const amount = centsToAmount(amountCents);
    const external = `seed:${transactionId}`;
    await connection.execute(
      `INSERT INTO wager_transaction
         (id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency,
          status, created_at, processed_at)
       VALUES (?, 'seed', ?, ?, ?, ?, ?, 'seed-round', 'seed-game', ?, ?, 'BRL', 'PROCESSED', ?, ?)`,
      [
        transactionId,
        external,
        external,
        '0'.repeat(64),
        walletId,
        playerId,
        isCredit ? 'WIN' : 'BET',
        amount,
        createdAt,
        createdAt,
      ],
    );
    await connection.execute(
      `INSERT INTO wallet_ledger_entry
         (id, wallet_id, transaction_id, direction, amount, currency,
          balance_before_amount, balance_before_currency,
          balance_after_amount, balance_after_currency, created_at)
       VALUES (?, ?, ?, ?, ?, 'BRL', ?, 'BRL', ?, 'BRL', ?)`,
      [
        id,
        walletId,
        transactionId,
        direction,
        amount,
        centsToAmount(balanceCents),
        centsToAmount(afterCents),
        createdAt,
      ],
    );
    seeded.set(id, {
      id,
      transactionId,
      direction: direction as 'DEBIT' | 'CREDIT',
      amount,
      balanceBefore: centsToAmount(balanceCents),
      balanceAfter: centsToAmount(afterCents),
      createdAt,
    });
    balanceCents = afterCents;
  }
  return seeded;
}

/** Drena a paginação (keyset, limit fixo) e devolve os ids na ordem lida. */
async function drainLedger(
  walletId: string,
  limit: number,
): Promise<{ ids: string[]; pages: number[]; cursors: Array<string | null> }> {
  const ids: string[] = [];
  const pages: number[] = [];
  const cursors: Array<string | null> = [];
  let cursor: string | undefined;
  for (let guard = 0; guard < 1_000; guard += 1) {
    const page = await getLedger(walletId, { limit: String(limit), cursor });
    expect(page.status).toBe(200);
    ids.push(...page.body.items.map((item) => item.id));
    pages.push(page.body.items.length);
    cursors.push(page.body.nextCursor);
    if (page.body.nextCursor === null) {
      return { ids, pages, cursors };
    }
    cursor = page.body.nextCursor;
  }
  throw new Error(`drain infinito em ${walletId}`);
}

/** Ordem lexicográfica estrita por `(createdAt, id)` — repetição quebra. */
function assertStrictlyOrdered(items: LedgerItemBody[]): void {
  let previous: string | undefined;
  for (const item of items) {
    const key = `${item.createdAt}#${item.id}`;
    expect(key > (previous ?? '')).toBe(true);
    previous = key;
  }
}

/** Wrapper fino do helper compartilhado com o `query` deste arquivo. */
async function assertLedgerConsistent(walletId: string): Promise<void> {
  await assertLedgerConsistency((sql, params) => query(sql, params), walletId);
}

beforeAll(async () => {
  app = await NestFactory.create(AppModule, { logger: false });
  await app.listen(0);

  const address = app.getHttpServer().address() as AddressInfo;
  baseUrl = `http://127.0.0.1:${address.port}`;
  orm = app.get(MikroORM);
  metrics = app.get(METRICS) as MemoryMetrics;
});

afterAll(async () => {
  await app.close();
});

describe('GET /wallets/:walletId/ledger: paginação keyset', () => {
  test('130 lançamentos → 3 páginas (50/50/30), ordem estável, sem repetir nem perder', async () => {
    const wallet = await postWallet({
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    expect(wallet.status).toBe(201);
    const seeded = await insertSeededLedger(wallet.body.id, wallet.body.playerId, 130);

    // limit 50 enche duas páginas; a última sai menor e fecha com null.
    const { ids, pages, cursors } = await drainLedger(wallet.body.id, 50);
    expect(pages).toEqual([50, 50, 30]);
    expect(cursors[0]).not.toBeNull();
    expect(cursors[1]).not.toBeNull();
    expect(cursors[2]).toBeNull();
    expect(new Set(ids).size).toBe(130);
    expect([...new Set(ids)].sort()).toEqual([...seeded.keys()].sort());

    // Com limit 200 tudo cabe numa página só (página < limit → cursor null).
    const full = await drainLedger(wallet.body.id, 200);
    expect(full.pages).toEqual([130]);
    expect(full.cursors).toEqual([null]);
    expect(new Set(full.ids)).toEqual(new Set(ids));

    // Reentrada pelo cursor confirmado: dois drains são idênticos (estável).
    const replay = await drainLedger(wallet.body.id, 50);
    expect(replay.ids).toEqual(ids);
  });

  test('linhas batem com o seed: ordem, direção, money, balanceBefore/After e createdAt', async () => {
    const wallet = await postWallet({
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const seeded = await insertSeededLedger(wallet.body.id, wallet.body.playerId, 130);
    const firstPage = await getLedger(wallet.body.id, { limit: '130' });
    expect(firstPage.status).toBe(200);
    expect(firstPage.body.nextCursor).toBeNull();

    assertStrictlyOrdered(firstPage.body.items);
    expect(firstPage.body.items).toHaveLength(130);
    for (const item of firstPage.body.items) {
      const row = seeded.get(item.id);
      expect(row).toBeDefined();
      if (row === undefined) {
        continue;
      }
      expect(item.transactionId).toBe(row.transactionId);
      expect(item.direction).toBe(row.direction);
      expect(item.money).toEqual({ amount: row.amount, currency: 'BRL' });
      expect(item.balanceBefore).toEqual({ amount: row.balanceBefore, currency: 'BRL' });
      expect(item.balanceAfter).toEqual({ amount: row.balanceAfter, currency: 'BRL' });
      expect(item.createdAt).toBe(row.createdAt.toISOString());
    }
  });

  test('limit 0/201/-1/25.5/abc/1e2 e cursor malformado → 400 INVALID_PAYLOAD', async () => {
    const wallet = await postWallet({
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const badLimits = ['0', '201', '-1', '25.5', 'abc', '1e2'];
    for (const limit of badLimits) {
      const page = await getLedger(wallet.body.id, { limit });
      expect(page.status).toBe(400);
      expect(page.body.failureCode).toBe('INVALID_PAYLOAD');
    }

    const garbageCursors = [
      '!!!!', // fora do alfabeto base64url
      Buffer.from('not json', 'utf8').toString('base64url'),
      Buffer.from('[]', 'utf8').toString('base64url'),
      Buffer.from('{"createdAt":"2026-10-17T00:00:00.000Z"}', 'utf8').toString('base64url'),
      Buffer.from(
        '{"createdAt":"not-a-date","id":"00000000-0000-0000-0000-000000000000"}',
        'utf8',
      ).toString('base64url'),
      Buffer.from(
        '{"createdAt":"2026-10-17T00:00:00.000Z","id":"not-a-uuid"}',
        'utf8',
      ).toString('base64url'),
      Buffer.from(
        '{"createdAt":"2026-10-17T00:00:00.000Z","id":"00000000-0000-0000-0000-000000000000","extra":true}',
        'utf8',
      ).toString('base64url'),
    ];
    for (const cursor of garbageCursors) {
      const page = await getLedger(wallet.body.id, { cursor, limit: '10' });
      expect(page.status).toBe(400);
      expect(page.body.failureCode).toBe('INVALID_PAYLOAD');
    }
  });

  test('wallet nova sem lançamentos → items [] e nextCursor null; wallet inexistente → 404', async () => {
    const wallet = await postWallet({
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const page = await getLedger(wallet.body.id);
    expect(page.status).toBe(200);
    expect(page.body.items).toEqual([]);
    expect(page.body.nextCursor).toBeNull();

    const missing = crypto.randomUUID();
    const missingPage = await getLedger(missing);
    expect(missingPage.status).toBe(404);
    expect(missingPage.body.failureCode).toBe('WALLET_NOT_FOUND');
    const missingReconciliation = await postReconcile(missing);
    expect(missingReconciliation.status).toBe(404);
    expect(missingReconciliation.body.failureCode).toBe('WALLET_NOT_FOUND');
  });

  test('escrevendo ENTRE páginas: nenhum id se repete e todos aparecem exatamente 1x', async () => {
    const wallet = await postWallet({
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '0.00', currency: 'BRL' },
    });
    const playerId = wallet.body.playerId;
    let expected = await insertSeededLedger(wallet.body.id, playerId, 6);

    // Página 1, depois escrita concorrente (offsets futuros), depois o resto.
    const firstPage = await getLedger(wallet.body.id, { limit: '2' });
    expect(firstPage.status).toBe(200);
    const seen = [...firstPage.body.items.map((item) => item.id)];
    expect(firstPage.body.nextCursor).not.toBeNull();

    // Escrita 1: +4 lançamentos (createdAt após todos os 6 do seed).
    expected = new Map([...expected, ...(await insertSeededLedger(wallet.body.id, playerId, 4, 20))]);

    let cursor: string | undefined = firstPage.body.nextCursor as string;
    let inserted = false;
    while (cursor !== undefined) {
      const page = await getLedger(wallet.body.id, { limit: '2', cursor });
      expect(page.status).toBe(200);
      seen.push(...page.body.items.map((item) => item.id));
      cursor = page.body.nextCursor ?? undefined;
      // Escrita 2: quando metade drainada, +2 lançamentos (offsets ainda futuros).
      if (!inserted && seen.length >= 8) {
        expected = new Map([...expected, ...(await insertSeededLedger(wallet.body.id, playerId, 2, 40))]);
        inserted = true;
      }
    }

    expect(new Set(seen).size).toBe(seen.length);
    expect(seen).toHaveLength(12);
    expect([...new Set(seen)].sort()).toEqual([...expected.keys()].sort());
  });
});

describe('POST /wallets/:walletId/reconciliation', () => {
  test('carga mista concorrente em 4 wallets → consistent true, difference 0.00, checkedEntries == COUNT', async () => {
    const wallets = await Promise.all(
      Array.from({ length: 4 }, () =>
        postWallet({
          playerId: crypto.randomUUID(),
          initialBalance: { amount: '100.00', currency: 'BRL' },
        }),
      ),
    );
    expect(wallets.every((wallet) => wallet.status === 201)).toBe(true);

    // Fluxos por wallet em paralelo (dentro da wallet sequencial para saldo
    // previsível): 3 BET 10.00 + 2 WIN 5.00 + 1 REFUND 10.00 → saldo 90.00.
    await Promise.all(
      wallets.map(async (wallet) => {
        const bets: string[] = [];
        for (let index = 0; index < 3; index += 1) {
          const wager = wagerInput(wallet.body.playerId, wallet.body.id);
          const response = await postWager(wager.body, wager.key);
          expect(response.status).toBe(201);
          bets.push(String(wager.body.externalTransactionId));
        }
        for (let index = 0; index < 2; index += 1) {
          const wager = wagerInput(wallet.body.playerId, wallet.body.id, {
            kind: 'WIN',
            money: { amount: '5.00', currency: 'BRL' },
            referenceExternalTransactionId: bets[0] as string,
          });
          const response = await postWager(wager.body, wager.key);
          expect(response.status).toBe(201);
        }
        const refund = wagerInput(wallet.body.playerId, wallet.body.id, {
          kind: 'REFUND',
          money: { amount: '10.00', currency: 'BRL' },
          referenceExternalTransactionId: bets[0] as string,
        });
        const response = await postWager(refund.body, refund.key);
        expect(response.status).toBe(201);
      }),
    );

    // consistent true → nada de log de erro nem contador.
    const counterBefore = metrics.counter(RECONCILIATION_METRIC);
    const errorSpy = spyOn(console, 'error');
    try {
      for (const wallet of wallets) {
        const finalWallet = await getWallet(wallet.body.id);
        expect(finalWallet.body.balance).toEqual({ amount: '90.00', currency: 'BRL' });

        const { status, body } = await postReconcile(wallet.body.id);
        expect(status).toBe(200);
        expect(body.consistent).toBe(true);
        expect(body.difference).toBe('0.00');
        expect(body.storedBalance).toEqual({ amount: '90.00', currency: 'BRL' });
        expect(body.calculatedBalance).toEqual({ amount: '90.00', currency: 'BRL' });
        expect(body.walletId).toBe(wallet.body.id);

        const checked = await query<Array<{ entries: string }>>(
          `SELECT COUNT(*)::text AS entries FROM wallet_ledger_entry WHERE wallet_id = ?`,
          [wallet.body.id],
        );
        // 1 OPENING CREDIT + 3 DEBIT + 2 WIN CREDIT + 1 REFUND CREDIT.
        expect(Number(checked[0]?.entries)).toBe(7);
        expect(body.checkedEntries).toBe(Number(checked[0]?.entries));

        await assertLedgerConsistent(wallet.body.id);
      }
    } finally {
      errorSpy.mockRestore();
    }
    expect(metrics.counter(RECONCILIATION_METRIC)).toBe(counterBefore);
  });

  test('divergência provocada: consistent false, difference exata, contador +1, log de erro, ledger intacto', async () => {
    const wallet = await postWallet({
      playerId: crypto.randomUUID(),
      initialBalance: { amount: '100.00', currency: 'BRL' },
    });
    const bet = wagerInput(wallet.body.playerId, wallet.body.id);
    expect((await postWager(bet.body, bet.key)).status).toBe(201);

    const counterBefore = metrics.counter(RECONCILIATION_METRIC);
    const errorSpy = spyOn(console, 'error');
    try {
      // A wallet não tem trigger de imutabilidade: UPDATE direto provoca a divergência.
      await query(`UPDATE wallet SET balance_amount = 85.00 WHERE id = ?`, [wallet.body.id]);

      const { status, body } = await postReconcile(wallet.body.id);
      expect(status).toBe(200);
      expect(body.consistent).toBe(false);
      expect(body.storedBalance).toEqual({ amount: '85.00', currency: 'BRL' });
      expect(body.calculatedBalance).toEqual({ amount: '90.00', currency: 'BRL' });
      expect(body.difference).toBe('-5.00');
      expect(body.checkedEntries).toBe(2);

      // Contador incrementado exatamente uma vez (delta por-execução).
      expect(metrics.counter(RECONCILIATION_METRIC) - counterBefore).toBe(1);

      // Log de erro JSON com walletId e diferença (asserção barata do evento da casa).
      const logged = errorSpy.mock.calls
        .map((call) => String(call[0] ?? ''))
        .filter((line) => line.includes('"event":"reconciliation.mismatch"'));
      expect(logged).toHaveLength(1);
      const loggedPayload = JSON.parse(logged[0] ?? '{}') as Record<string, unknown>;
      expect(loggedPayload.walletId).toBe(wallet.body.id);
      expect(loggedPayload.difference).toBe('-5.00');
      expect(loggedPayload.stored).toBe('85.00');

      // Divergência NUNCA é corrigida: ledger intacto e saldo mantido.
      const snapshot = await query<Array<{ count: string; ledger_sum: string | null; balance: string }>>(
        `SELECT COUNT(*)::text AS count,
                COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)::text AS ledger_sum,
                (SELECT balance_amount::text FROM wallet WHERE id = ?) AS balance
           FROM wallet_ledger_entry WHERE wallet_id = ?`,
        [wallet.body.id, wallet.body.id],
      );
      expect(Number(snapshot[0]?.count)).toBe(2);
      expect(snapshot[0]?.ledger_sum).toBe('90.00');
      expect(snapshot[0]?.balance).toBe('85.00');
    } finally {
      errorSpy.mockRestore();
    }
  });
});
